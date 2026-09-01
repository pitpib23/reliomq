"""Crash-resistant FIFO storage for reliable MQTT message envelopes.

This is deliberately NOT dressed up as an MQTT concept -- :class:`Outbox` is
what gives reliomq its durability, and it exists because plain MQTT has
nothing like it.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._compat import resolve_renamed_argument
from .protocol import MessageEnvelope, ProtocolError, validate_message_id


class OutboxError(RuntimeError):
    """Raised when durable queue state cannot be read or safely updated."""


# Deprecated (0.1.x/0.2.x) alias -- same class, same behavior.
StoreError = OutboxError


@dataclass(frozen=True, slots=True)
class _StoredRecord:
    raw: bytes
    envelope: MessageEnvelope | None
    pending: bool


class Outbox:
    """A crash-safe, on-disk FIFO queue of pending :class:`MessageEnvelope`.

    Used internally by :class:`~reliomq.sender.Sender`, and safe to use
    directly if you want to inspect or manage the durable queue outside of
    a running sender (for example, a maintenance script).

    Every :meth:`append` is durable before it returns: the record is
    flushed and ``fsync``'d to disk, so a crash immediately afterward cannot
    lose it. :meth:`remove_oldest` is the only way a record leaves the
    queue, and it only succeeds when the envelope you pass in is an exact
    match for the current oldest record -- this is what keeps FIFO order and
    ACK correlation trustworthy even across a restart.

    Synchronization is process-local (one :class:`threading.RLock` guards
    every operation). Applications must not have multiple *processes* write
    the same queue file concurrently.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        logger: logging.Logger | None = None,
    ) -> None:
        try:
            raw_path = os.fspath(path)
        except TypeError as error:
            raise ValueError("path must be a filesystem path") from error
        if isinstance(raw_path, bytes) or not raw_path or "\x00" in raw_path:
            raise ValueError("path must be a non-empty text filesystem path")

        self._path = Path(raw_path).expanduser()
        self._logger = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()

        self._logger.info(
            "Outbox opened | path=%s | pending=%s", self._path, self.size()
        )

    @property
    def path(self) -> Path:
        return self._path

    def append(self, envelope: MessageEnvelope) -> bool:
        """Durably append ``envelope``; return False if its ID is pending."""

        self._require_envelope(envelope)
        encoded = envelope.to_bytes()

        with self._lock:
            records = self._read_records()
            if any(
                record.envelope is not None
                and record.envelope.message_id == envelope.message_id
                for record in records
            ):
                self._logger.debug(
                    "Append skipped; message_id already pending | message_id=%s",
                    envelope.message_id,
                )
                return False

            self._ensure_parent_directory()
            created = not self._path.exists()
            try:
                with self._path.open("a+b") as file:
                    file.seek(0, os.SEEK_END)
                    end_offset = file.tell()
                    if end_offset:
                        file.seek(-1, os.SEEK_END)
                        if file.read(1) != b"\n":
                            # Preserve a complete or torn final record, while
                            # ensuring this append starts a new JSONL record.
                            file.seek(0, os.SEEK_END)
                            file.write(b"\n")
                    file.write(encoded)
                    file.write(b"\n")
                    file.flush()
                    os.fsync(file.fileno())
            except OSError as error:
                raise OutboxError(f"cannot append to queue {self._path}: {error}") from error

            if created:
                self._fsync_parent_directory()
            self._logger.debug(
                "Message durably appended | message_id=%s | topic=%s",
                envelope.message_id,
                envelope.topic,
            )
            return True

    def load(self) -> list[MessageEnvelope]:
        """Load all valid, unique pending envelopes in FIFO order."""

        with self._lock:
            return [
                record.envelope
                for record in self._read_records()
                if record.pending and record.envelope is not None
            ]

    def peek_oldest(self) -> MessageEnvelope | None:
        """Return the oldest valid pending envelope without changing storage."""

        with self._lock:
            for record in self._read_records():
                if record.pending:
                    return record.envelope
            return None

    def remove_oldest(self, expected: MessageEnvelope) -> bool:
        """Remove one record only when the oldest envelope exactly matches."""

        self._require_envelope(expected)
        expected_bytes = expected.to_bytes()

        with self._lock:
            records = self._read_records()
            oldest_index: int | None = None
            oldest: MessageEnvelope | None = None
            for index, record in enumerate(records):
                if record.pending:
                    oldest_index = index
                    oldest = record.envelope
                    break

            if oldest_index is None or oldest is None:
                return False
            if oldest.to_bytes() != expected_bytes:
                return False

            remaining = [
                record.raw
                for index, record in enumerate(records)
                if index != oldest_index
            ]
            self._atomic_rewrite(remaining)
            self._logger.debug(
                "Persisted message removed | message_id=%s | remaining=%s",
                expected.message_id,
                len(remaining),
            )
            return True

    def size(self) -> int:
        """Return the number of valid, unique pending envelopes."""

        with self._lock:
            return sum(1 for record in self._read_records() if record.pending)

    def contains(
        self,
        message_id: str | None = None,
        *,
        event_id: str | None = None,
    ) -> bool:
        """Return whether a valid pending record has ``message_id``."""

        resolved_id = resolve_renamed_argument(
            new_value=message_id,
            old_value=event_id,
            new_name="message_id",
            old_name="event_id",
            owner="Outbox.contains",
            default="",
        )
        validate_message_id(resolved_id)
        with self._lock:
            return any(
                record.pending
                and record.envelope is not None
                and record.envelope.message_id == resolved_id
                for record in self._read_records()
            )

    def __len__(self) -> int:
        return self.size()

    @staticmethod
    def _require_envelope(value: Any) -> None:
        if not isinstance(value, MessageEnvelope):
            raise TypeError("Outbox operations require a MessageEnvelope")

    def _read_records(self) -> list[_StoredRecord]:
        try:
            with self._path.open("rb") as file:
                raw_records = file.readlines()
        except FileNotFoundError:
            return []
        except OSError as error:
            raise OutboxError(f"cannot read queue {self._path}: {error}") from error

        records: list[_StoredRecord] = []
        seen_message_ids: set[str] = set()
        for line_number, raw in enumerate(raw_records, start=1):
            try:
                envelope = MessageEnvelope.from_bytes(raw)
            except ProtocolError as error:
                self._logger.warning(
                    "Skipping corrupt durable queue record | path=%s | line=%s | error=%s",
                    self._path,
                    line_number,
                    error,
                )
                records.append(_StoredRecord(raw=raw, envelope=None, pending=False))
                continue

            pending = envelope.message_id not in seen_message_ids
            if pending:
                seen_message_ids.add(envelope.message_id)
            else:
                self._logger.warning(
                    "Skipping duplicate durable queue message_id | path=%s | "
                    "line=%s | message_id=%s",
                    self._path,
                    line_number,
                    envelope.message_id,
                )
            records.append(
                _StoredRecord(raw=raw, envelope=envelope, pending=pending)
            )
        return records

    def _ensure_parent_directory(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise OutboxError(
                f"cannot create queue directory {self._path.parent}: {error}"
            ) from error

    def _atomic_rewrite(self, raw_records: list[bytes]) -> None:
        self._ensure_parent_directory()
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                dir=str(self._path.parent),
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as file:
                for raw in raw_records:
                    file.write(raw)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, self._path)
            temporary_path = None
            self._fsync_parent_directory()
        except OSError as error:
            raise OutboxError(f"cannot rewrite queue {self._path}: {error}") from error
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    self._logger.warning(
                        "Could not remove temporary queue file %s", temporary_path
                    )

    def _fsync_parent_directory(self) -> None:
        """Best-effort directory sync; opening directories fails on Windows."""

        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        descriptor: int | None = None
        try:
            descriptor = os.open(self._path.parent, flags)
            os.fsync(descriptor)
        except OSError as error:
            self._logger.debug(
                "Directory fsync unavailable for %s: %s", self._path.parent, error
            )
        finally:
            if descriptor is not None:
                os.close(descriptor)


# Deprecated (0.1.x/0.2.x) alias -- same class, same behavior, same on-disk
# format. Nothing about durability or FIFO semantics changed with the name.
DurableMessageStore = Outbox
