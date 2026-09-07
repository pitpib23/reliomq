"""Segmented, crash-resistant FIFO storage for reliable MQTT envelopes.

The public :class:`Outbox` API deliberately remains small. Internally the
queue is a set of immutable, numbered segment files plus an atomically
replaced head checkpoint. Acknowledging one message therefore advances a
cursor; it never rewrites the remaining payload records.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import re
import shutil
import struct
import tempfile
import threading
import zlib
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ._compat import resolve_renamed_argument
from .protocol import MessageEnvelope, ProtocolError, validate_message_id


class OutboxError(RuntimeError):
    """Raised when durable queue state cannot be read or safely updated."""


StoreError = OutboxError


@dataclass(frozen=True, slots=True)
class AppendResult:
    """Result of a segmented batch append."""

    appended_count: int
    bytes_written: int
    rotated: bool


@dataclass(frozen=True, slots=True)
class _Cursor:
    segment_id: int
    byte_offset: int


@dataclass(frozen=True, slots=True)
class _StoredEnvelope:
    message_id: str
    segment_id: int
    start_offset: int
    end_offset: int


@dataclass(slots=True)
class _Segment:
    segment_id: int
    path: Path
    byte_size: int
    record_count: int


@dataclass(frozen=True, slots=True)
class _LegacyRecord:
    operation: str
    message_id: str
    envelope: MessageEnvelope | None = None


_FORMAT_NAME = "reliomq-segmented-outbox"
_FORMAT_VERSION = 1
_CHECKPOINT_VERSION = 1
_LEGACY_JOURNAL_VERSION = 1

_SEGMENT_MAGIC = b"RLMQSEG1"
_SEGMENT_HEADER = struct.Struct(">8sQ")
_RECORD_MAGIC = b"MSG1"
_RECORD_HEADER = struct.Struct(">4sII")
_MAX_RECORD_BYTES = (1 << 32) - 1
_MAX_SEGMENT_ID = (1 << 64) - 1

_SEGMENT_PATTERN = re.compile(r"^segment-(\d{20})\.dat$")
_FORMAT_FILENAME = "format.json"
_CHECKPOINT_FILENAME = "checkpoint.json"

_LEGACY_ENQUEUE = "enqueue"
_LEGACY_ACK = "ack"


class Outbox:
    """A process-local, segmented persistent FIFO of message envelopes.

    ``path`` may name a new segmented directory or an existing legacy JSONL
    file. Legacy files are replayed into an atomically installed sibling
    directory named ``<path>.segments`` and are deliberately retained. Once
    present, that sidecar is authoritative on subsequent opens.

    The highest numbered segment is active and appendable; older segments are
    immutable. The logical head is a ``(segment_id, byte_offset)`` cursor.
    ``remove_oldest(sync=True)`` durably checkpoints that cursor before it
    removes the live head. ``sync=False`` advances only the process-local
    cursor so a policy such as GroupMode can checkpoint several ACKs together.

    Exactly one live Outbox instance may own writes to a path, including
    within a single process. Its lock protects threads sharing that instance;
    it is not a cross-instance or cross-process lock. Payloads stay on disk;
    the runtime index retains only IDs and record offsets.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        logger: logging.Logger | None = None,
        *,
        segment_max_bytes: int = 8 * 1024 * 1024,
        segment_max_records: int = 10_000,
    ) -> None:
        try:
            raw_path = os.fspath(path)
        except TypeError as error:
            raise ValueError("path must be a filesystem path") from error
        if isinstance(raw_path, bytes) or not raw_path or "\x00" in raw_path:
            raise ValueError("path must be a non-empty text filesystem path")
        if type(segment_max_bytes) is not int or segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes must be a positive integer")
        if type(segment_max_records) is not int or segment_max_records <= 0:
            raise ValueError("segment_max_records must be a positive integer")

        self._path = Path(raw_path).expanduser()
        self._logger = logger or logging.getLogger(__name__)
        self._segment_max_bytes = segment_max_bytes
        self._segment_max_records = segment_max_records
        self._lock = threading.RLock()

        self._segments: dict[int, _Segment] = {}
        self._pending: deque[_StoredEnvelope] = deque()
        self._pending_by_id: dict[str, _StoredEnvelope] = {}
        # IDs removed only from the volatile GroupMode cursor cannot safely
        # be reused until that cursor is checkpointed: restart would otherwise
        # see both the old and replacement records as pending.
        self._volatile_completed_ids: set[str] = set()
        self._volatile_cursor = _Cursor(1, _SEGMENT_HEADER.size)
        self._checkpoint_cursor = self._volatile_cursor
        self._checkpoint_generation = 0
        self._checkpoint_needs_rewrite = False
        self._data_sync_pending = False
        self._data_sync_generation = 0

        with self._lock:
            self._storage_path = self._resolve_or_create_storage()
            self._load_segmented_storage()
            # A process-only restart can recover GroupMode bytes that never
            # left the OS cache. Establish a data boundary for the active
            # segment before a fresh policy starts with zero dirty counters.
            self._data_sync_pending = self._active_segment_locked().record_count > 0
            self._sync_data_locked()
            # A preceding owner may have failed after rename but before its
            # directory barrier. Reopening must establish that barrier before
            # accepting new durable messages in the surviving namespace.
            try:
                self._fsync_directory(self._storage_path)
                # Include ancestors because parent directories may themselves
                # have been created by an interrupted queue installation.
                for ancestor in self._storage_path.absolute().parents:
                    self._fsync_directory(ancestor)
            except OSError as error:
                raise OutboxError(
                    f"cannot sync recovered Outbox directories: {error}"
                ) from error

        self._logger.info(
            "Outbox opened | path=%s | storage_path=%s | pending=%s",
            self._path,
            self._storage_path,
            len(self._pending),
        )

    @property
    def path(self) -> Path:
        """Return the configured path (preserved for API compatibility)."""

        return self._path

    @property
    def storage_path(self) -> Path:
        """Return the directory containing segments and checkpoint state."""

        return self._storage_path

    @property
    def data_sync_generation(self) -> int:
        """Process-local count of completed segment data sync boundaries."""

        with self._lock:
            return self._data_sync_generation

    @property
    def checkpoint_generation(self) -> int:
        """Generation of the latest successfully installed head checkpoint."""

        with self._lock:
            return self._checkpoint_generation

    @classmethod
    def record_size(cls, envelope: MessageEnvelope) -> int:
        """Return the exact number of bytes in one framed segment record."""

        cls._require_envelope(envelope)
        payload = envelope.to_bytes()
        if len(payload) > _MAX_RECORD_BYTES:
            raise OutboxError("message envelope is too large for the segment format")
        return _RECORD_HEADER.size + len(payload)

    def append(self, envelope: MessageEnvelope, *, sync: bool = True) -> bool:
        """Append one envelope, preserving the historical boolean result."""

        self._require_envelope(envelope)
        self._require_sync_flag(sync)
        with self._lock:
            if envelope.message_id in self._pending_by_id:
                self._logger.debug(
                    "Append skipped; message_id already pending | message_id=%s",
                    envelope.message_id,
                )
                return False
            result = self._append_many_locked((envelope,), sync=sync)
            return result.appended_count == 1

    def append_many(
        self,
        envelopes: Iterable[MessageEnvelope],
        *,
        sync: bool = True,
    ) -> AppendResult:
        """Append a batch with one sequential write per touched segment.

        With ``sync=True``, each touched segment is fsync-confirmed before the
        method returns. Exact existing envelopes are accepted as idempotent
        retry input; reusing a pending ID for different content raises.
        """

        self._require_sync_flag(sync)
        try:
            batch = tuple(envelopes)
        except TypeError as error:
            raise TypeError("envelopes must be an iterable") from error
        for envelope in batch:
            self._require_envelope(envelope)
        with self._lock:
            return self._append_many_locked(batch, sync=sync)

    def sync(self) -> None:
        """Fsync all deferred segment appends (checkpoint state is separate)."""

        with self._lock:
            self._sync_data_locked()

    def checkpoint(self) -> None:
        """Sync data, persist the live head cursor, then clean old segments."""

        with self._lock:
            self._sync_data_locked()
            cursor = self._normalized_volatile_cursor_locked()
            if cursor != self._checkpoint_cursor or self._checkpoint_needs_rewrite:
                self._persist_checkpoint_locked(cursor)
                self._checkpoint_cursor = cursor
            self._volatile_cursor = cursor
            self._volatile_completed_ids.clear()
            self._cleanup_completed_segments_locked()

    def load(self) -> list[MessageEnvelope]:
        """Load pending envelopes in FIFO order."""

        with self._lock:
            return [self._read_envelope_locked(item) for item in self._pending]

    def pending_ids(self) -> list[str]:
        """Return the FIFO's IDs without materializing stored payloads."""

        with self._lock:
            return [item.message_id for item in self._pending]

    def get(self, message_id: str) -> MessageEnvelope | None:
        """Read one pending envelope by ID without loading the backlog."""

        validate_message_id(message_id)
        with self._lock:
            item = self._pending_by_id.get(message_id)
            return None if item is None else self._read_envelope_locked(item)

    def peek_oldest(self) -> MessageEnvelope | None:
        """Return the oldest pending envelope without changing storage."""

        with self._lock:
            return (
                self._read_envelope_locked(self._pending[0])
                if self._pending
                else None
            )

    def remove_oldest(
        self,
        expected: MessageEnvelope,
        *,
        sync: bool = True,
    ) -> bool:
        """Advance the FIFO head only when it exactly matches ``expected``."""

        self._require_envelope(expected)
        self._require_sync_flag(sync)
        expected_bytes = expected.to_bytes()
        with self._lock:
            if not self._pending:
                return False
            oldest = self._pending[0]
            if oldest.message_id != expected.message_id or (
                self._read_envelope_locked(oldest).to_bytes() != expected_bytes
            ):
                return False

            next_cursor = self._cursor_after_oldest_locked()
            if sync:
                self._sync_data_locked()
                self._persist_checkpoint_locked(next_cursor)

            removed = self._pending.popleft()
            self._pending_by_id.pop(removed.message_id, None)
            self._volatile_cursor = next_cursor
            if sync:
                self._checkpoint_cursor = next_cursor
                self._volatile_completed_ids.clear()
                self._cleanup_completed_segments_locked()
            else:
                self._volatile_completed_ids.add(expected.message_id)
            self._logger.debug(
                "%s Outbox head | message_id=%s | remaining=%s",
                "Checkpointed" if sync else "Advanced volatile",
                expected.message_id,
                len(self._pending),
            )
            return True

    def compact(self) -> None:
        """Checkpoint and delete completed segments without rewriting payloads."""

        self.checkpoint()

    def completed_closed_segment_pending(self) -> bool:
        """Return whether the live cursor has fully consumed a closed segment."""

        with self._lock:
            target_id = self._normalized_volatile_cursor_locked().segment_id
            active_id = self._active_segment_locked().segment_id
            return any(
                segment_id < target_id and segment_id < active_id
                for segment_id in self._segments
            )

    def size(self) -> int:
        with self._lock:
            return len(self._pending)

    def contains(
        self,
        message_id: str | None = None,
        *,
        event_id: str | None = None,
    ) -> bool:
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
            return resolved_id in self._pending_by_id

    def __len__(self) -> int:
        return self.size()

    # Appending and rotation -------------------------------------------------

    def _append_many_locked(
        self, batch: tuple[MessageEnvelope, ...], *, sync: bool
    ) -> AppendResult:
        if not batch:
            if sync:
                self._sync_data_locked()
            return AppendResult(0, 0, False)

        if any(
            envelope.message_id in self._volatile_completed_ids
            for envelope in batch
        ):
            # Preserve the historical ability to reuse a completed ID while
            # ensuring a GroupMode volatile ACK cannot make restart observe
            # two pending records with that ID.
            self.checkpoint()

        unique_new: list[tuple[MessageEnvelope, bytes]] = []
        batch_by_id: dict[str, bytes] = {}
        for envelope in batch:
            encoded = envelope.to_bytes()
            snapshot = MessageEnvelope.from_bytes(encoded)
            earlier = batch_by_id.get(envelope.message_id)
            if earlier is not None:
                if earlier != encoded:
                    raise OutboxError(
                        "batch reuses message_id with different content: "
                        f"{envelope.message_id!r}"
                    )
                continue
            batch_by_id[envelope.message_id] = encoded
            existing = self._pending_by_id.get(envelope.message_id)
            if existing is not None:
                if self._read_envelope_locked(existing).to_bytes() != encoded:
                    raise OutboxError(
                        "pending message_id has different content: "
                        f"{envelope.message_id!r}"
                    )
                continue
            unique_new.append((snapshot, self._encode_frame(encoded)))

        if not unique_new:
            if sync:
                self._sync_data_locked()
            return AppendResult(0, 0, False)

        appended_count = 0
        bytes_written = 0
        rotated = False
        remaining = unique_new
        while remaining:
            active = self._active_segment_locked()
            if self._would_rotate_locked(active, len(remaining[0][1])):
                self._rotate_locked()
                rotated = True
                active = self._active_segment_locked()

            available_records = self._segment_max_records - active.record_count
            available_bytes = self._segment_max_bytes - active.byte_size
            chunk: list[tuple[MessageEnvelope, bytes]] = []
            chunk_bytes = 0
            for envelope, frame in remaining:
                frame_size = len(frame)
                if chunk and (
                    len(chunk) >= available_records
                    or chunk_bytes + frame_size > available_bytes
                ):
                    break
                if not chunk and frame_size > available_bytes:
                    # An oversized envelope is allowed as a singleton segment.
                    chunk.append((envelope, frame))
                    chunk_bytes += frame_size
                    break
                if (
                    len(chunk) >= available_records
                    or chunk_bytes + frame_size > available_bytes
                ):
                    break
                chunk.append((envelope, frame))
                chunk_bytes += frame_size

            if not chunk:
                self._rotate_locked()
                rotated = True
                continue

            more_after_chunk = len(chunk) < len(remaining)
            self._write_chunk_locked(chunk, sync=(sync or more_after_chunk))
            appended_count += len(chunk)
            bytes_written += chunk_bytes
            remaining = remaining[len(chunk) :]
            if remaining:
                self._rotate_locked()
                rotated = True
        return AppendResult(appended_count, bytes_written, rotated)

    def _write_chunk_locked(
        self, chunk: list[tuple[MessageEnvelope, bytes]], *, sync: bool
    ) -> None:
        segment = self._active_segment_locked()
        original_size = segment.byte_size
        blob = b"".join(frame for _envelope, frame in chunk)
        try:
            with segment.path.open("r+b") as file:
                file.seek(0, os.SEEK_END)
                if file.tell() != original_size:
                    raise OutboxError(
                        "Outbox segment changed outside its owning instance; "
                        "refusing to overwrite queued data"
                    )
                file.seek(original_size)
                try:
                    file.write(blob)
                    file.flush()
                    if sync:
                        os.fsync(file.fileno())
                except OSError:
                    try:
                        file.seek(original_size)
                        file.truncate()
                        file.flush()
                        if sync:
                            os.fsync(file.fileno())
                    except OSError:
                        self._logger.warning(
                            "Could not roll back failed segment append | path=%s",
                            segment.path,
                            exc_info=True,
                        )
                    raise
        except OSError as error:
            raise OutboxError(
                f"cannot append to Outbox segment {segment.path}: {error}"
            ) from error

        offset = original_size
        for envelope, frame in chunk:
            end_offset = offset + len(frame)
            stored = _StoredEnvelope(
                envelope.message_id, segment.segment_id, offset, end_offset
            )
            self._pending.append(stored)
            self._pending_by_id[envelope.message_id] = stored
            offset = end_offset
        segment.byte_size += len(blob)
        segment.record_count += len(chunk)
        self._data_sync_pending = not sync
        if sync:
            self._data_sync_generation += 1

    def _would_rotate_locked(self, segment: _Segment, frame_size: int) -> bool:
        if segment.record_count == 0:
            return False
        return (
            segment.record_count >= self._segment_max_records
            or segment.byte_size + frame_size > self._segment_max_bytes
        )

    def _rotate_locked(self) -> None:
        self._sync_data_locked()
        old_active = self._active_segment_locked()
        next_id = old_active.segment_id + 1
        if next_id > _MAX_SEGMENT_ID:
            raise OutboxError("Outbox segment identifier space is exhausted")
        final_path = self._segment_path(next_id)
        if final_path.exists():
            raise OutboxError(f"next Outbox segment already exists: {final_path}")
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{final_path.name}.",
                suffix=".tmp",
                dir=str(self._storage_path),
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as file:
                file.write(self._encode_segment_header(next_id))
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, final_path)
            temporary_path = None
            self._fsync_directory(self._storage_path)
        except OSError as error:
            raise OutboxError(f"cannot rotate Outbox segment: {error}") from error
        finally:
            if temporary_path is not None:
                self._unlink_temporary(temporary_path)

        self._segments[next_id] = _Segment(
            next_id, final_path, _SEGMENT_HEADER.size, 0
        )
        if not self._pending:
            self._volatile_cursor = _Cursor(next_id, _SEGMENT_HEADER.size)

    def _sync_data_locked(self) -> None:
        if not self._data_sync_pending:
            return
        segment = self._active_segment_locked()
        try:
            with segment.path.open("r+b") as file:
                os.fsync(file.fileno())
        except OSError as error:
            raise OutboxError(
                f"cannot sync Outbox segment {segment.path}: {error}"
            ) from error
        self._data_sync_pending = False
        self._data_sync_generation += 1

    # Checkpoint and cleanup -------------------------------------------------

    def _cursor_after_oldest_locked(self) -> _Cursor:
        if len(self._pending) > 1:
            following = self._pending[1]
            return _Cursor(following.segment_id, following.start_offset)
        active = self._active_segment_locked()
        return _Cursor(active.segment_id, active.byte_size)

    def _normalized_volatile_cursor_locked(self) -> _Cursor:
        if self._pending:
            head = self._pending[0]
            return _Cursor(head.segment_id, head.start_offset)
        active = self._active_segment_locked()
        return _Cursor(active.segment_id, active.byte_size)

    def _persist_checkpoint_locked(self, cursor: _Cursor) -> None:
        generation = self._checkpoint_generation + 1
        encoded = self._canonical_json(
            {
                "version": _CHECKPOINT_VERSION,
                "generation": generation,
                "segment_id": cursor.segment_id,
                "byte_offset": cursor.byte_offset,
            }
        ) + b"\n"
        final_path = self._storage_path / _CHECKPOINT_FILENAME
        temporary_path: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{_CHECKPOINT_FILENAME}.",
                suffix=".tmp",
                dir=str(self._storage_path),
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as file:
                file.write(encoded)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, final_path)
            temporary_path = None
            self._fsync_directory(self._storage_path)
        except OSError as error:
            raise OutboxError(
                f"cannot checkpoint Outbox {self._storage_path}: {error}"
            ) from error
        finally:
            if temporary_path is not None:
                self._unlink_temporary(temporary_path)
        self._checkpoint_generation = generation
        self._checkpoint_needs_rewrite = False

    def _cleanup_completed_segments_locked(self) -> None:
        active_id = self._active_segment_locked().segment_id
        removable = [
            segment_id
            for segment_id in sorted(self._segments)
            if segment_id < self._checkpoint_cursor.segment_id
            and segment_id < active_id
        ]
        removed_any = False
        for segment_id in removable:
            segment = self._segments[segment_id]
            try:
                segment.path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                self._logger.warning(
                    "Could not delete completed Outbox segment | path=%s",
                    segment.path,
                    exc_info=True,
                )
                continue
            self._segments.pop(segment_id, None)
            removed_any = True
        if removed_any:
            try:
                self._fsync_directory(self._storage_path)
            except OSError:
                # The checkpoint is already safe. A failed cleanup-directory
                # sync may resurrect only completed segments after a crash.
                self._logger.warning(
                    "Could not sync completed-segment cleanup; checkpoint remains safe",
                    exc_info=True,
                )

    # Opening, scanning, and migration --------------------------------------

    def _resolve_or_create_storage(self) -> Path:
        sidecar = Path(f"{self._path}.segments")
        try:
            path_exists = self._path.exists()
            sidecar_exists = sidecar.exists()
        except OSError as error:
            raise OutboxError(f"cannot inspect Outbox path: {error}") from error

        if self._path.is_dir():
            if sidecar_exists:
                raise OutboxError(
                    f"ambiguous Outbox storage: both {self._path} and {sidecar} exist"
                )
            return self._path
        if sidecar_exists:
            if not sidecar.is_dir():
                raise OutboxError(f"Outbox sidecar is not a directory: {sidecar}")
            if path_exists and not self._path.is_file():
                raise OutboxError(f"legacy Outbox path is not a regular file: {self._path}")
            return sidecar
        if path_exists:
            if not self._path.is_file():
                raise OutboxError(f"Outbox path is not a regular file: {self._path}")
            pending = self._read_legacy_pending(self._path)
            self._install_storage_atomically(sidecar, pending)
            self._logger.info(
                "Migrated legacy Outbox without deleting source | source=%s | storage_path=%s | pending=%s",
                self._path,
                sidecar,
                len(pending),
            )
            return sidecar
        self._install_storage_atomically(self._path, [])
        return self._path

    def _install_storage_atomically(
        self, destination: Path, pending: list[MessageEnvelope]
    ) -> None:
        self._ensure_parent_directory(destination)
        temporary_root: Path | None = None
        try:
            temporary_root = Path(
                tempfile.mkdtemp(
                    prefix=f".{destination.name}.",
                    suffix=".tmp",
                    dir=str(destination.parent),
                )
            )
            self._build_storage_directory(temporary_root, pending)
            os.replace(temporary_root, destination)
            temporary_root = None
            self._fsync_directory(destination.parent)
        except OSError as error:
            raise OutboxError(
                f"cannot create segmented Outbox {destination}: {error}"
            ) from error
        finally:
            if temporary_root is not None:
                try:
                    shutil.rmtree(temporary_root)
                except FileNotFoundError:
                    pass
                except OSError:
                    self._logger.warning(
                        "Could not remove temporary Outbox directory %s",
                        temporary_root,
                    )

    def _build_storage_directory(
        self, root: Path, pending: list[MessageEnvelope]
    ) -> None:
        format_bytes = self._canonical_json(
            {"format": _FORMAT_NAME, "version": _FORMAT_VERSION}
        ) + b"\n"
        self._write_new_file_synced(root / _FORMAT_FILENAME, format_bytes)
        segment_id = 1
        frames: list[bytes] = []
        segment_size = _SEGMENT_HEADER.size
        for envelope in pending:
            frame = self._encode_frame(envelope.to_bytes())
            if frames and (
                len(frames) >= self._segment_max_records
                or segment_size + len(frame) > self._segment_max_bytes
            ):
                self._write_built_segment(root, segment_id, frames)
                segment_id += 1
                frames = []
                segment_size = _SEGMENT_HEADER.size
            frames.append(frame)
            segment_size += len(frame)
        self._write_built_segment(root, segment_id, frames)
        checkpoint = self._canonical_json(
            {
                "version": _CHECKPOINT_VERSION,
                "generation": 0,
                "segment_id": 1,
                "byte_offset": _SEGMENT_HEADER.size,
            }
        ) + b"\n"
        self._write_new_file_synced(root / _CHECKPOINT_FILENAME, checkpoint)
        self._fsync_directory(root)

    def _write_built_segment(
        self, root: Path, segment_id: int, frames: list[bytes]
    ) -> None:
        data = self._encode_segment_header(segment_id) + b"".join(frames)
        self._write_new_file_synced(root / self._segment_filename(segment_id), data)

    @staticmethod
    def _write_new_file_synced(path: Path, data: bytes) -> None:
        with path.open("xb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

    def _load_segmented_storage(self) -> None:
        self._validate_format_file()
        try:
            children = list(self._storage_path.iterdir())
        except OSError as error:
            raise OutboxError(
                f"cannot list Outbox directory {self._storage_path}: {error}"
            ) from error
        segment_paths: list[tuple[int, Path]] = []
        for child in children:
            match = _SEGMENT_PATTERN.fullmatch(child.name)
            if match is not None:
                segment_id = int(match.group(1))
                if segment_id <= 0 or segment_id > _MAX_SEGMENT_ID:
                    raise OutboxError(f"Outbox segment id is out of range: {child}")
                segment_paths.append((segment_id, child))
        segment_paths.sort()
        if not segment_paths:
            raise OutboxError(f"Outbox has no segment files: {self._storage_path}")

        all_entries: list[_StoredEnvelope] = []
        active_id = segment_paths[-1][0]
        for segment_id, path in segment_paths:
            if not path.is_file():
                raise OutboxError(f"Outbox segment is not a regular file: {path}")
            segment, entries = self._scan_segment(
                segment_id, path, active=(segment_id == active_id)
            )
            if segment_id in self._segments:
                raise OutboxError(f"duplicate Outbox segment id: {segment_id}")
            self._segments[segment_id] = segment
            all_entries.extend(entries)

        checkpoint = self._read_checkpoint_conservatively()
        boundaries: dict[int, set[int]] = {
            segment_id: {_SEGMENT_HEADER.size, segment.byte_size}
            for segment_id, segment in self._segments.items()
        }
        for entry in all_entries:
            boundaries[entry.segment_id].add(entry.start_offset)
            boundaries[entry.segment_id].add(entry.end_offset)
        if (
            checkpoint.segment_id not in self._segments
            or checkpoint.byte_offset not in boundaries[checkpoint.segment_id]
        ):
            earliest_id = min(self._segments)
            self._logger.warning(
                "Invalid Outbox checkpoint; replaying from earliest surviving segment | path=%s",
                self._storage_path / _CHECKPOINT_FILENAME,
            )
            checkpoint = _Cursor(earliest_id, _SEGMENT_HEADER.size)
            self._checkpoint_generation = 0
            self._checkpoint_needs_rewrite = True

        # Cleanup can legitimately leave an old, already-acknowledged segment
        # behind when one unlink fails. Missing IDs at or after the persisted
        # head, however, would mean silent loss of unacknowledged records.
        surviving_from_head = sorted(
            segment_id
            for segment_id in self._segments
            if segment_id >= checkpoint.segment_id
        )
        if (
            not surviving_from_head
            or surviving_from_head[0] != checkpoint.segment_id
            or any(
                following != previous + 1
                for previous, following in zip(
                    surviving_from_head, surviving_from_head[1:]
                )
            )
        ):
            raise OutboxError(
                "missing Outbox segment at or after the persisted checkpoint"
            )

        pending = [
            entry
            for entry in all_entries
            if entry.segment_id > checkpoint.segment_id
            or (
                entry.segment_id == checkpoint.segment_id
                and entry.start_offset >= checkpoint.byte_offset
            )
        ]
        pending_by_id: dict[str, _StoredEnvelope] = {}
        for entry in pending:
            if entry.message_id in pending_by_id:
                raise OutboxError(
                    "duplicate pending message_id in segmented Outbox: "
                    f"{entry.message_id!r}"
                )
            pending_by_id[entry.message_id] = entry
        self._pending = deque(pending)
        self._pending_by_id = pending_by_id
        self._checkpoint_cursor = checkpoint
        self._volatile_cursor = self._normalized_volatile_cursor_locked()
        self._volatile_completed_ids = set()
        self._data_sync_pending = False

    def _validate_format_file(self) -> None:
        path = self._storage_path / _FORMAT_FILENAME
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise OutboxError(f"cannot read Outbox format metadata {path}: {error}") from error
        if value != {"format": _FORMAT_NAME, "version": _FORMAT_VERSION}:
            raise OutboxError(f"unsupported Outbox format metadata: {path}")

    def _read_checkpoint_conservatively(self) -> _Cursor:
        path = self._storage_path / _CHECKPOINT_FILENAME
        fallback_error: Exception | None = None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if type(value) is not dict or set(value) != {
                "version", "generation", "segment_id", "byte_offset"
            }:
                raise ValueError("invalid schema")
            version = value["version"]
            generation = value["generation"]
            segment_id = value["segment_id"]
            byte_offset = value["byte_offset"]
            if type(version) is not int or version != _CHECKPOINT_VERSION:
                raise ValueError("unsupported version")
            if any(
                type(item) is not int or item < 0
                for item in (generation, segment_id, byte_offset)
            ):
                raise ValueError("checkpoint values must be non-negative integers")
        except OSError as error:
            if not isinstance(error, FileNotFoundError):
                raise OutboxError(
                    f"cannot read Outbox checkpoint {path}: {error}"
                ) from error
            fallback_error = error
        except (UnicodeError, json.JSONDecodeError, ValueError, KeyError) as error:
            fallback_error = error

        if fallback_error is not None:
            earliest_id = min(self._segments)
            self._logger.warning(
                "Missing or corrupt Outbox checkpoint; replaying conservatively | path=%s | error=%s",
                path,
                fallback_error,
            )
            self._checkpoint_generation = 0
            self._checkpoint_needs_rewrite = True
            return _Cursor(earliest_id, _SEGMENT_HEADER.size)
        self._checkpoint_generation = generation
        self._checkpoint_needs_rewrite = False
        return _Cursor(segment_id, byte_offset)

    def _scan_segment(
        self, segment_id: int, path: Path, *, active: bool
    ) -> tuple[_Segment, list[_StoredEnvelope]]:
        entries: list[_StoredEnvelope] = []
        try:
            with path.open("rb") as file:
                file.seek(0, os.SEEK_END)
                physical_size = file.tell()
                file.seek(0)
                header = file.read(_SEGMENT_HEADER.size)
                if len(header) != _SEGMENT_HEADER.size:
                    raise OutboxError(f"truncated Outbox segment header: {path}")
                magic, stored_id = _SEGMENT_HEADER.unpack(header)
                if magic != _SEGMENT_MAGIC or stored_id != segment_id:
                    raise OutboxError(f"invalid Outbox segment header: {path}")
                offset = _SEGMENT_HEADER.size
                torn_at: int | None = None
                while True:
                    file.seek(offset)
                    raw_header = file.read(_RECORD_HEADER.size)
                    if not raw_header:
                        break
                    if len(raw_header) != _RECORD_HEADER.size:
                        torn_at = offset
                        break
                    record_magic, payload_size, expected_crc = _RECORD_HEADER.unpack(
                        raw_header
                    )
                    if record_magic != _RECORD_MAGIC:
                        raise OutboxError(
                            f"invalid record framing in Outbox segment {path} at offset {offset}"
                        )
                    payload_offset = offset + _RECORD_HEADER.size
                    if payload_size > physical_size - payload_offset:
                        torn_at = offset
                        break
                    payload = file.read(payload_size)
                    if len(payload) != payload_size:
                        torn_at = offset
                        break
                    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                    if actual_crc != expected_crc:
                        raise OutboxError(
                            f"record checksum mismatch in Outbox segment {path} at offset {offset}"
                        )
                    try:
                        envelope = MessageEnvelope.from_bytes(payload)
                    except ProtocolError as error:
                        raise OutboxError(
                            f"invalid envelope in Outbox segment {path} at offset {offset}: {error}"
                        ) from error
                    end_offset = offset + _RECORD_HEADER.size + payload_size
                    entries.append(
                        _StoredEnvelope(
                            envelope.message_id, segment_id, offset, end_offset
                        )
                    )
                    offset = end_offset
        except OutboxError:
            raise
        except OSError as error:
            raise OutboxError(f"cannot read Outbox segment {path}: {error}") from error

        if torn_at is not None:
            if not active:
                raise OutboxError(
                    f"truncated record in closed Outbox segment {path} at offset {torn_at}"
                )
            self._logger.warning(
                "Repairing torn active Outbox segment tail | path=%s | offset=%s",
                path,
                torn_at,
            )
            try:
                with path.open("r+b") as file:
                    file.truncate(torn_at)
                    file.flush()
                    os.fsync(file.fileno())
            except OSError as error:
                raise OutboxError(
                    f"cannot repair torn Outbox segment {path}: {error}"
                ) from error
            byte_size = torn_at
        else:
            try:
                byte_size = path.stat().st_size
            except OSError as error:
                raise OutboxError(f"cannot inspect Outbox segment {path}: {error}") from error
        return _Segment(segment_id, path, byte_size, len(entries)), entries

    # Legacy journal reader --------------------------------------------------

    def _read_legacy_pending(self, path: Path) -> list[MessageEnvelope]:
        try:
            raw_records = path.read_bytes().splitlines()
        except OSError as error:
            raise OutboxError(f"cannot read legacy Outbox {path}: {error}") from error
        pending: dict[str, MessageEnvelope] = {}
        for line_number, raw in enumerate(raw_records, start=1):
            try:
                record = self._decode_legacy_record(raw)
            except ProtocolError as error:
                self._logger.warning(
                    "Skipping corrupt legacy Outbox record during migration | path=%s | line=%s | error=%s",
                    path,
                    line_number,
                    error,
                )
                continue
            if record.operation == _LEGACY_ENQUEUE:
                assert record.envelope is not None
                if record.message_id in pending:
                    self._logger.warning(
                        "Skipping duplicate pending legacy message_id | path=%s | line=%s | message_id=%s",
                        path,
                        line_number,
                        record.message_id,
                    )
                    continue
                pending[record.message_id] = record.envelope
                continue
            oldest_id = next(iter(pending), None)
            if record.message_id == oldest_id:
                del pending[record.message_id]
            elif record.message_id in pending:
                self._logger.warning(
                    "Ignoring out-of-order legacy ACK during migration | path=%s | line=%s | message_id=%s",
                    path,
                    line_number,
                    record.message_id,
                )
        return list(pending.values())

    @classmethod
    def _decode_legacy_record(cls, raw: bytes) -> _LegacyRecord:
        value = cls._parse_json(raw)
        if not isinstance(value, dict) or (
            "journal_version" not in value and "op" not in value
        ):
            envelope = MessageEnvelope.from_bytes(raw)
            return _LegacyRecord(_LEGACY_ENQUEUE, envelope.message_id, envelope)
        if value.get("journal_version") != _LEGACY_JOURNAL_VERSION:
            raise ProtocolError(
                f"journal_version must be exactly {_LEGACY_JOURNAL_VERSION}"
            )
        operation = value.get("op")
        if operation == _LEGACY_ENQUEUE:
            if set(value) != {"journal_version", "op", "envelope"}:
                raise ProtocolError("enqueue journal record has an invalid schema")
            try:
                nested = cls._canonical_json(value["envelope"])
            except OutboxError as error:
                raise ProtocolError(str(error)) from error
            envelope = MessageEnvelope.from_bytes(nested)
            return _LegacyRecord(_LEGACY_ENQUEUE, envelope.message_id, envelope)
        if operation == _LEGACY_ACK:
            if set(value) != {"journal_version", "op", "message_id"}:
                raise ProtocolError("ack journal record has an invalid schema")
            message_id = validate_message_id(value["message_id"])
            return _LegacyRecord(_LEGACY_ACK, message_id)
        raise ProtocolError(f"unsupported journal operation {operation!r}")

    # Retained for migration tests/tooling from the short-lived v1 journal.
    @classmethod
    def _encode_enqueue(cls, envelope: MessageEnvelope) -> bytes:
        cls._require_envelope(envelope)
        try:
            nested = json.loads(envelope.to_bytes().decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise OutboxError(f"cannot encode legacy enqueue record: {error}") from error
        return cls._canonical_json(
            {
                "journal_version": _LEGACY_JOURNAL_VERSION,
                "op": _LEGACY_ENQUEUE,
                "envelope": nested,
            }
        )

    @classmethod
    def _encode_ack(cls, message_id: str) -> bytes:
        validate_message_id(message_id)
        return cls._canonical_json(
            {
                "journal_version": _LEGACY_JOURNAL_VERSION,
                "op": _LEGACY_ACK,
                "message_id": message_id,
            }
        )

    # Encoding and filesystem helpers ---------------------------------------

    @staticmethod
    def _require_envelope(value: Any) -> None:
        if not isinstance(value, MessageEnvelope):
            raise TypeError("Outbox operations require a MessageEnvelope")

    def _read_envelope_locked(self, item: _StoredEnvelope) -> MessageEnvelope:
        """Seek directly to one indexed record and validate its stored bytes."""

        path = self._segment_path(item.segment_id)
        try:
            with path.open("rb") as file:
                file.seek(item.start_offset)
                raw = file.read(item.end_offset - item.start_offset)
            if len(raw) != item.end_offset - item.start_offset:
                raise OutboxError(f"truncated indexed Outbox record: {path}")
            magic, size, crc = _RECORD_HEADER.unpack(raw[:_RECORD_HEADER.size])
            payload = raw[_RECORD_HEADER.size:]
            if (
                magic != _RECORD_MAGIC
                or size != len(payload)
                or zlib.crc32(payload) & 0xFFFFFFFF != crc
            ):
                raise OutboxError(f"corrupt indexed Outbox record: {path}")
            envelope = MessageEnvelope.from_bytes(payload)
            if envelope.message_id != item.message_id:
                raise OutboxError(f"indexed Outbox message_id changed: {path}")
            return envelope
        except (OSError, ProtocolError, struct.error) as error:
            raise OutboxError(f"cannot read indexed Outbox record {path}: {error}") from error

    @staticmethod
    def _require_sync_flag(value: Any) -> None:
        if not isinstance(value, bool):
            raise TypeError("sync must be a bool")

    @staticmethod
    def _encode_segment_header(segment_id: int) -> bytes:
        return _SEGMENT_HEADER.pack(_SEGMENT_MAGIC, segment_id)

    @staticmethod
    def _encode_frame(payload: bytes) -> bytes:
        if len(payload) > _MAX_RECORD_BYTES:
            raise OutboxError("message envelope is too large for the segment format")
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        return _RECORD_HEADER.pack(_RECORD_MAGIC, len(payload), checksum) + payload

    @staticmethod
    def _segment_filename(segment_id: int) -> str:
        return f"segment-{segment_id:020d}.dat"

    def _segment_path(self, segment_id: int) -> Path:
        return self._storage_path / self._segment_filename(segment_id)

    def _active_segment_locked(self) -> _Segment:
        return self._segments[max(self._segments)]

    @staticmethod
    def _canonical_json(value: Any) -> bytes:
        try:
            return json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError, RecursionError) as error:
            raise OutboxError(f"cannot encode Outbox metadata: {error}") from error

    @staticmethod
    def _reject_json_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant {value}")

    @staticmethod
    def _object_without_duplicate_keys(
        pairs: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    @classmethod
    def _parse_json(cls, raw: bytes) -> Any:
        try:
            text = raw.decode("utf-8")
            return json.loads(
                text,
                object_pairs_hook=cls._object_without_duplicate_keys,
                parse_constant=cls._reject_json_constant,
            )
        except (
            UnicodeError,
            json.JSONDecodeError,
            ValueError,
            TypeError,
            RecursionError,
        ) as error:
            raise ProtocolError(f"journal record is not valid JSON: {error}") from error

    @staticmethod
    def _ensure_parent_directory(path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise OutboxError(f"cannot create Outbox parent {path.parent}: {error}") from error

    def _unlink_temporary(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            self._logger.warning("Could not remove temporary Outbox file %s", path)

    def _fsync_directory(self, path: Path) -> None:
        """Sync directory metadata where supported; propagate real I/O errors."""

        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        descriptor: int | None = None
        try:
            descriptor = os.open(path, flags)
            os.fsync(descriptor)
        except OSError as error:
            unsupported = {errno.EINVAL, errno.ENOTSUP, errno.ENOSYS}
            if os.name == "nt":
                unsupported.update((errno.EACCES, errno.EPERM))
                # The Windows CRT reports ENOENT when opening an existing
                # drive root as a file descriptor. Do not hide genuinely
                # missing child directories under that compatibility case.
                if error.errno == errno.ENOENT and path == Path(path.anchor) and path.is_dir():
                    unsupported.add(errno.ENOENT)
            if error.errno not in unsupported:
                raise
            self._logger.debug("Directory fsync unavailable for %s: %s", path, error)
        finally:
            if descriptor is not None:
                os.close(descriptor)


DurableMessageStore = Outbox
