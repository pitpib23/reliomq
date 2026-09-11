"""Immutable client-level persistence and delivery modes.

A :class:`~reliomq.sender.Sender` selects exactly one of these mode objects
when it is constructed. Modes are deliberately data-only configuration; the
runtime state machines live in :mod:`reliomq.persistence` and
:mod:`reliomq.sender`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TypeAlias


def _positive_integer(value: object, name: str) -> int:
    """Validate an integer threshold without accepting ``bool``."""

    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _optional_positive_integer(value: object, name: str) -> int | None:
    """Validate an optional integer trigger; ``None`` disables it."""

    if value is None:
        return None
    return _positive_integer(value, name)


def _finite_number(value: object, name: str, *, allow_zero: bool) -> float:
    """Validate and normalize a finite duration or ratio."""

    qualifier = "non-negative" if allow_zero else "positive"
    if type(value) not in (int, float):
        raise ValueError(f"{name} must be a finite {qualifier} number")
    try:
        normalized = float(value)
    except OverflowError as error:
        raise ValueError(
            f"{name} must be a finite {qualifier} number"
        ) from error
    if not math.isfinite(normalized) or (
        normalized < 0 if allow_zero else normalized <= 0
    ):
        raise ValueError(f"{name} must be a finite {qualifier} number")
    return normalized


def _optional_finite_number(
    value: object, name: str, *, allow_zero: bool
) -> float | None:
    """Validate an optional numeric trigger; ``None`` disables it."""

    if value is None:
        return None
    return _finite_number(value, name, allow_zero=allow_zero)


@dataclass(frozen=True, slots=True)
class DurableMode:
    """Reliability-first mode: fsync messages and ACK progress individually."""


@dataclass(frozen=True, slots=True)
class GroupMode:
    """Append immediately while batching data fsync and ACK checkpoints."""

    sync_messages: int | None = 20
    sync_interval: float | None = 0.25
    sync_bytes: int | None = 64 * 1024
    ack_checkpoint_messages: int | None = 50
    ack_checkpoint_interval: float | None = 1.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "sync_messages",
            _optional_positive_integer(self.sync_messages, "sync_messages"),
        )
        object.__setattr__(
            self,
            "sync_interval",
            _optional_finite_number(
                self.sync_interval, "sync_interval", allow_zero=False
            ),
        )
        object.__setattr__(
            self,
            "sync_bytes",
            _optional_positive_integer(self.sync_bytes, "sync_bytes"),
        )
        object.__setattr__(
            self,
            "ack_checkpoint_messages",
            _optional_positive_integer(
                self.ack_checkpoint_messages, "ack_checkpoint_messages"
            ),
        )
        object.__setattr__(
            self,
            "ack_checkpoint_interval",
            _optional_finite_number(
                self.ack_checkpoint_interval,
                "ack_checkpoint_interval",
                allow_zero=False,
            ),
        )
        if (
            self.sync_messages is None
            and self.sync_interval is None
            and self.sync_bytes is None
        ):
            raise ValueError(
                "at least one GroupMode data sync trigger must be enabled"
            )
        if (
            self.ack_checkpoint_messages is None
            and self.ack_checkpoint_interval is None
        ):
            raise ValueError(
                "at least one GroupMode ACK checkpoint trigger must be enabled"
            )


@dataclass(frozen=True, slots=True)
class FastMode:
    """RAM-first mode with bounded capacity and durable batch spill limits."""

    ram_max_messages: int = 10_000
    ram_max_bytes: int = 32 * 1024 * 1024
    high_watermark: float | None = 0.75
    max_ram_age: float | None = 5.0
    disconnect_grace: float | None = 3.0
    spill_batch_messages: int = 1_000
    spill_batch_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "ram_max_messages",
            _positive_integer(self.ram_max_messages, "ram_max_messages"),
        )
        object.__setattr__(
            self,
            "ram_max_bytes",
            _positive_integer(self.ram_max_bytes, "ram_max_bytes"),
        )

        high_watermark = _optional_finite_number(
            self.high_watermark, "high_watermark", allow_zero=False
        )
        if high_watermark is not None and high_watermark > 1:
            raise ValueError("high_watermark must be greater than 0 and at most 1")
        object.__setattr__(self, "high_watermark", high_watermark)

        object.__setattr__(
            self,
            "max_ram_age",
            _optional_finite_number(
                self.max_ram_age, "max_ram_age", allow_zero=False
            ),
        )
        object.__setattr__(
            self,
            "disconnect_grace",
            _optional_finite_number(
                self.disconnect_grace, "disconnect_grace", allow_zero=True
            ),
        )
        object.__setattr__(
            self,
            "spill_batch_messages",
            _positive_integer(
                self.spill_batch_messages, "spill_batch_messages"
            ),
        )
        object.__setattr__(
            self,
            "spill_batch_bytes",
            _positive_integer(self.spill_batch_bytes, "spill_batch_bytes"),
        )


DeliveryMode: TypeAlias = DurableMode | GroupMode | FastMode


def resolve_mode(mode: DeliveryMode | None) -> DeliveryMode:
    """Return one validated lifetime mode, defaulting to ``FastMode``."""

    if mode is None:
        return FastMode()
    if type(mode) not in (DurableMode, GroupMode, FastMode):
        raise TypeError(
            "mode must be a DurableMode, GroupMode, or FastMode instance"
        )
    return mode


class FastQueueFullError(RuntimeError):
    """Raised when FastMode cannot accept work within its hard RAM limits."""
