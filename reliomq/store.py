"""Deprecated import path.

``reliomq.store`` was renamed to :mod:`reliomq.outbox` in 0.3.0. This module
re-exports the same objects under their old names so
``from reliomq.store import DurableMessageStore, StoreError`` keeps working;
prefer importing ``Outbox``/``OutboxError`` from :mod:`reliomq.outbox` (or
from the top-level ``reliomq`` package) in new code.
"""

from __future__ import annotations

from .outbox import DurableMessageStore, Outbox, OutboxError, StoreError


__all__ = ["DurableMessageStore", "Outbox", "OutboxError", "StoreError"]
