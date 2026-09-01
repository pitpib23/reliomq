"""Deprecated import path.

``reliomq.publisher`` was renamed to :mod:`reliomq.sender` in 0.3.0. This
module re-exports the same objects under their old names so
``from reliomq.publisher import ReliablePublisher`` keeps working; prefer
importing ``Sender`` from :mod:`reliomq.sender` (or from the top-level
``reliomq`` package) in new code.
"""

from __future__ import annotations

from .sender import DeliveryStatus, ReliablePublisher, Sender


__all__ = ["DeliveryStatus", "ReliablePublisher", "Sender"]
