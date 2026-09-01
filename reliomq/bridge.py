"""Deprecated import path.

``reliomq.bridge`` was renamed to :mod:`reliomq.relay` in 0.3.0. This
module re-exports the same objects under their old names so
``from reliomq.bridge import ReliableMqttBridge`` keeps working; prefer
importing ``Relay`` from :mod:`reliomq.relay` (or from the top-level
``reliomq`` package) in new code.
"""

from __future__ import annotations

from .relay import Relay, ReliableMqttBridge


__all__ = ["Relay", "ReliableMqttBridge"]
