"""Thread-safe correlation for one in-flight application acknowledgement."""

from __future__ import annotations

import threading


class AckTracker:
    """Track the acknowledgement expected by a single delivery worker.

    ``begin`` must run before the corresponding MQTT publish so an immediate
    acknowledgement cannot be missed.  Only one expectation may be active at
    a time.  Network callbacks call :meth:`match`, while the delivery worker
    calls :meth:`wait`; the internal lock is never held during that wait.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._expected_event_id: str | None = None
        self._matched = False
        self._interrupted = False

    def begin(self, event_id: str) -> None:
        """Begin waiting for ``event_id``.

        Raises:
            ValueError: If ``event_id`` is empty or is not a string.
            RuntimeError: If another expectation is already active.
        """

        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event_id must be a non-empty string")

        with self._lock:
            if self._expected_event_id is not None:
                raise RuntimeError("an ACK expectation is already active")
            self._event.clear()
            self._expected_event_id = event_id
            self._matched = False
            self._interrupted = False

    def match(self, event_id: str) -> bool:
        """Record a matching ACK and wake the waiter.

        Wrong, stale, and late event IDs return ``False`` without changing the
        active expectation.
        """

        with self._lock:
            matched = (
                self._expected_event_id is not None
                and not self._interrupted
                and event_id == self._expected_event_id
            )
            if matched:
                self._matched = True
                self._event.set()
            return matched

    def wait(self, timeout: float | None) -> bool:
        """Wait without holding the tracker lock and report a matched ACK."""

        signalled = self._event.wait(timeout=timeout)
        with self._lock:
            # Close the expectation at the timeout boundary.  A callback that
            # acquires the lock afterward is late and must not report a match.
            if not signalled and not self._matched:
                self._interrupted = True
            return self._matched and not self._interrupted

    def interrupt(self) -> None:
        """Wake the current waiter without treating the wakeup as an ACK."""

        with self._lock:
            self._interrupted = True
            self._event.set()

    def end(self) -> None:
        """Clear the active expectation so subsequent ACKs are considered late."""

        with self._lock:
            self._expected_event_id = None
            self._matched = False
            self._interrupted = False
            self._event.clear()

    @property
    def expected_event_id(self) -> str | None:
        """Return the current expected ID, primarily for diagnostics."""

        with self._lock:
            return self._expected_event_id
