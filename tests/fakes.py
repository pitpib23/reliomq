from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class FakeMessage:
    topic: str
    payload: bytes


class FakePublishInfo:
    def __init__(
        self,
        *,
        rc: int = 0,
        published: bool = True,
        wait_hook: Callable[[float | None], None] | None = None,
        wait_error: Exception | None = None,
    ) -> None:
        self.rc = rc
        self.published = published
        self.wait_hook = wait_hook
        self.wait_error = wait_error
        self.wait_timeouts: list[float | None] = []

    def wait_for_publish(self, timeout: float | None = None) -> None:
        self.wait_timeouts.append(timeout)
        if self.wait_hook is not None:
            self.wait_hook(timeout)
        if self.wait_error is not None:
            raise self.wait_error

    def is_published(self) -> bool:
        return self.published


class FakeClient:
    """Small Paho-shaped client with explicitly driven callbacks."""

    def __init__(self) -> None:
        self.on_connect: Any = None
        self.on_connect_fail: Any = None
        self.on_disconnect: Any = None
        self.on_subscribe: Any = None
        self.on_message: Any = None

        self.connected = False
        self.connect_calls: list[tuple[str, int, int]] = []
        self.publish_calls: list[dict[str, Any]] = []
        self.subscribe_calls: list[tuple[str, int, int]] = []
        self.reconnect_delays: tuple[float, float] | None = None
        self.loop_started = False
        self.disconnect_calls = 0

        self.connect_result: Any = 0
        self.loop_start_result: Any = 0
        self.subscribe_result = 0
        self._next_mid = 1
        self.publish_results: deque[FakePublishInfo] = deque()
        self.publish_hook: Callable[[dict[str, Any]], None] | None = None

    def reconnect_delay_set(self, min_delay: float, max_delay: float) -> None:
        self.reconnect_delays = (min_delay, max_delay)

    def connect_async(self, host: str, port: int, keepalive: int) -> Any:
        self.connect_calls.append((host, port, keepalive))
        return self.connect_result

    def loop_start(self) -> Any:
        self.loop_started = True
        return self.loop_start_result

    def loop_stop(self) -> Any:
        self.loop_started = False
        return 0

    def disconnect(self) -> Any:
        self.disconnect_calls += 1
        self.connected = False
        return 0

    def is_connected(self) -> bool:
        return self.connected

    def subscribe(self, topic: str, qos: int = 0) -> tuple[int, int]:
        mid = self._next_mid
        self._next_mid += 1
        self.subscribe_calls.append((topic, qos, mid))
        return self.subscribe_result, mid

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
    ) -> FakePublishInfo:
        call = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain,
        }
        self.publish_calls.append(call)
        if self.publish_hook is not None:
            self.publish_hook(call)
        if self.publish_results:
            return self.publish_results.popleft()
        return FakePublishInfo()

    def emit_connect(self, reason_code: Any = 0) -> None:
        self.connected = reason_code == 0
        if self.on_connect is not None:
            self.on_connect(self, None, None, reason_code, None)

    def emit_latest_suback(self, reason_codes: Any = (1,)) -> int:
        if not self.subscribe_calls:
            raise AssertionError("no subscription request to acknowledge")
        mid = self.subscribe_calls[-1][2]
        if self.on_subscribe is None:
            raise AssertionError("client has no on_subscribe callback")
        self.on_subscribe(self, None, mid, reason_codes, None)
        return mid

    def emit_disconnect(self, reason_code: Any = 1) -> None:
        self.connected = False
        if self.on_disconnect is not None:
            self.on_disconnect(self, None, None, reason_code, None)

    def emit_message(self, topic: str, payload: bytes | str) -> None:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        if self.on_message is None:
            raise AssertionError("client has no on_message callback")
        self.on_message(self, None, FakeMessage(topic=topic, payload=payload))


def client_factory_for(client: FakeClient):
    def factory(**_kwargs: Any) -> FakeClient:
        return client

    return factory

