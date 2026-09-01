from __future__ import annotations

import unittest
from dataclasses import dataclass

from fakes import FakeClient, FakePublishInfo

from reliomq.mqtt import (
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)


@dataclass
class _ReasonCodeLike:
    """Mimics paho.mqtt.reasoncodes.ReasonCode's is_failure attribute."""

    is_failure: bool


class ReasonCodeIsSuccessTests(unittest.TestCase):
    def test_plain_integer_zero_is_success(self) -> None:
        self.assertTrue(reason_code_is_success(0))

    def test_plain_nonzero_integer_is_failure(self) -> None:
        self.assertFalse(reason_code_is_success(7))

    def test_reason_code_object_defers_to_is_failure_attribute(self) -> None:
        self.assertTrue(reason_code_is_success(_ReasonCodeLike(is_failure=False)))
        self.assertFalse(reason_code_is_success(_ReasonCodeLike(is_failure=True)))

    def test_unrecognized_value_is_treated_as_failure(self) -> None:
        self.assertFalse(reason_code_is_success(object()))


class SubackIsSuccessTests(unittest.TestCase):
    def test_empty_or_none_is_failure(self) -> None:
        self.assertFalse(suback_is_success(None))
        self.assertFalse(suback_is_success(()))

    def test_single_granted_qos_is_success(self) -> None:
        for granted in (0, 1, 2):
            with self.subTest(granted=granted):
                self.assertTrue(suback_is_success(granted))

    def test_single_failure_code_is_failure(self) -> None:
        self.assertFalse(suback_is_success(0x80))

    def test_all_granted_in_a_sequence_is_success(self) -> None:
        self.assertTrue(suback_is_success((1, 2, 0)))

    def test_any_failure_in_a_sequence_is_failure(self) -> None:
        self.assertFalse(suback_is_success((1, 0x80, 0)))

    def test_reason_code_objects_defer_to_is_failure_attribute(self) -> None:
        self.assertTrue(
            suback_is_success((_ReasonCodeLike(is_failure=False),))
        )
        self.assertFalse(
            suback_is_success(
                (_ReasonCodeLike(is_failure=False), _ReasonCodeLike(is_failure=True))
            )
        )


class ConfirmedPublishTests(unittest.TestCase):
    def test_success_waits_for_publish_and_checks_is_published(self) -> None:
        client = FakeClient()
        client.publish_results.append(FakePublishInfo(rc=0, published=True))

        self.assertTrue(
            confirmed_publish(client, "topic", b"payload", qos=1, timeout=1.0)
        )
        self.assertEqual(client.publish_calls[0]["qos"], 1)

    def test_nonzero_return_code_is_failure_without_waiting(self) -> None:
        client = FakeClient()
        info = FakePublishInfo(rc=4, published=False)
        client.publish_results.append(info)

        self.assertFalse(confirmed_publish(client, "topic", b"x", timeout=1.0))
        self.assertEqual(info.wait_timeouts, [])

    def test_confirmed_but_not_published_is_failure(self) -> None:
        client = FakeClient()
        client.publish_results.append(FakePublishInfo(rc=0, published=False))

        self.assertFalse(confirmed_publish(client, "topic", b"x", timeout=1.0))

    def test_wait_for_publish_timeout_is_treated_as_failure_not_raised(self) -> None:
        client = FakeClient()
        client.publish_results.append(
            FakePublishInfo(wait_error=TimeoutError("no puback"))
        )

        self.assertFalse(confirmed_publish(client, "topic", b"x", timeout=1.0))

    def test_unexpected_exception_from_publish_is_treated_as_failure_not_raised(
        self,
    ) -> None:
        class ExplodingClient(FakeClient):
            def publish(self, *_args, **_kwargs):
                raise RuntimeError("socket gone")

        self.assertFalse(
            confirmed_publish(ExplodingClient(), "topic", b"x", timeout=1.0)
        )


class CreateClientTests(unittest.TestCase):
    def test_default_factory_produces_a_real_paho_client_with_client_id(self) -> None:
        client = create_client(None, client_id="test-client")
        try:
            self.assertEqual(client._client_id.decode(), "test-client")
        finally:
            client.loop_stop()

    def test_injected_factory_receives_keyword_arguments(self) -> None:
        captured = {}

        def factory(*, client_id, userdata=None):
            captured["client_id"] = client_id
            captured["userdata"] = userdata
            return FakeClient()

        create_client(factory, client_id="abc", userdata={"role": "test"})

        self.assertEqual(captured, {"client_id": "abc", "userdata": {"role": "test"}})


if __name__ == "__main__":
    unittest.main()
