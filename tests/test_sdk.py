from unittest.mock import patch

import pytest

from aethera import Event, Fabric


def test_resource_handles_are_lazy() -> None:
    fabric = Fabric(queue_state="/q", bucket_state="/b")
    assert fabric.queue("fortune").name == "subject:queues/fortune"
    assert fabric.bucket("photos").name == "photos"


def test_client_key_is_not_silent() -> None:
    with pytest.raises(NotImplementedError):
        Fabric(key="concierge")


def test_queue_publish() -> None:
    queue = Fabric(queue_state="/q").queue("fortune")
    with patch("aethera.queue.json_request", return_value={"id": "e1", "fresh": True}) as call:
        assert queue.publish(Event(id="e1", kind="request", source="test")) == ("e1", True)
    assert call.call_args.args[:3] == (queue.socket_path, "POST", "/v0/append")


def test_event_parser_ignores_future_envelope_fields() -> None:
    event = Event.from_dict({"kind": "result", "source": "worker", "content": "ok", "future": 42})
    assert event.content == "ok"


def test_fortune_caller_round_trip() -> None:
    from aethera_examples.fortune import call

    seen: dict[str, dict] = {}

    def daemon(socket_path, method, path, body=None, timeout=None):
        if path == "/v0/append":
            seen[body["name"]] = body
            return {"id": body["event"].get("id", "e1"), "fresh": True}
        if path == "/v0/claim":
            request = seen.get("subject:queues/fortune")
            if request is None:
                return []
            return [{"event": {"id": "r1", "kind": "result", "source": "worker",
                               "content": "you will ship",
                               "correlation_id": request["event"]["correlation_id"]},
                     "consumer": body["consumer"], "lease_until": 1}]
        return None

    with patch("aethera.queue.json_request", side_effect=daemon):
        assert call(Fabric(queue_state="/q"), timeout=5) == "you will ship"
    request = seen["subject:queues/fortune"]["event"]
    assert request["reply_target"].startswith("subject:queues/_reply/")
