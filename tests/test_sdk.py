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


def test_default_sockets_follow_the_daemon_state_move(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    data, config = tmp_path / ".local" / "share", tmp_path / ".config"

    # A fresh machine: the data home, for both subsystems.
    fabric = Fabric()
    assert fabric.queue_socket == data / "astral" / "control.sock"
    assert fabric.bucket_socket == data / "vault" / "control.sock"

    # Not migrated yet: only the old directory exists.
    (config / "astral").mkdir(parents=True)
    assert Fabric().queue_socket == config / "astral" / "control.sock"
    assert Fabric().bucket_socket == data / "vault" / "control.sock"

    # Migrated: the new directory wins even if an old one is still around.
    (data / "astral").mkdir(parents=True)
    assert Fabric().queue_socket == data / "astral" / "control.sock"

    # XDG_DATA_HOME is honoured when absolute, ignored when relative.
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    assert Fabric().bucket_socket == tmp_path / "xdg" / "vault" / "control.sock"
    monkeypatch.setenv("XDG_DATA_HOME", "relative")
    assert Fabric().bucket_socket == data / "vault" / "control.sock"

    # An explicit state directory is never redirected.
    assert Fabric(queue_state="/q").queue_socket.as_posix() == "/q/control.sock"
