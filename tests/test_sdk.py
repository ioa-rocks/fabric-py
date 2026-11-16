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


class FakeDaemon:
    """The control socket as the SDK sees it, for request/reply."""

    def __init__(self, *, replies_after: int = 0, reply: dict | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.polls = 0
        self.replies_after = replies_after
        self.reply = reply or {"id": "e1:reply", "kind": "result", "source": "fortune@nas",
                               "content": "you will ship", "correlation_id": "e1",
                               "signer": "did:key:z6MkWorker", "signature": "c2ln"}

    def __call__(self, socket_path, method, path, body=None, timeout=None):
        self.calls.append((path, body))
        if path == "/v0/append":
            return {"id": "e1", "fresh": True}
        if path == "/v0/outcome":
            self.polls += 1
            return {"outcome": self.reply} if self.polls > self.replies_after else {}
        if path == "/v0/complete":
            return {"delivered": True}
        if path == "/v0/claim":
            return [{"event": {"id": "e1", "kind": "request", "source": "caller@desk"},
                     "consumer": body["consumer"], "lease_until": 1, "awaited": True},
                    {"event": {"id": "e2", "kind": "request", "source": "cron@desk"},
                     "consumer": body["consumer"], "lease_until": 1}]
        return {"ok": True}

    def paths(self) -> list[str]:
        return [path for path, _ in self.calls]


def test_fortune_caller_round_trip() -> None:
    from aethera_examples.fortune import call

    daemon = FakeDaemon()
    with patch("aethera.queue.json_request", side_effect=daemon):
        assert call(Fabric(queue_state="/q"), timeout=5) == "you will ship"
    # One request, nothing declared, the reply read and then released.
    assert daemon.paths() == ["/v0/append", "/v0/outcome", "/v0/release"]
    append = daemon.calls[0][1]
    assert append["name"] == "subject:queues/fortune" and append["reply"] is True
    assert "reply_target" not in append["event"]
    assert daemon.calls[1][1] == {"name": "subject:queues/fortune", "event_id": "e1", "timeout_s": 5}


def test_call_times_out_and_releases() -> None:
    daemon = FakeDaemon(replies_after=99)
    queue = Fabric(queue_state="/q").queue("fortune")
    with patch("aethera.queue.json_request", side_effect=daemon):
        with pytest.raises(TimeoutError):
            queue.call(Event(kind="request", source="test"), timeout=1)
    # Giving up frees the caller's place in the quota of requests in flight.
    assert daemon.paths()[-1] == "/v0/release"


def test_call_handle_survives_a_restart() -> None:
    daemon = FakeDaemon()
    queue = Fabric(queue_state="/q").queue("fortune")
    with patch("aethera.queue.json_request", side_effect=daemon):
        request_id = queue.request(Event(kind="request", source="test")).id
        # A new process knows only the queue and the id it stored.
        reply = queue.call_handle(request_id).result(timeout=0)
    assert reply is not None and reply.content == "you will ship"
    assert reply.signer == "did:key:z6MkWorker"
    # Reading is not consuming: nothing was released.
    assert "/v0/release" not in daemon.paths()


def test_long_wait_is_split_into_daemon_sized_parks() -> None:
    daemon = FakeDaemon(replies_after=2)
    queue = Fabric(queue_state="/q").queue("fortune")
    with patch("aethera.queue.json_request", side_effect=daemon):
        reply = queue.call_handle("e1").result(timeout=1500)
    assert reply is not None
    assert [body["timeout_s"] for _, body in daemon.calls] == [600, 600, 300]


def test_worker_completes_in_one_step() -> None:
    daemon = FakeDaemon()
    queue = Fabric(queue_state="/q").queue("fortune")
    with patch("aethera.queue.json_request", side_effect=daemon):
        awaited, plain = queue.claim(consumer="worker", limit=2)
        assert awaited.awaited and not plain.awaited
        assert queue.complete(awaited.event.id, Event(kind="result", source="fortune", content="ok"))
    path, body = daemon.calls[-1]
    assert path == "/v0/complete" and body["event_id"] == "e1"
    assert body["event"] == {"kind": "result", "source": "fortune", "content": "ok"}
    # No separate ack and no publish to a reply queue.
    assert daemon.paths() == ["/v0/claim", "/v0/complete"]


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


def test_queue_host_spread_is_sent_only_when_asked() -> None:
    queue = Fabric(queue_state="/q").queue("crawl")
    with patch("aethera.queue.json_request", return_value={}) as call:
        queue.host(delivery="shared")
        assert "spread" not in call.call_args.args[3]
        queue.host(delivery="shared", spread=True)
        assert call.call_args.args[3]["spread"] is True
        queue.host(delivery="shared", spread=False)
        assert call.call_args.args[3]["spread"] is False
