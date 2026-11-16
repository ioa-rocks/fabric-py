from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any
from .transport import FabricError, json_request


@dataclass(slots=True)
class Event:
    kind: str
    source: str
    content: str = ""
    id: str = ""
    payload_ref: str = ""
    reply_target: str = ""
    correlation_id: str = ""
    at: int = 0
    signer: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v not in ("", 0)}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Event:
        known = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in known})


@dataclass(frozen=True, slots=True)
class Claim:
    """A leased event. ``awaited`` says a caller is waiting for a reply to it."""

    event: Event
    consumer: str
    lease_until: int
    awaited: bool = False


class Call:
    """A request whose reply is kept for you by the host that took it.

    The handle is just the queue and the request's id, so a caller that
    restarts can rebuild it with ``queue.call_handle(request_id)``.
    """

    def __init__(self, queue: Queue, request_id: str) -> None:
        self.queue = queue
        self.id = request_id

    def result(self, *, timeout: int = 30) -> Event | None:
        """Wait up to ``timeout`` seconds for the reply; None if there is none yet.

        The local daemon has checked the reply before returning it: it is
        signed for this request by ``reply.signer``, and that key was
        entitled to answer. Reading does not consume the reply; call
        ``release()`` once you have acted on it. Raises FabricError if the
        worker acknowledged the request without replying, or if the reply
        is no longer kept.
        """
        remaining = max(timeout, 0)
        while True:
            step = min(remaining, _MAX_PARK)
            value = json_request(self.queue.socket_path, "POST", "/v0/outcome", {
                "name": self.queue.name, "event_id": self.id, "timeout_s": step,
            }, timeout=step + 10)
            outcome = (value or {}).get("outcome")
            if outcome is not None:
                return Event.from_dict(outcome)
            remaining -= step
            if remaining <= 0:
                return None

    def release(self) -> None:
        """Tell the host you have the reply, or no longer want one.

        The host forgets the call and it stops counting against your quota
        of requests in flight. Releasing twice is harmless.
        """
        json_request(self.queue.socket_path, "POST", "/v0/release", {
            "name": self.queue.name, "event_id": self.id,
        })


# The daemon parks one outcome request for at most ten minutes.
_MAX_PARK = 600


class Queue:
    def __init__(self, socket_path: Path, name: str) -> None:
        self.socket_path = socket_path
        self.name = name if name.startswith("subject:") else f"subject:queues/{name}"

    def host(self, *, delivery: str = "exclusive", accepts: list[str] | None = None,
             idle_s: int = 0, max_events: int = 0, spread: bool | None = None) -> Queue:
        """Declare this queue on the local node.

        spread=True lets other nodes host the same shared queue too:
        publishers pick one host and fall over to the next, consumers
        claim from all. Call it on every node that should host the queue.
        It stays on across later host() calls that leave spread unset;
        spread=False turns it off on this node.

        idle_s self-destructs the queue after that much inactivity and
        max_events caps total publishes; zero leaves each unlimited. A
        publish to an expired queue raises with "expired": its owner is
        gone; do not retry.
        """
        body: dict[str, object] = {
            "name": self.name, "delivery": delivery, "accepts": accepts or [],
        }
        if idle_s:
            body["idle_s"] = idle_s
        if max_events:
            body["max_events"] = max_events
        if spread is not None:
            body["spread"] = spread
        json_request(self.socket_path, "POST", "/v0/subjects", body)
        return self

    def publish(self, event: Event) -> tuple[str, bool]:
        value = json_request(self.socket_path, "POST", "/v0/append", {
            "name": self.name, "event": event.to_dict(),
        })
        return value["id"], value["fresh"]

    def request(self, event: Event) -> Call:
        """Publish a request whose reply you will wait for.

        Nothing is declared: the host that takes the request keeps the
        reply with it, readable only by you. Unlike ``publish``, a request
        is never parked when no host is reachable; it fails at once.
        """
        value = json_request(self.socket_path, "POST", "/v0/append", {
            "name": self.name, "event": event.to_dict(), "reply": True,
        })
        return Call(self, value["id"])

    def call_handle(self, request_id: str) -> Call:
        """The handle of a request made earlier, by its id."""
        return Call(self, request_id)

    def call(self, event: Event, *, timeout: int = 30) -> Event:
        """Publish a request and return its reply: request, wait, release.

        Raises TimeoutError if no reply arrives in time. The request stays
        in the queue then, so a worker may still act on it, but nobody is
        waiting for its reply any more. Use ``request()`` yourself to keep
        waiting across restarts or to release only after a durable commit.
        """
        pending = self.request(event)
        try:
            reply = pending.result(timeout=timeout)
        except BaseException:
            try:
                pending.release()
            except FabricError:
                pass
            raise
        pending.release()
        if reply is None:
            raise TimeoutError(f"no reply to {pending.id} on {self.name} within {timeout}s")
        return reply

    def claim(self, *, consumer: str, lease: int = 30, limit: int = 1) -> list[Claim]:
        values = json_request(self.socket_path, "POST", "/v0/claim", {
            "name": self.name, "consumer": consumer, "lease_s": lease, "n": limit,
        })
        return [Claim(Event.from_dict(v["event"]), v["consumer"], v["lease_until"],
                      bool(v.get("awaited"))) for v in values]

    def complete(self, request_id: str, reply: Event) -> bool:
        """Finish a request you claimed: record its reply and ack it, in one step.

        This is how a worker finishes any request. The daemon signs the
        reply with this device's key. Returns False when nobody is waiting
        for the reply (none was wanted, or the caller gave up); the request
        is acknowledged either way. Raises FabricError if your lease has
        lapsed: another worker may hold the request by now.
        """
        value = json_request(self.socket_path, "POST", "/v0/complete", {
            "name": self.name, "event_id": request_id, "event": reply.to_dict(),
        })
        return bool(value["delivered"])

    def ack(self, event_id: str, *, consumer: str) -> None:
        json_request(self.socket_path, "POST", "/v0/ack", {
            "name": self.name, "consumer": consumer, "event_id": event_id,
        })

    def wait(self, *, timeout: int = 30) -> str:
        value = json_request(self.socket_path, "POST", "/v0/wait", {
            "name": self.name, "timeout_s": timeout,
        }, timeout=timeout + 5)
        return value["woke"]
