from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any
from uuid import uuid4

from .auth import Signer, append_value, field_digest
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
    signature: str = ""

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
            value = self.queue._post("outcome", {
                "event_id": self.id, "timeout_s": step,
            }, event_id=self.id, timeout=step + 10)
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
        self.queue._post("release", {"event_id": self.id}, event_id=self.id)


# The daemon parks one outcome request for at most ten minutes.
_MAX_PARK = 600


class Queue:
    def __init__(self, socket_path: Path, name: str, *, signer: Signer | None = None) -> None:
        self.socket_path = socket_path
        self.name = name if name.startswith("subject:") else f"subject:queues/{name}"
        self._signer = signer

    def _post(self, op: str, body: dict, *, event_id: str = "",
              consumer: str = "", value: str = "", timeout: float | None = None) -> Any:
        body = {"name": self.name, **body}
        if self._signer is not None:
            body["circle"] = self._signer.circle
            body["principal"] = self._signer.sign(op, self.name, event_id, consumer, value)
        if timeout is None:
            return json_request(self.socket_path, "POST", "/v0/" + op, body)
        return json_request(self.socket_path, "POST", "/v0/" + op, body, timeout=timeout)

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
        if self._signer is not None:
            raise FabricError("queue hosting with a scoped key is not supported")
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
        value = self._append(event, reply=False)
        return value["id"], value["fresh"]

    def _append(self, event: Event, *, reply: bool) -> dict:
        if self._signer is not None and not event.id:
            event = replace(event, id=uuid4().hex)
        body: dict[str, Any] = {"event": event.to_dict()}
        if reply:
            body["reply"] = True
        value = append_value(event, reply) if self._signer is not None else ""
        return self._post("append", body, event_id=event.id, value=value)

    def request(self, event: Event) -> Call:
        """Publish a request whose reply you will wait for.

        Nothing is declared: the host that takes the request keeps the
        reply with it, readable only by you. Unlike ``publish``, a request
        is never parked when no host is reachable; it fails at once.
        """
        value = self._append(event, reply=True)
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
        values = self._post("claim", {"consumer": consumer, "lease_s": lease, "n": limit},
                            consumer=consumer)
        return [Claim(Event.from_dict(v["event"]), v["consumer"], v["lease_until"],
                      bool(v.get("awaited"))) for v in values]

    def complete(self, request_id: str, reply: Event) -> bool:
        """Finish a request you claimed: record its reply and ack it, in one step.

        This is how a worker finishes any request. A credentialed client
        signs with its sub-key; otherwise the daemon signs with the device
        key. Returns False when nobody is waiting
        for the reply (none was wanted, or the caller gave up); the request
        is acknowledged either way. Raises FabricError if your lease has
        lapsed: another worker may hold the request by now.
        """
        digest = ""
        if self._signer is not None:
            reply = replace(reply, correlation_id=request_id, signer=self._signer.did,
                            signature=self._signer.reply_signature(self.name, request_id, reply))
            digest = field_digest("astral-complete-v1", reply.kind,
                                  reply.content, reply.payload_ref).hex()
        value = self._post("complete", {"event_id": request_id, "event": reply.to_dict()},
                           event_id=request_id, value=digest)
        return bool(value["delivered"])

    def ack(self, event_id: str, *, consumer: str) -> None:
        self._post("ack", {"consumer": consumer, "event_id": event_id},
                   event_id=event_id, consumer=consumer)

    def wait(self, *, timeout: int = 30) -> str:
        value = self._post("wait", {"timeout_s": timeout}, timeout=timeout + 5)
        return value["woke"]
