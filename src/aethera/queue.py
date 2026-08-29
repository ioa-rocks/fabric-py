from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any
from .transport import json_request


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

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v not in ("", 0)}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Event:
        known = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in known})


@dataclass(frozen=True, slots=True)
class Claim:
    event: Event
    consumer: str
    lease_until: int


class Queue:
    def __init__(self, socket_path: Path, name: str) -> None:
        self.socket_path = socket_path
        self.name = name if name.startswith("subject:") else f"subject:queues/{name}"

    def host(self, *, delivery: str = "exclusive", accepts: list[str] | None = None,
             idle_s: int = 0, max_events: int = 0) -> Queue:
        """Declare this queue on the local node.

        idle_s self-destructs the queue after that much inactivity and
        max_events caps total publishes; zero leaves each unlimited, except
        temporary ``_reply/`` queues, which get tight server-side defaults
        either way. A publish to an expired reply queue raises with
        "expired" — the caller is gone; do not retry.
        """
        body: dict[str, object] = {
            "name": self.name, "delivery": delivery, "accepts": accepts or [],
        }
        if idle_s:
            body["idle_s"] = idle_s
        if max_events:
            body["max_events"] = max_events
        json_request(self.socket_path, "POST", "/v0/subjects", body)
        return self

    def publish(self, event: Event) -> tuple[str, bool]:
        value = json_request(self.socket_path, "POST", "/v0/append", {
            "name": self.name, "event": event.to_dict(),
        })
        return value["id"], value["fresh"]

    def claim(self, *, consumer: str, lease: int = 30, limit: int = 1) -> list[Claim]:
        values = json_request(self.socket_path, "POST", "/v0/claim", {
            "name": self.name, "consumer": consumer, "lease_s": lease, "n": limit,
        })
        return [Claim(Event.from_dict(v["event"]), v["consumer"], v["lease_until"]) for v in values]

    def ack(self, event_id: str, *, consumer: str) -> None:
        json_request(self.socket_path, "POST", "/v0/ack", {
            "name": self.name, "consumer": consumer, "event_id": event_id,
        })

    def wait(self, *, timeout: int = 30) -> str:
        value = json_request(self.socket_path, "POST", "/v0/wait", {
            "name": self.name, "timeout_s": timeout,
        }, timeout=timeout + 5)
        return value["woke"]
