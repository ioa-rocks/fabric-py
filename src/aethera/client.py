from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from .bucket import Bucket
from .queue import Queue
from .transport import json_request


@dataclass(frozen=True, slots=True)
class Resource:
    name: str
    kind: str
    host: str = ""


class Fabric:
    """Entry point for resources available through the local Fabric daemon."""

    def __init__(self, *, key: str | None = None,
                 queue_state: str | os.PathLike[str] | None = None,
                 bucket_state: str | os.PathLike[str] | None = None) -> None:
        if key is not None:
            raise NotImplementedError("client-key authentication is designed but not implemented")
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        self.queue_socket = Path(queue_state or base / "astral") / "control.sock"
        self.bucket_socket = Path(bucket_state or base / "vault") / "control.sock"

    def queue(self, name: str) -> Queue:
        return Queue(self.queue_socket, name)

    def bucket(self, name: str) -> Bucket:
        return Bucket(self.bucket_socket, name)

    def resources(self, *, kind: str | None = None) -> list[Resource]:
        out: list[Resource] = []
        if kind in (None, "bucket"):
            for value in json_request(self.bucket_socket, "GET", "/v0/buckets"):
                out.append(Resource(value.get("name") or value["id"], "bucket"))
        if kind in (None, "queue"):
            status = json_request(self.queue_socket, "GET", "/v0/status")
            out.extend(Resource(v["name"], "queue", status.get("member", "")) for v in status.get("hosted", []))
            out.extend(Resource(name, "queue", host) for name, host in status.get("placed", {}).items())
        return out

