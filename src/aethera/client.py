from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from .auth import Signer
from .bucket import Bucket
from .queue import Queue
from .transport import FabricError, json_request


@dataclass(frozen=True, slots=True)
class Resource:
    name: str
    kind: str
    host: str = ""


def _xdg_home(variable: str, *fallback: str) -> Path:
    value = os.environ.get(variable, "")
    return Path(value) if os.path.isabs(value) else Path.home().joinpath(*fallback)


def default_state(name: str) -> Path:
    """Fabric's default state directory for a subsystem ("astral", "vault").

    State lives under the data home. Before the daemon has moved an older
    install, it is still under the config home, so use that while the new
    directory does not exist. Mirrors Fabric's own client discovery.
    """
    current = _xdg_home("XDG_DATA_HOME", ".local", "share") / name
    if not current.exists():
        legacy = _xdg_home("XDG_CONFIG_HOME", ".config") / name
        if legacy.is_dir():
            return legacy
    return current


class Fabric:
    """Entry point for resources available through the local Fabric daemon."""

    def __init__(self, *, key: str | os.PathLike[str] | None = None,
                 queue_state: str | os.PathLike[str] | None = None,
                 bucket_state: str | os.PathLike[str] | None = None) -> None:
        self._signer = Signer.load(Path(key)) if key is not None else None
        self.queue_socket = Path(queue_state or default_state("astral")) / "control.sock"
        self.bucket_socket = Path(bucket_state or default_state("vault")) / "control.sock"

    def queue(self, name: str) -> Queue:
        return Queue(self.queue_socket, name, signer=self._signer)

    def bucket(self, name: str) -> Bucket:
        if self._signer is not None:
            raise FabricError("Vault access with a scoped key is not supported")
        return Bucket(self.bucket_socket, name)

    def resources(self, *, kind: str | None = None) -> list[Resource]:
        if self._signer is not None:
            raise FabricError("resource listing with a scoped key is not supported")
        out: list[Resource] = []
        if kind in (None, "bucket"):
            for value in json_request(self.bucket_socket, "GET", "/v0/buckets"):
                out.append(Resource(value.get("name") or value["id"], "bucket"))
        if kind in (None, "queue"):
            status = json_request(self.queue_socket, "GET", "/v0/status")
            out.extend(Resource(v["name"], "queue", status.get("member", "")) for v in status.get("hosted", []))
            out.extend(Resource(name, "queue", host) for name, host in status.get("placed", {}).items())
        return out

