from __future__ import annotations

import http.client
import json
from pathlib import Path
import socket
from typing import Any


class FabricError(RuntimeError):
    """A local Fabric resource request failed."""


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: Path, timeout: float | None = None) -> None:
        super().__init__("fabric", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not None:
            self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.socket_path))


def request(socket_path: Path, method: str, path: str, body: bytes | None = None,
            content_type: str | None = None, timeout: float | None = None) -> tuple[http.client.HTTPResponse, bytes]:
    connection = UnixConnection(socket_path, timeout)
    headers = {"Content-Type": content_type} if content_type else {}
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read()
    except OSError as error:
        raise FabricError(f"cannot reach {socket_path}; is 'fabric up' running? {error}") from error
    finally:
        connection.close()
    if response.status != 200:
        raise FabricError(data.decode(errors="replace").strip() or f"HTTP {response.status}")
    return response, data


def json_request(socket_path: Path, method: str, path: str,
                 body: dict[str, Any] | None = None, timeout: float | None = None) -> Any:
    encoded = None if body is None else json.dumps(body).encode()
    _, data = request(socket_path, method, path, encoded,
                      "application/json" if encoded is not None else None, timeout)
    return json.loads(data) if data else None
