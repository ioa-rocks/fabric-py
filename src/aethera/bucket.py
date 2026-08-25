from __future__ import annotations

import json
from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote, urlencode

from .transport import json_request, request


class Bucket:
    def __init__(self, socket_path: Path, name: str) -> None:
        self.socket_path = socket_path
        self.name = name

    @property
    def _path(self) -> str:
        return "/v0/buckets/" + quote(self.name, safe="")

    def objects(self, *, tags: list[str] | None = None) -> list[dict]:
        query = "" if not tags else "?" + urlencode({"tags": ",".join(tags)})
        return json_request(self.socket_path, "GET", self._path + "/objects" + query)

    def put(self, name: str, data: bytes | BinaryIO, *, tags: list[str] | None = None) -> dict:
        content = data.read() if hasattr(data, "read") else data
        query = {"name": name}
        if tags:
            query["tags"] = ",".join(tags)
        _, raw = request(self.socket_path, "POST", self._path + "/objects?" + urlencode(query),
                         content, "application/octet-stream")
        return json.loads(raw)

    def get(self, ref: str) -> bytes:
        _, data = request(self.socket_path, "GET", self._path + "/content?" + urlencode({"ref": ref}))
        return data
