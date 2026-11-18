"""Signing for the key files issued by ``fabric astral key new``.

The daemon verifies authority, expiry and revocation. This module only reads
the leaf's identity and signs the same operation bytes as the Go client.
Optional dependencies are loaded only when a credential is selected.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
from pathlib import Path
import time
from typing import Any


def _base64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _did(public_key: bytes) -> str:
    # did:key's ed25519-pub multicodec, encoded as base58btc.
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(b"\xed\x01" + public_key, "big")
    encoded = ""
    while number:
        number, digit = divmod(number, 58)
        encoded = alphabet[digit] + encoded
    return "did:key:z" + encoded


def field_digest(tag: str, *fields: str) -> bytes:
    """astral.FieldDigest: UTF-8 byte lengths, field count, then fields."""
    digest = hashlib.sha256()

    def write(value: str) -> None:
        raw = value.encode("utf-8")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)

    write(tag)
    digest.update(len(fields).to_bytes(8, "big"))
    for value in fields:
        write(value)
    return digest.digest()


def _zero_free(**fields: str) -> None:
    for name, value in fields.items():
        if "\0" in value:
            raise ValueError(f"the {name} of a signed operation holds a zero byte")


def append_value(event: Any, reply: bool) -> str:
    _zero_free(kind=event.kind, payload_ref=event.payload_ref,
               correlation_id=event.correlation_id, reply_target=event.reply_target)
    wanted = "1" if reply else ""
    if event.reply_target:
        return "v2:" + field_digest("astral-append-v2", event.kind, event.content,
                                   event.payload_ref, event.correlation_id,
                                   wanted, event.reply_target).hex()
    return "\0".join((event.kind, event.content, event.payload_ref,
                      event.correlation_id, wanted))


def _leaf(chain: bytes, cbor2: Any) -> tuple[dict, bytes]:
    # Go's statement wire is a two-entry map. Read its values separately
    # to retain the exact payload bytes: re-encoding would change the ID
    # of a valid noncanonical payload or discard future fields.
    stream = io.BytesIO(chain)
    if stream.read(1) != b"\xa2":
        raise ValueError("invalid statement envelope")
    decoder = cbor2.CBORDecoder(stream)
    entries: dict[str, Any] = {}
    payload_bytes = b""
    for _ in range(2):
        key = decoder.decode()
        if key not in ("payload", "sig") or key in entries:
            raise ValueError("invalid statement envelope")
        start = stream.tell()
        entries[key] = decoder.decode()
        if key == "payload":
            payload_bytes = chain[start:stream.tell()]
    if stream.read(1) or not isinstance(entries.get("payload"), dict):
        raise ValueError("invalid statement payload")
    if not isinstance(entries.get("sig"), bytes) or len(entries["sig"]) != 64:
        raise ValueError("invalid statement signature")
    return entries["payload"], payload_bytes


class Signer:
    def __init__(self, private_key: Any, did: str, circle: str,
                 auth: bytes, chain: bytes) -> None:
        self._private_key = private_key
        self.did = did
        self.circle = circle
        self._auth = auth
        self._chain = chain

    @classmethod
    def load(cls, path: str | Path) -> Signer:
        try:
            import cbor2
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
        except ImportError:
            raise ImportError(
                "scoped credentials require the auth extra: "
                "python3 -m pip install 'aethera-sdk[auth]'"
            ) from None

        # Never include the key file's contents or decoder errors in an
        # exception: the seed is secret, even when the file is malformed.
        try:
            raw = Path(path).read_text(encoding="utf-8")
            key_file = json.loads(raw)
            seed = base64.b64decode(key_file["seed"], validate=True)
            chain = base64.b64decode(key_file["chain"], validate=True)
            if len(seed) != 32:
                raise ValueError("bad seed")
            payload, payload_bytes = _leaf(chain, cbor2)
            private_key = Ed25519PrivateKey.from_private_bytes(seed)
            did = _did(private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
            if payload.get("typ") != "assert" or payload.get("aud") != did:
                raise ValueError("seed does not match the statement recipient")
            circle = payload["sub"]
            if not isinstance(circle, str) or not circle:
                raise ValueError("missing circle")
            _zero_free(circle=circle)
        except (ValueError, TypeError, KeyError, UnicodeError, cbor2.CBORDecodeError):
            raise ValueError(f"invalid Astral key file: {path}") from None
        # Like the Go loader, derive these from the statement rather than
        # trusting the editable id/did/circle labels beside its chain.
        return cls(private_key, did, circle, hashlib.sha256(payload_bytes).digest(), chain)

    def sign(self, op: str, name: str, event_id: str = "",
             consumer: str = "", value: str = "") -> dict:
        _zero_free(op=op, circle=self.circle, name=name,
                   event_id=event_id, consumer=consumer)
        at = int(time.time())
        digest = hashlib.sha256()
        for field in ("astral-op-v1", op, self.circle, name, event_id,
                      consumer, value, str(at)):
            digest.update(field.encode("utf-8"))
            digest.update(b"\0")
        return {"auth": _base64(self._auth), "key": self.did,
                "chain": _base64(self._chain), "at": at,
                "sig": _base64(self._private_key.sign(digest.digest()))}

    def reply_signature(self, name: str, request_id: str, reply: Any) -> str:
        digest = field_digest("astral-reply-v2", self.circle, name, request_id,
                              reply.kind, reply.content, reply.payload_ref)
        return _base64(self._private_key.sign(digest))
