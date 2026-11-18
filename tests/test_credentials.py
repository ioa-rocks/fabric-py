import base64
import builtins
import hashlib
import json
from unittest.mock import patch

import cbor2
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
import pytest

from aethera import Event, Fabric, FabricError
from aethera.auth import Signer, _did


@pytest.fixture
def key_file(tmp_path):
    # Synthetic loader fixture. Authority is exercised against real Go
    # daemons by run_daemon_tests.py, not by this unsigned leaf.
    seed = bytes(range(32))
    private = Ed25519PrivateKey.from_private_bytes(seed)
    did = _did(private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
    payload = {"typ": "assert", "aud": did, "sub": "did:key:test-circle", "v": 2}
    # Deliberately not canonical: the loader must hash the original bytes.
    payload_bytes = cbor2.dumps(payload)
    chain = b"\xa2" + cbor2.dumps("payload") + payload_bytes
    chain += cbor2.dumps("sig") + cbor2.dumps(bytes(64))
    path = tmp_path / "agent.key"
    path.write_text(json.dumps({"seed": base64.b64encode(seed).decode(),
                                "chain": base64.b64encode(chain).decode(),
                                "id": "editable label", "did": "editable label",
                                "circle": "editable label"}))
    return path, payload_bytes, private


def test_key_identity_comes_from_statement_and_exact_payload_bytes(key_file):
    path, payload, _ = key_file
    signer = Signer.load(path)
    envelope = signer.sign("claim", "subject:jobs", consumer="worker")
    assert base64.b64decode(envelope["auth"]) == hashlib.sha256(payload).digest()
    assert envelope["key"] == cbor2.loads(payload)["aud"]
    assert signer.circle == "did:key:test-circle"
    assert "seed" not in envelope and "private" not in repr(signer)


@pytest.mark.parametrize("change", [
    {"seed": "invalid base64"}, {"seed": base64.b64encode(b"short").decode()},
    {"seed": base64.b64encode(bytes(32)).decode()}, {"chain": "bad base64"},
    {"chain": base64.b64encode(b"\xa2").decode()}, {"seed": None}, {"chain": []},
])
def test_bad_key_files_fail_without_exposing_seed(key_file, change):
    path, _, _ = key_file
    original = json.loads(path.read_text())
    original.update(change)
    path.write_text(json.dumps(original))
    with pytest.raises(ValueError, match="invalid Astral key file") as raised:
        Fabric(key=path)
    assert str(original["seed"]) not in str(raised.value)


def test_missing_auth_dependencies_have_actionable_error(key_file):
    path, _, _ = key_file
    real_import = builtins.__import__

    def without_cbor(name, *args, **kwargs):
        if name == "cbor2":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=without_cbor):
        with pytest.raises(ImportError, match=r"aethera-sdk\[auth\]"):
            Fabric(key=path)
        # The base client still has no third-party dependency.
        assert Fabric().queue("jobs").name == "subject:queues/jobs"


@pytest.mark.parametrize("operation", ["host", "bucket", "resources"])
def test_unsupported_keyed_operations_never_use_operator_endpoints(key_file, operation):
    fabric = Fabric(key=key_file[0])
    with patch("aethera.queue.json_request") as queue_io, patch("aethera.client.json_request") as client_io:
        with pytest.raises(FabricError, match="scoped key"):
            if operation == "host":
                fabric.queue("jobs").host()
            elif operation == "bucket":
                fabric.bucket("private")
            else:
                fabric.resources()
        queue_io.assert_not_called()
        client_io.assert_not_called()


def test_signed_publish_binds_circle_and_generates_id_without_mutating_input(key_file):
    queue = Fabric(key=key_file[0]).queue("jobs")
    event = Event(kind="job", source="agent", content="hello")
    with patch("aethera.queue.json_request", return_value={"id": "e", "fresh": True}) as call:
        queue.publish(event)
    body = call.call_args.args[3]
    assert body["event"]["id"] and not event.id
    assert body["circle"] == "did:key:test-circle"
    assert body["principal"]["key"] == queue._signer.did
    assert "seed" not in json.dumps(body)


@pytest.mark.parametrize("field", ["kind", "payload_ref", "correlation_id", "reply_target"])
def test_ambiguous_signed_fields_are_refused_before_io(key_file, field):
    queue = Fabric(key=key_file[0]).queue("jobs")
    event = Event(kind="job", source="agent", **({field: "x\0y"} if field != "kind" else {}))
    if field == "kind":
        event.kind = "x\0y"
    with patch("aethera.queue.json_request") as call:
        with pytest.raises(ValueError, match="zero byte"):
            queue.publish(event)
        call.assert_not_called()


def test_each_outcome_poll_gets_fresh_signature(key_file):
    queue = Fabric(key=key_file[0]).queue("jobs")
    with patch("aethera.auth.time.time", side_effect=[1000, 1600]), patch(
        "aethera.queue.json_request", side_effect=[{}, {"outcome": {"kind": "result", "source": "worker"}}]
    ) as call:
        assert queue.call_handle("r1").result(timeout=601) is not None
    assert [c.args[3]["principal"]["at"] for c in call.call_args_list] == [1000, 1600]


def test_completion_signs_with_subkey_and_preserves_reply_input(key_file):
    queue = Fabric(key=key_file[0]).queue("jobs")
    reply = Event(kind="result", source="worker", content="hello\0世界")
    with patch("aethera.queue.json_request", return_value={"delivered": True}) as call:
        assert queue.complete("r1", reply)
    body = call.call_args.args[3]
    assert body["event"]["signer"] == queue._signer.did
    assert body["event"]["correlation_id"] == "r1"
    assert len(base64.b64decode(body["event"]["signature"])) == 64
    assert reply.signer == reply.signature == reply.correlation_id == ""
