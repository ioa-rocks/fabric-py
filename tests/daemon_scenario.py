"""Python half of the two-node Go fixture; no live host state is used."""

import copy
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

from aethera import Event, Fabric, FabricError
from aethera.transport import json_request


fixture = json.loads(Path(sys.argv[1]).read_text())
clients = {label: Fabric(key=path, queue_state=fixture["state"])
           for label, path in fixture["keys"].items()}


def denied(action):
    try:
        action()
    except FabricError as error:
        assert "denied" in str(error) or "belongs to" in str(error), str(error)
    else:
        raise AssertionError("operation with insufficient authority succeeded")


if sys.argv[2] == "revoked":
    for name in ("local", "remote"):
        denied(lambda: clients["worker"].queue(name).claim(consumer="worker"))
        denied(lambda: clients["expired"].queue(name).publish(Event(kind="job", source="agent")))
    print("revoked and expired keys refused on local and remote hosts")
    raise SystemExit(0)


for name in ("local", "remote"):
    publisher = clients["publisher"].queue(name)
    worker = clients["worker"].queue(name)
    other = clients["other"].queue(name)
    key_label = clients["publisher"]._signer.did[-8:]

    # Every public queue operation, including a complete signed call.
    event = Event(id="plain-" + name, kind="job", source="agent", content="hello\0世界")
    assert publisher.publish(event) == (event.id, True)
    assert publisher.publish(event) == (event.id, False)
    assert worker.wait(timeout=0) == "mail"
    claim, = worker.claim(consumer="worker")
    assert key_label in claim.event.source, claim.event.source
    assert claim.event.content == event.content
    worker.ack(claim.event.id, consumer="worker")
    assert not worker.claim(consumer="worker")

    pending = publisher.request(Event(kind="job", source="caller", content="request"))
    claim, = worker.claim(consumer="worker")
    assert claim.awaited and claim.event.id == pending.id
    assert worker.complete(claim.event.id, Event(kind="result", source="worker", content="done\0世界"))
    result = pending.result(timeout=0)
    assert result.content == "done\0世界"
    assert result.signer == clients["worker"]._signer.did
    assert result.signature
    # Ownership survives a fresh SDK instance, and is checked for reads
    # and release even when another key has the same publish permission.
    denied(lambda: other.call_handle(pending.id).result(timeout=0))
    denied(lambda: other.call_handle(pending.id).release())
    restored = Fabric(key=fixture["keys"]["publisher"], queue_state=fixture["state"])
    assert restored.queue(name).call_handle(pending.id).result(timeout=0).content == result.content
    restored.queue(name).call_handle(pending.id).release()

    # Scope and verbs are checked at both paths, not by Python policy.
    denied(lambda: worker.publish(Event(kind="job", source="worker")))
    denied(lambda: publisher.claim(consumer="sneak"))
    denied(lambda: publisher.wait(timeout=0))
    denied(lambda: clients["publisher"].queue("other").publish(Event(kind="job", source="agent")))

    # What an intermediary could change after the Python signature.
    def tamper(body_change):
        def altered(socket, method, path, body=None, timeout=None):
            body = copy.deepcopy(body)
            body_change(body)
            return json_request(socket, method, path, body, timeout)
        return altered

    def attempt(change, event):
        with patch("aethera.queue.json_request", side_effect=tamper(change)):
            denied(lambda: publisher.publish(event))

    def replace_signature(body):
        body["principal"]["sig"] = "AA=="

    attempt(replace_signature, Event(kind="job", source="agent"))
    attempt(lambda body: body["principal"].update(at=int(time.time()) - 3600),
            Event(kind="job", source="agent"))
    attempt(lambda body: body["event"].update(content="rewritten"),
            Event(kind="job", source="agent", content="original"))
    attempt(lambda body: body["event"].update(reply_target="subject:queues/stolen"),
            Event(kind="job", source="agent", reply_target="subject:queues/results"))
    # Exercise the length-prefixed publish form with the signed target.
    sent, fresh = publisher.publish(Event(kind="job", source="agent", content="reply here",
                                         reply_target="subject:queues/results"))
    claim, = worker.claim(consumer="worker")
    assert claim.event.id == sent and claim.event.reply_target == "subject:queues/results"
    assert worker.complete(sent, Event(kind="result", source="worker")) is False

print("all SDK operations, reply ownership, source stamping, tampering and scope checks passed on two nodes")
