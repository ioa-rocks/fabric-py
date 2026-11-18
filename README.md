# fabric-py

Python SDK for resources exposed by the local Fabric daemon. Operator clients
are dependency-free; scoped credentials use the optional `auth` extra.
The distribution is `aethera-sdk` and the import is `aethera` to avoid
colliding with Python's established `fabric` deployment package.

```python
from aethera import Event, Fabric

fabric = Fabric()

photos = fabric.bucket("photos")
photos.put("hello.txt", b"hello")
print(photos.get("hello.txt"))

fortune = fabric.queue("fortune")
fortune.publish(Event(kind="request", source="example"))
```

Resource handles are lazy. The SDK talks only to local Unix sockets; Fabric
resolves placement and carries operations across the mesh. `Bucket` currently
supports `objects`, `put`, and `get`; `Queue` supports `host`, `publish`,
`claim`, `ack`, `wait`, and request/reply through `request`, `call` and
`complete`.

## Request and reply

The reply to a request is kept with the request, by the node that hosts the
queue. Nothing is declared for it.

```python
# caller
reply = fabric.queue("fortune").call(Event(kind="request", source="me"), timeout=30)
print(reply.content, "answered by", reply.signer)

# worker
queue = fabric.queue("fortune").host(delivery="shared")
for claim in queue.claim(consumer="worker"):
    queue.complete(claim.event.id, Event(kind="result", source="fortune", content="..."))
```

`complete` records the reply and acknowledges the request in one step, and
is how a worker finishes any request: it returns `False` when nobody is
waiting for the reply. The daemon signs the reply with the device key. The
caller's daemon checks that signature, and that the signer was entitled to
answer, before `call` returns; the SDK itself does no cryptography.

`call` is request, wait, release. To keep waiting across a restart, or to
release only after your own commit, use the parts: `queue.request(event)`
returns a handle whose `id` you can store; `queue.call_handle(id).result()`
reads the reply as often as needed; `.release()` tells the host you are
done. A caller may have 100 requests in flight per host; releasing frees a
place at once. `call` raises `TimeoutError` when no reply arrives, and
`FabricError` when the worker acknowledged the request without replying.

For a long-lived conversation, host an ordinary queue for the answers and
name it in the request's `reply_target`. Temporary `_reply/...` queues, the
earlier pattern, are refused by the daemon.

`queue.host(delivery="shared", spread=True)` on two or more nodes makes
them all hosts of that queue, so it keeps working when one is down.
Publishers and workers need no change: the local daemon picks a host,
falls over to the next, and claims from all of them.

By default the sockets are `~/.local/share/astral/control.sock` and
`~/.local/share/vault/control.sock` (`$XDG_DATA_HOME` when set). An install
the daemon has not moved yet is found under `~/.config`. The path is chosen
when `Fabric()` is created, so restart long-running workers after the
daemon moves its state. `queue_state=` and `bucket_state=` override it.

## Scoped agent credentials

Install signing support with the interpreter that runs your worker:

```bash
python3 -m pip install -e '.[auth]'
```

This adds `cryptography` (Ed25519) and `cbor2` (reading the issued statement).
Ordinary clients do not import or require either package.

Mint a credential through the Fabric CLI, using the complete queue name:

```bash
fabric astral key new -res subject:queues/jobs -verbs claim -out worker.key
```

The operator declares and places the queue separately. The worker selects the
key file explicitly; its circle comes from the signed statement:

```python
worker = Fabric(key="worker.key")
jobs = worker.queue("jobs")
for claim in jobs.claim(consumer="worker"):
    jobs.complete(claim.event.id, Event(kind="result", source="worker", content="done"))
```

`key=` is a path (including a `Path` object), not a self-asserted agent name.
The SDK signs `publish`, `request`, `claim`, `ack`, `wait`, `complete`, and the
call handle's `result` and `release`. A claim-only key can complete requests
whose live leases it holds. For `ack`, pass the original consumer name you
used in `claim`, rather than the server's stamped `claim.consumer`.

Replies are signed by the worker's sub-key. The caller's daemon checks the
signature and current authority before returning `Event.signer` and
`Event.signature` (base64). Ordinary published events have a daemon-stamped
`source`, not a verified `signer` field. A source label by itself is not an
identity credential.

Use an explicit `Event.id` when retrying a send from a durable outbox. If it
is empty the SDK generates an ID for that invocation, without changing the
input event. Save request IDs and use `call_handle(id).result()` followed by
`release()` after your own durable commit to recover across restarts.
Signed sends are refused when no host can be reached; the daemon never
retries them under its broader device authority.

A credentialed client refuses queue hosting, resource listing and Vault
access: those endpoints have no scoped-key binding here. It never silently
falls back to the operator. A separate `Fabric()` client deliberately acts
as the local operator.

The daemon enforces scope, expiry and revocation; the SDK does not implement
a second grant verifier. Socket access remains one Unix-user trust domain.
The deployment must isolate a worker from operator sockets and other keys
when it needs an adversarial boundary.

Use the November 16 request/reply build or newer on both ends for scoped
calls and signed `reply_target`. This is the local-daemon client; directly
dialing a grantee key's `gates` without a daemon is not implemented.

## Validation

```bash
python3 -m pytest -q
python3 tests/run_daemon_tests.py /path/to/fabric
```

The second command overlays one test into the Fabric source checkout and
runs two disposable nodes over loopback. It verifies the Python signatures
against the Go daemon, both local and remote authorization, reply ownership,
tampering, expiry and revocation. It needs Go, the Fabric source checkout
and the `auth` extra; it leaves the source and live fleet unchanged.

The fortune example installs as `fabric-fortune-worker` and `fabric-fortune`.

License: MPL-2.0.

