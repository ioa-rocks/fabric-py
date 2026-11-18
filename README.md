# Fabric Python SDK

**Use Fabric's durable queues and object storage from Python.** The SDK talks
to a local [Fabric node](https://github.com/ioa-rocks/fabric); the node handles
peer connectivity, resource placement, circle authority and remote delivery.

The distribution is `aethera-sdk` and the import is `aethera` (IOA was formerly
called Aethera). This avoids a collision with Python's unrelated `fabric` package.

**Pre-alpha.** The API and daemon protocol can change together. Install from
source and use matching current checkouts of this SDK and Fabric. There is no
stable PyPI release yet. Python 3.11 or newer is required.

## Install

```sh
git clone https://github.com/ioa-rocks/fabric-py.git
cd fabric-py
python3 -m pip install -e '.[auth]'
```

The `auth` extra supplies `cryptography` for Ed25519 signatures and `cbor2` for
reading issued credentials. For local operator use without scoped credentials,
`python3 -m pip install -e .` installs the dependency-free resource client.

Build the node from [Fabric's repository](https://github.com/ioa-rocks/fabric).
Then try the [two-node scoped-worker demo](examples/README.md):

```sh
python3 examples/scoped_worker.py --fabric ../fabric/fabric
```

It creates disposable identities and state, starts two loopback nodes, delivers
a job, verifies a signed reply, and demonstrates denied and revoked access.
It needs no root privileges, model API, external account, or live node setup.

## Request work

With a connected Fabric node and a declared `jobs` queue:

```python
from aethera import Event, Fabric

reply = Fabric().queue("jobs").call(
    Event(kind="request", source="caller", content="hello"), timeout=30
)
print(reply.content, "answered by", reply.signer)
```

The operator declares the queue on one host:

```sh
fabric astral subject -delivery shared subject:queues/jobs
```

`queue("jobs")` maps to `subject:queues/jobs`. Fully qualified `subject:...`
names are accepted unchanged. Handles are lazy; constructing one does not
contact the daemon.

## Scope a worker's access

On the worker's node, issue a credential under its device admission:

```sh
fabric astral key new -res subject:queues/jobs -verbs claim -out worker.key
```

The worker selects that file:

```python
jobs = Fabric(key="worker.key").queue("jobs")
for claim in jobs.claim(consumer="worker"):
    jobs.complete(claim.event.id, Event(kind="result", source="worker", content="done"))
```

`key=` accepts a string path or `Path`, not a self-asserted agent name. Its
circle and principal come from the issued statement. The SDK signs exposed
queue operations; the daemon verifies the chain, scope, expiry and revocation.
The circle's owner key is not needed by the worker.

| Operation | Behavior |
| --- | --- |
| `publish(event)` | Send an event; returns `(event_id, fresh)`. |
| `request(event)` | Start an awaited request; returns a durable call handle. |
| `call(event, timeout=...)` | Request, wait, and release the reply. |
| `claim(consumer=..., lease=..., limit=...)` | Lease pending events. |
| `complete(request_id, reply)` | Sign the reply and acknowledge its leased request in one host write. |
| `ack(event_id, consumer=...)` | Acknowledge using the original consumer name passed to `claim`. |
| `wait(timeout=...)` | Wait for pending mail or timeout. |

A claim-only key can finish requests whose live leases it holds. It cannot
publish new jobs or claim other resources. The SDK refuses queue hosting,
resource listing and Vault access when a key is selected, because those SDK
endpoints have no scoped-key binding. A missing or malformed credential never
falls back to operator authority.

Replies retain `Event.signer` and `Event.signature` (base64), checked by the
caller's daemon before returning them. Ordinary published events have a
daemon-stamped `source`; a source label by itself is not an identity credential.

## Recover across a restart

```python
jobs = Fabric().queue("jobs")
pending = jobs.request(Event(id="stable-job-id", kind="request", source="caller"))
# Commit pending.id with your application's state before relying on it.

# A later process, under the same principal:
pending = jobs.call_handle("stable-job-id")
reply = pending.result(timeout=30)
if reply is not None:
    # Commit the reply to application state first.
    pending.release()
```

Reading a result does not consume it. Release after incorporating it. The
convenience `call()` releases on success, timeout or interruption; it is not
the durable-checkpoint interface. Releasing or timing out does not cancel the
worker's work.

Delivery is **at least once**. Use stable `Event.id` values for retries and
idempotent external effects. With a scoped credential and an empty event ID,
the SDK generates one per invocation without changing the input event. Signed
sends to unreachable hosts are refused, so an application outbox must retry
them under the same identity. A long-lived conversation uses ordinary queues
for progress and later results, optionally named by `reply_target`.

## Storage and placement

An operator client can use existing Vault buckets:

```python
photos = Fabric().bucket("photos")
photos.put("hello.txt", b"hello")
print(photos.get("hello.txt"))
```

The bucket must exist and have a writable placement for edits. The current
bucket wrapper exposes `objects`, `put` and `get`; it is not the full Vault CLI.
See [Vault's guide](https://github.com/ioa-rocks/fabric/blob/main/vault/README.md).

An operator can host a shared queue on multiple nodes with
`queue.host(delivery="shared", spread=True)`. These are independent host
journals, not replicated queue storage. Events on an unavailable host wait
there; loss of its disk loses them.

## State and boundaries

Default sockets are `~/.local/share/astral/control.sock` and
`~/.local/share/vault/control.sock`, honoring an absolute `$XDG_DATA_HOME`.
The legacy `~/.config` directories are used until the daemon migrates them.
`queue_state=` and `bucket_state=` override the subsystem directories.

The SDK is a local Unix-socket client, not a daemon-less peer. Keep explicit
state paths short: the daemon's hashed fallback for very long socket paths is
not implemented in Python yet. A scoped credential does not isolate a process
that can also access operator sockets or other keys. The deployment supplies
that process boundary. See [SECURITY.md](SECURITY.md).

The included `fabric-fortune` and `fabric-fortune-worker` commands provide an
older operator-level example; the worker additionally needs the `fortune`
executable. The scoped-worker demo needs only this SDK and the Fabric binary.

## Development

```sh
python3 -m pip install -e '.[auth]' pytest
python3 -m pytest -q
python3 tests/run_daemon_tests.py ../fabric
```

The last command needs the sibling Fabric source checkout and Go. It overlays
a test into that checkout, starts two disposable nodes, and verifies signatures,
permissions, reply ownership, tampering, expiry and revocation against the Go
implementation. It leaves the source and live fleet unchanged.

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MPL-2.0](LICENSE).
