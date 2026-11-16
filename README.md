# fabric-py

Dependency-free Python SDK for resources exposed by the local Fabric daemon.
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

Client-key authentication is designed but not implemented. Passing `key=`
fails loudly rather than pretending the current Unix-user boundary isolates
agents.

The fortune example installs as `fabric-fortune-worker` and `fabric-fortune`.

License: MPL-2.0.

