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
`claim`, `ack`, and `wait`.

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

