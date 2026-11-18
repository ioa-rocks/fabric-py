# Scoped-worker demo

From the SDK repository root, with the `auth` extra installed and a built
Fabric binary:

```sh
python3 examples/scoped_worker.py --fabric ../fabric/fabric
```

The demo starts two nodes on loopback with fresh temporary state. One hosts
`jobs`; the other runs a claim-only worker. It exercises the actual CLI
onboarding path, public SDK methods and signed daemon operations:

1. Create an owner circle and admit a second device without copying its root key.
2. Issue a publish-only caller key and a claim-only worker key.
3. Send a request, complete it on the other node, and collect its verified reply.
4. Refuse the worker's attempt to publish and to consume another queue.
5. Revoke the worker credential and refuse its next claim.

No root, TUN, FUSE, model API or external account is required. Both node
processes, temporary key files and state directories are cleaned up on exit.
Logs are printed on failure; the demo does not use default host state.

For two actual machines, follow
[Fabric's onboarding guide](https://github.com/ioa-rocks/fabric/blob/main/docs/getting-started.md).
The queue handlers can be ordinary Python functions, tools, or an agent runtime;
transport and credential handling stay the same.
