# Contributing

This SDK follows the current Fabric daemon's local APIs. For a bug report,
include both repository commits, Python version, platform and a reproduction
using synthetic credentials and disposable state.

```sh
python3 -m pip install -e '.[auth]' pytest
python3 -m pytest -q
```

For credential or protocol changes, also check against the Go implementation:

```sh
python3 tests/run_daemon_tests.py ../fabric
```

This requires Go, a matching Fabric source checkout, and permission to open
loopback sockets. It uses a Go test overlay and leaves that checkout unchanged.
The standalone `examples/scoped_worker.py` additionally exercises the CLI setup.

Keep the operator client dependency-free, load signing dependencies only when
a credential is selected, and refuse unsupported credentialed operations instead
of acting as the operator. The daemon owns authority verification; avoid adding
a parallel grant-policy implementation in Python.

Use stable IDs for recovery examples. Do not put real keys, customer material,
machine state or infrastructure addresses in tests or issues. Security reports
follow [SECURITY.md](SECURITY.md). Contributions use [MPL-2.0](LICENSE).
