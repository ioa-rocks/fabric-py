# Security

The SDK and Fabric are pre-alpha with no supported stable release series.
Use matching current code. The SDK signs scoped calls; the daemon verifies
authority, expiry, revocation and replies. It relies on the local daemon to
perform those checks correctly.

Selecting `Fabric(key=...)` does not sandbox a process that can access operator
sockets or other credential files. Protect the key file and use deployment-level
isolation when a worker should be untrusted. Never post its seed or full chain
file in an issue.

For a suspected vulnerability use GitHub's **Report a vulnerability** if
available. Otherwise ask for a private reporting channel in an issue without
including exploit details or credentials. Include both SDK and daemon commits
and a synthetic reproduction through that private channel.
