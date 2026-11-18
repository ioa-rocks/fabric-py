"""Two disposable Fabric nodes exchanging work through scoped credentials."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

from aethera import Event, Fabric, FabricError


def eventually(action, description, *, timeout=30):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = action()
            if value:
                return value
        except (FabricError, FileNotFoundError) as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {description}: {last_error}")


def expect_denied(action):
    try:
        action()
    except FabricError as error:
        if "denied" in str(error):
            return True
        raise
    raise AssertionError("an unauthorized operation succeeded")


def demo(binary: Path, root: Path) -> None:
    env = {**os.environ, "FABRIC_PASSPHRASE": "disposable-demo",
           "CIRCLE_PASSPHRASE": "disposable-demo"}
    processes = []
    logs = []

    def run(*arguments):
        return subprocess.check_output([str(binary), *map(str, arguments)], env=env,
                                       stderr=subprocess.STDOUT, text=True)

    def start(name):
        node, astral = root / name / "node", root / name / "astral"
        log = root / (name + ".log")
        output = log.open("w")
        logs.append((log, output))
        process = subprocess.Popen([
            str(binary), "up", "-state", str(node), "-astral-state", str(astral),
            "-no-tun", "-no-vault",
        ], env=env, stdout=output, stderr=subprocess.STDOUT)
        processes.append(process)
        return node, astral, log

    def listening(log):
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"a demo node exited with status {process.returncode}")
        match = re.search(r"listening on (/ip4/127\.0\.0\.1/tcp/\d+/p2p/\w+)", log.read_text())
        return match.group(1) if match else None

    owner = root / "owner" / "node"
    worker = root / "worker" / "node"
    try:
        run("init", "-state", owner, "-name", "owner", "-circle", "demo",
            "-subnet", "", "-listen", "/ip4/127.0.0.1/tcp/0")
        run("queue", "on", "-state", owner)
        _, owner_astral, owner_log = start("owner")
        address = eventually(lambda: listening(owner_log), "owner's listener")
        eventually(lambda: (owner / "control.sock").exists(), "owner's control socket")
        identity = run("join", "-state", worker)
        worker_did = re.search(r"this device: (did:key:\w+)", identity).group(1)
        bundle = root / "worker.bundle"
        run("circle", "admit", "-state", owner, "-circle", "demo", "-name", "worker",
            "-gate", address, "-out", bundle, worker_did)
        run("join", "-state", worker, "-listen", "/ip4/127.0.0.1/tcp/0", bundle)
        _, worker_astral, worker_log = start("worker")
        eventually(lambda: listening(worker_log), "worker's listener")
        eventually(lambda: (worker / "control.sock").exists(), "worker's control socket")
        eventually(lambda: (owner_astral / "control.sock").exists(), "owner's queue socket")
        eventually(lambda: (worker_astral / "control.sock").exists(), "worker's queue socket")
        for name in ("jobs", "other"):
            run("astral", "subject", "-state", owner_astral, "-delivery", "shared",
                "subject:queues/" + name)
        caller_key, worker_key = root / "caller.key", root / "worker.key"
        for state, verbs, output in ((owner, "publish", caller_key), (worker, "claim", worker_key)):
            run("astral", "key", "new", "-fabric-state", state,
                "-res", "subject:queues/jobs", "-verbs", verbs, "-out", output)

        caller = Fabric(key=caller_key, queue_state=owner_astral).queue("jobs")
        worker_client = Fabric(key=worker_key, queue_state=worker_astral)
        jobs = worker_client.queue("jobs")
        pending = caller.request(Event(id="demo-job", kind="request", source="caller", content="hello"))
        claims = eventually(lambda: jobs.claim(consumer="worker"), "the remote request")
        claim, = claims
        assert claim.event.id == pending.id and claim.awaited
        assert jobs.complete(claim.event.id, Event(kind="result", source="worker",
                                                  content=claim.event.content.upper()))
        reply = pending.result(timeout=10)
        assert reply is not None and reply.content == "HELLO" and reply.signature
        pending.release()
        print("PASS: request crossed two Fabric nodes; caller verified the worker's signed reply.", flush=True)

        expect_denied(lambda: jobs.publish(Event(kind="request", source="worker", content="not allowed")))
        eventually(lambda: expect_denied(lambda: worker_client.queue("other").claim(consumer="worker")),
                   "the out-of-scope queue refusal")
        print("PASS: claim-only worker cannot publish jobs or consume another queue.", flush=True)

        key_id = json.loads(worker_key.read_text())["id"]
        run("astral", "key", "revoke", "-fabric-state", worker, key_id)

        def revoked():
            try:
                jobs.claim(consumer="worker")
            except FabricError as error:
                if "denied" in str(error):
                    return True
                raise
            return False  # The remote host may not have received the removal yet.

        eventually(revoked, "revocation")
        print("PASS: revoked worker credential is refused.", flush=True)
    except BaseException:
        for path, output in logs:
            output.flush()
            print(f"\n{path.name}:\n" + "\n".join(path.read_text().splitlines()[-30:]))
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for _, output in logs:
            output.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fabric", default="fabric", help="built Fabric binary or command on PATH")
    args = parser.parse_args()
    binary = Path(shutil.which(args.fabric) or args.fabric).resolve()
    if not binary.is_file():
        parser.error("build Fabric first, then pass --fabric /path/to/fabric")
    with tempfile.TemporaryDirectory(prefix="ioa-demo-") as temporary:
        demo(binary, Path(temporary))


if __name__ == "__main__":
    main()
