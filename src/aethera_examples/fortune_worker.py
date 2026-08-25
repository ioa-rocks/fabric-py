"""A durable queue worker that runs the host's ``fortune`` command."""

from __future__ import annotations

import argparse
import shutil
import socket
import subprocess
import sys

from aethera import Event, Fabric, FabricError, Queue


def run_fortune(executable: str) -> str:
    path = shutil.which(executable)
    if path is None:
        raise RuntimeError(f"{executable!r} is not installed on this worker")
    completed = subprocess.run([path, "-s"], check=True, capture_output=True,
                               text=True, timeout=10)
    return completed.stdout.strip()


def serve(queue: Queue, *, consumer: str, executable: str) -> None:
    queue.host(delivery="shared", accepts=["request"])
    print(f"{consumer} listening on {queue.name}", flush=True)
    while True:
        claims = queue.claim(consumer=consumer, lease=30)
        if not claims:
            queue.wait(timeout=30)
            continue
        for claim in claims:
            request = claim.event
            try:
                if not request.reply_target:
                    raise RuntimeError("request has no reply_target")
                content, kind = run_fortune(executable), "result"
            except Exception as error:
                content, kind = str(error), "error"
            try:
                Queue(queue.socket_path, request.reply_target).publish(Event(
                    id=f"{request.id}:fortune-result",
                    kind=kind,
                    source=consumer,
                    content=content,
                    correlation_id=request.correlation_id or request.id,
                ))
                queue.ack(request.id, consumer=consumer)
            except FabricError as error:
                print(f"delivery failed; request remains leased for retry: {error}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", help="Astral state directory")
    parser.add_argument("--consumer", default=f"fortune@{socket.gethostname()}")
    parser.add_argument("--fortune", default="fortune", help="fortune executable")
    args = parser.parse_args()
    try:
        serve(Fabric(queue_state=args.state).queue("fortune"), consumer=args.consumer, executable=args.fortune)
    except KeyboardInterrupt:
        pass
    except (FabricError, RuntimeError) as error:
        print(f"fortune worker: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
