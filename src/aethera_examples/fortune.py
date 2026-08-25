"""Request a fortune from any worker reachable through the Fabric mesh."""

from __future__ import annotations

import argparse
import os
import socket
import sys
import uuid

from aethera import Event, Fabric, FabricError

def call(fabric: Fabric, *, timeout: int) -> str:
    request_id = uuid.uuid4().hex
    entity = f"fortune-caller-{socket.gethostname()}-{os.getpid()}-{request_id[:8]}"
    replies = fabric.queue(f"_reply/{entity}").host(delivery="exclusive", accepts=["result", "error"])
    requests = fabric.queue("fortune")
    requests.publish(Event(
        id=request_id,
        kind="request",
        source="astral-fortune",
        content="fortune -s",
        reply_target=replies.name,
        correlation_id=request_id,
    ))
    remaining = timeout
    while remaining > 0:
        claims = replies.claim(consumer=entity, lease=30)
        if claims:
            response = claims[0].event
            replies.ack(response.id, consumer=entity)
            if response.correlation_id != request_id:
                continue
            if response.kind == "error":
                raise RuntimeError(response.content)
            return response.content
        wait_for = min(remaining, 10)
        replies.wait(timeout=wait_for)
        remaining -= wait_for
    raise TimeoutError(f"no fortune worker replied within {timeout}s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", help="Astral state directory")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    try:
        print(call(Fabric(queue_state=args.state), timeout=args.timeout))
    except (FabricError, RuntimeError, TimeoutError) as error:
        print(f"fortune: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
