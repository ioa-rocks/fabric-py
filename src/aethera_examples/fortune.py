"""Request a fortune from any worker reachable through the Fabric mesh."""

from __future__ import annotations

import argparse
import sys

from aethera import Event, Fabric, FabricError


def call(fabric: Fabric, *, timeout: int) -> str:
    # One call: the host of the queue keeps the reply with the request, so
    # there is nothing to declare and nothing to clean up.
    reply = fabric.queue("fortune").call(
        Event(kind="request", source="astral-fortune", content="fortune -s"),
        timeout=timeout,
    )
    if reply.kind == "error":
        raise RuntimeError(reply.content)
    return reply.content


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
