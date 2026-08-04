#!/usr/bin/env python3
"""Loading named behaviors from an SDK config file.

The config file (resolved from ``AEROSPIKE_SDK_CONFIG_URL`` at connect time)
defines a ``behaviors:`` section — named operation-policy profiles that form an
inheritance tree. This enumerates every registered behavior, shows its parent,
and prints the settings each resolves for a point read.
"""

import asyncio
import os
from pathlib import Path

import _env
from aerospike_sdk.policy import OpKind, OpShape, get_all_behaviors

_CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"


async def main() -> None:
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    async with await _env.connect().connect():
        try:
            behaviors = get_all_behaviors()
            print(f"Registered behaviors: {len(behaviors)}")
            for name, behavior in sorted(behaviors.items()):
                parent = behavior.parent.name if behavior.parent else "(none)"
                read = behavior.get_settings(OpKind.READ, OpShape.POINT)
                print(f"  {name:18s} parent={parent:14s} "
                      f"read.total_timeout={read.total_timeout} "
                      f"retries={read.max_retries}")
        finally:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
