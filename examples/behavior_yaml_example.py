#!/usr/bin/env python3
"""Loading named behaviors from an SDK config file.

The config file (resolved from ``AEROSPIKE_SDK_CONFIG_URL`` at connect time)
defines a ``behaviors:`` section — named operation-policy profiles that form an
inheritance tree. This enumerates every registered behavior, shows its parent,
and prints the settings each resolves.

Settings resolve per operation *shape*: a profile that overrides only
batch- or query-shaped fields (e.g. ``batchReads.maxConcurrentServers``,
``query.recordQueueSize``) looks identical to its parent for a point read but
differs once the batch/query shape is resolved. This prints all three shapes
so a shape-scoped inheritance override is visible.
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
                point = behavior.get_settings(OpKind.READ, OpShape.POINT)
                batch = behavior.get_settings(OpKind.READ, OpShape.BATCH)
                query = behavior.get_settings(OpKind.READ, OpShape.QUERY)
                print(f"  {name:18s} parent={parent:16s}")
                print(f"    point: total_timeout={point.total_timeout} "
                      f"retries={point.max_retries}")
                print(f"    batch: max_concurrent_nodes={batch.max_concurrent_nodes} "
                      f"allow_inline={batch.allow_inline}")
                print(f"    query: record_queue_size={query.record_queue_size}")
        finally:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
