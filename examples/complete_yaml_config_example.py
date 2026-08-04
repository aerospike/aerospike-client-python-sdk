#!/usr/bin/env python3
"""End-to-end SDK configuration: system settings + behaviors + inheritance.

A complete tour of an SDK config file: the ``system:`` profiles (connection and
transaction settings), the ``behaviors:`` tree with parent inheritance, and the
per-operation settings a behavior resolves. Connects with the config applied and
runs one operation through a configured session.
"""

import asyncio
import os
from pathlib import Path

import _env
from aerospike_sdk import DataSet
from aerospike_sdk.policy import (
    OpKind,
    OpShape,
    get_all_behaviors,
    get_behavior_or_default,
)
from aerospike_sdk.policy.sdk_config_loader import parse_sdk_config

_CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"


async def main() -> None:
    # 1. System settings (per-cluster connection + transaction config).
    print("=== System settings ===")
    for name, s in parse_sdk_config(_CONFIG.read_text()).items():
        txns = s.transactions
        print(f"  {name}: "
              f"connections={s.min_connections_per_node}..{s.max_connections_per_node} "
              f"tend_interval={s.tend_interval} "
              f"implicit_batch_txns={txns.implicit_batch_write_transactions}")

    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    async with await _env.connect().connect() as cluster:
        try:
            # 2. Behaviors + inheritance.
            print("\n=== Behaviors ===")
            for name, behavior in sorted(get_all_behaviors().items()):
                if behavior.parent is None and name not in ("high-performance",):
                    continue  # skip framework defaults; show config-defined ones
                parent = behavior.parent.name if behavior.parent else "(none)"
                read = behavior.get_settings(OpKind.READ, OpShape.POINT)
                print(f"  {name:18s} parent={parent:14s} "
                      f"read.total_timeout={read.total_timeout}")

            # 3. A live operation through a configured session.
            print("\n=== Operation via configured session ===")
            session = cluster.create_session(get_behavior_or_default("batch-optimized"))
            key = DataSet.of("test", "cfg_complete").id("k1")
            await session.upsert(key).put({"v": 1}).execute()
            record = (await (await session.query(key).execute()).first_or_raise()).record
            print(f"  behavior={session.behavior.name} bins={record.bins}")
            await session.delete(key).execute()
        finally:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
