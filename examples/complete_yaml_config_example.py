#!/usr/bin/env python3
"""End-to-end SDK configuration: system settings + behaviors + inheritance.

A complete tour of an SDK config file: the ``system:`` profiles (connection and
transaction settings), the ``behaviors:`` tree with parent inheritance, and the
per-operation settings a behavior resolves. Connects with the config applied and
runs one operation through a configured session.
"""

from _env import SdkConfigFileExample
from aerospike_sdk import DataSet
from aerospike_sdk.policy import (
    OpKind,
    OpShape,
    get_all_behaviors,
    get_behavior_or_default,
)
from aerospike_sdk.policy.sdk_config_loader import parse_sdk_config


class CompleteYamlSystemSettings(SdkConfigFileExample):
    async def run(self) -> None:
        print("=== System settings ===")
        for name, s in parse_sdk_config(self._CONFIG.read_text()).items():
            txns = s.transactions
            print(f"  {name}: "
                  f"connections={s.min_connections_per_node}..{s.max_connections_per_node} "
                  f"tend_interval={s.tend_interval} "
                  f"implicit_batch_txns={txns.implicit_batch_write_transactions}")


class CompleteYamlBehaviors(SdkConfigFileExample):
    async def run(self) -> None:
        print("\n=== Behaviors ===")
        for name, behavior in sorted(get_all_behaviors().items()):
            if behavior.parent is None and name not in ("high-performance",):
                continue  # skip framework defaults; show config-defined ones
            parent = behavior.parent.name if behavior.parent else "(none)"
            read = behavior.get_settings(OpKind.READ, OpShape.POINT)
            print(f"  {name:18s} parent={parent:14s} "
                  f"read.total_timeout={read.total_timeout}")


class CompleteYamlConfiguredOperation(SdkConfigFileExample):
    async def run(self) -> None:
        print("\n=== Operation via configured session ===")
        session = self.cluster.create_session(get_behavior_or_default("batch-optimized"))
        key = DataSet.of("test", "cfg_complete").id("k1")
        await session.upsert(key).put({"v": 1}).execute()
        record = (await (await session.query(key).execute()).first_or_raise()).record
        print(f"  behavior={session.behavior.name} bins={record.bins}")
        await session.delete(key).execute()
