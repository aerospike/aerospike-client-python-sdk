#!/usr/bin/env python3
"""SDK configuration file: named behaviors and hot-reload.

Covers the ``AEROSPIKE_SDK_CONFIG_URL`` config file — how a file's ``system:``
settings and ``behaviors:`` profiles reach a connected client, and how edits
hot-reload into a live session.

Part 1 uses the shipped ``examples/sdk-config-example.yaml`` (read-only).
Part 2 writes a small temporary file and edits it while connected to show a
behavior change propagating to an already-created session.

The SDK never configures logging itself. To see the config log lines, run
with ``AEROSPIKE_LOG_LEVEL=INFO`` (load / register / reload breadcrumbs) or
``AEROSPIKE_LOG_LEVEL=DEBUG`` (adds the "watching <file>" monitor line, plus
noisier core logs). They are emitted on the ``aerospike_sdk.behavior`` logger,
which a host app can raise on its own without enabling the rest of the SDK.
"""

import asyncio
import os
import tempfile
import time
from pathlib import Path

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import OpKind, OpShape, get_behavior

_SHIPPED_CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"


def _read_total_timeout(behavior: Behavior) -> float | None:
    """The resolved point-read total timeout (seconds) for a behavior."""
    td = behavior.get_settings(OpKind.READ, OpShape.POINT).total_timeout
    return td.total_seconds() if td is not None else None


from _env import Example

class NamedBehaviors(Example):
    async def __init__(self):
        os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_SHIPPED_CONFIG)
        await super().__init__(self)

    async def cleanup(self):
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
        await super().cleanup()

    async def run(self):
        # The file's `system:` settings were applied to the connection during
        # connect(); its `behaviors:` profiles are now in the registry.
        for name in ("high-performance", "batch-optimized"):
            behavior = get_behavior(name)
            parent = behavior.parent.name if behavior and behavior.parent else "-"
            print(f"  behavior {name!r}: registered, parent={parent}")

        # `batch-optimized` inherits `high-performance` and overrides per field.
        session = self.cluster.create_session(get_behavior("high-performance"))
        users = DataSet.of("test", "users")
        key = users.id("cfg_demo_user")
        await session.upsert(key).put({"name": "Ada", "age": 36}).execute()
        stream = await session.query(key).execute()
        record = (await stream.first_or_raise()).record
        print(f"  op via 'high-performance' session: {record.bins}")
        await session.delete(key).execute()


class HotReload(Example):
    async def run(self):
        """Edit the config file while connected; watch a live session update."""
        print("\n=== Part 2: hot-reload into a live session ===")
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "sdk-config.yaml"
            config.write_text(
                "behaviors:\n  demo-fast:\n    allOperations:\n      abandonCallAfter: 1s\n"
            )
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(config)

            session = self.cluster.create_session(get_behavior("demo-fast"))
            print(f"  initial total_timeout: {_read_total_timeout(session.behavior)}s")

            # Edit the file; the client polls its mtime (~1s cadence) and
            # pushes rebuilt policies into sessions bound to the behavior.
            config.write_text(
                "behaviors:\n  demo-fast:\n    allOperations:\n      abandonCallAfter: 5s\n"
            )
            print("  edited config (abandonCallAfter 1s -> 5s); waiting for reload...")

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if _read_total_timeout(session.behavior) == 5.0:
                    break
                await asyncio.sleep(0.25)
            print(f"  live session total_timeout: {_read_total_timeout(session.behavior)}s")
