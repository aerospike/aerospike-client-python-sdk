#!/usr/bin/env python3
"""Connecting with an SDK config file, then running operations.

Point ``AEROSPIKE_SDK_CONFIG_URL`` at a config file and the client applies its
``system:`` settings to the connection and registers its ``behaviors:`` at
connect. Sessions are then created from named behaviors, and every operation on
that session uses the behavior's resolved policy.
"""

import asyncio
import os
from pathlib import Path

import _env
from aerospike_sdk import DataSet
from aerospike_sdk.policy import get_behavior_or_default

_CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"


async def main() -> None:
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    async with await _env.connect().connect() as cluster:
        try:
            # Create a session from a config-defined behavior; the connection's
            # system settings came from the same file.
            session = cluster.create_session(get_behavior_or_default("high-performance"))
            print(f"Session behavior: {session.behavior.name}")

            users = DataSet.of("test", "yaml_users")
            key = users.id("ada")
            await session.upsert(key).put({"name": "Ada", "age": 36}).execute()
            record = (await (await session.query(key).execute()).first_or_raise()).record
            print(f"Read back via configured session: {record.bins}")
            await session.delete(key).execute()
        finally:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
