#!/usr/bin/env python3
"""Connecting with an SDK config file, then running operations.

Point ``AEROSPIKE_SDK_CONFIG_URL`` at a config file and the client applies its
``system:`` settings to the connection and registers its ``behaviors:`` at
connect. Sessions are then created from named behaviors, and every operation on
that session uses the behavior's resolved policy.
"""

import os
from pathlib import Path

from _env import SdkConfigFileExample
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import get_behavior_or_default



class YamlConfigConnectionExample(SdkConfigFileExample):
    async def run(self) -> None:
        # Create a session from a config-defined behavior; the connection's
        # system settings came from the same file.
        session = self.cluster.create_session(get_behavior_or_default("high-performance"))
        print(f"Session behavior: {session.behavior.name}")

        users = DataSet.of("test", "yaml_users")
        key = users.id("ada")
        await session.upsert(key).put({"name": "Ada", "age": 36}).execute()
        record = (await (await session.query(key).execute()).first_or_raise()).record
        print(f"Read back via configured session: {record.bins}")
        await session.delete(key).execute()
