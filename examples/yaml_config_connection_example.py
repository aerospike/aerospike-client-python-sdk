#!/usr/bin/env python3
"""Connecting with an SDK config file, then running operations.

Point ``AEROSPIKE_SDK_CONFIG_URL`` at a config file and the client applies its
``system:`` settings to the connection and registers its ``behaviors:`` at
connect. Sessions are then created from named behaviors, and every operation on
that session uses the behavior's resolved policy — so a process can hold one
session per workload shape without threading policy through each call.
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional

import _env
from aerospike_sdk import DataSet
from aerospike_sdk.policy import Behavior, Mode, OpKind, OpShape, get_behavior, get_behavior_or_default

_CONFIG = Path(__file__).resolve().parent / "client-config-example.yaml"
_ENV_CONFIG_URL = "AEROSPIKE_SDK_CONFIG_URL"

SET = DataSet.of("test", "yaml_users")

# The profiles the config file defines, and what each one is shaped for.
_PROFILES = (
    ("fast-operations", "real-time, latency-sensitive queries"),
    ("safe-operations", "critical operations requiring high reliability"),
    ("batch-fast", "batch operations (inherits from fast-operations)"),
)


def demonstrate_behavior_loading() -> None:
    """Report which of the config's profiles actually registered."""
    print("=== Behavior configuration status ===")
    print(f"Config file: {os.environ.get(_ENV_CONFIG_URL, '(not set)')}")

    for name, _ in _PROFILES:
        behavior = get_behavior(name)
        if behavior is None:
            print(f"  Behavior not found (using DEFAULT): {name}")
            continue
        print(f"  Found behavior: {name}")
        show_behavior_settings(behavior)


async def perform_example_operations(session) -> None:
    """Run ordinary operations through a session bound to a config behavior."""
    print("=== Performing example operations ===")

    print("Writing a test record...")
    key = SET.id("user-001")
    await session.upsert(key).put(
        {"name": "Alice", "email": "alice@example.com", "age": 30}
    ).execute()
    print("  Record written successfully.")

    print("Reading the record back...")
    record = (await (await session.query(key).execute()).first_or_raise()).record
    print(f"  Retrieved: {record.bins}")

    print("Writing batch of records...")
    await (
        session.upsert(SET.id("user-002"))
        .put({"name": "Bob", "email": "bob@example.com", "age": 25})
        .upsert(SET.id("user-003"))
        .put({"name": "Charlie", "email": "charlie@example.com", "age": 35})
        .upsert(SET.id("user-004"))
        .put({"name": "Diana", "email": "diana@example.com", "age": 28})
        .execute()
    )
    print("  Batch written successfully.")

    print("Checking record existence...")
    stream = await (
        session.exists(SET.ids("user-001", "user-002", "user-999"))
        .include_missing_keys()
        .execute()
    )
    async for row in stream:
        print(f"  {row.key.value} -> {row.as_bool()}")
    stream.close()

    await session.delete(SET.ids("user-001", "user-002", "user-003", "user-004")).execute()
    print()


def demonstrate_behavior_switching(cluster) -> None:
    """One session per workload shape, each bound to its own profile."""
    print("=== Demonstrating behavior switching ===")
    print("Different behaviors can serve different operation types:\n")

    for position, (name, purpose) in enumerate(_PROFILES, start=1):
        behavior = get_behavior_or_default(name)
        print(f"{position}. {name}: {purpose}")
        show_behavior_settings(behavior)
        if behavior.parent is not None and behavior.parent is not Behavior.DEFAULT:
            print(f"   (parent: {behavior.parent.name})")

    print("\nYou can create one session per operation type:")
    print('  fast_session = cluster.create_session(get_behavior_or_default("fast-operations"))')
    print('  safe_session = cluster.create_session(get_behavior_or_default("safe-operations"))')


def show_behavior_settings(behavior: Behavior) -> None:
    """Print the settings one profile resolves for reads and retryable writes."""
    read = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
    print(f"    - read timeout: {format_duration(read.total_timeout)}")
    print(f"    - max retries: {read.max_retries}")
    write = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
    print(f"    - durable delete: {write.durable_delete}")
    print()


def format_duration(value: Optional[timedelta]) -> str:
    """Render a duration in milliseconds, or note that it is unset."""
    if value is None:
        return "(not set)"
    return f"{value.total_seconds() * 1000:.0f}ms"


async def main() -> None:
    print("=== SDK YAML configuration example ===\n")
    os.environ[_ENV_CONFIG_URL] = str(_CONFIG)
    try:
        async with _env.connect().connect() as cluster:
            print("Successfully connected to cluster!\n")

            demonstrate_behavior_loading()

            behavior = get_behavior_or_default("fast-operations")
            print(f"Using behavior: {behavior.name}\n")
            session = cluster.create_session(behavior)

            await perform_example_operations(session)
            demonstrate_behavior_switching(cluster)
    finally:
        os.environ.pop(_ENV_CONFIG_URL, None)


if __name__ == "__main__":
    asyncio.run(main())
