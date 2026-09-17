#!/usr/bin/env python3
"""End-to-end SDK configuration: system settings + behaviors + inheritance.

A complete tour of an SDK config file: the ``system:`` profiles (connection and
transaction settings), the ``behaviors:`` tree with parent inheritance, and the
per-operation settings a behavior resolves. Finishes by connecting with the
config applied and running operations through sessions bound to two different
profiles.
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional

import _env
from aerospike_sdk import DataSet
from aerospike_sdk.policy import (
    Behavior,
    Mode,
    OpKind,
    OpShape,
    SystemSettings,
    get_all_behaviors,
    get_behavior,
)
from aerospike_sdk.policy.sdk_config_loader import parse_sdk_config

_CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"

SET = DataSet.of("test", "cfg_complete")

# Every profile the config file defines, each child listed after its parent so
# inheritance reads top-down.
_PROFILES = (
    "high-performance",
    "high-reliability",
    "batch-optimized",
    "development",
    "analytics",
    "real-time",
    "cache-refresh",
)

# The subset exercised against the cluster: enough to show a session honoring
# its profile without paying for a round trip per profile.
_CLUSTER_PROFILES = ("high-performance", "high-reliability", "batch-optimized", "real-time")


def display_system_settings() -> None:
    """Print the DEFAULT profile, then any per-cluster profile layered on it."""
    print("=== System settings ===\n")
    profiles = parse_sdk_config(_CONFIG.read_text())

    print("--- DEFAULT system settings ---")
    display_system_settings_details(profiles.get("DEFAULT"))

    for name, settings in profiles.items():
        if name == "DEFAULT":
            continue
        print(f"--- Cluster {name!r} system settings ---")
        display_system_settings_details(settings)


def display_system_settings_details(settings: Optional[SystemSettings]) -> None:
    """Print one system profile, grouped the way the config file groups it."""
    if settings is None:
        print("  (not configured)")
        return

    print("  connections:")
    print(f"    min_connections_per_node: {settings.min_connections_per_node}")
    print(f"    max_connections_per_node: {settings.max_connections_per_node}")
    print(f"    max_socket_idle_time: {format_duration(settings.max_socket_idle_time)}")

    print("  circuit_breaker:")
    print(f"    num_tend_intervals_in_error_window: {settings.num_tend_intervals_in_error_window}")
    print(f"    max_errors_in_error_window: {settings.max_errors_in_error_window}")

    print("  refresh:")
    print(f"    tend_interval: {format_duration(settings.tend_interval)}")

    # A per-cluster profile only carries what it overrides; the rest resolves
    # from DEFAULT at connect, so unset reads as such here.
    txns = settings.transactions
    print("  transactions:")
    print(f"    implicit_batch_write_transactions: "
          f"{_render(txns.implicit_batch_write_transactions)}")
    print(f"    number_of_attempts: {_render(txns.number_of_attempts)}")
    print()


def display_all_behaviors() -> None:
    """Print every profile the config file registered."""
    print("=== Behavior definitions ===\n")
    behaviors = get_all_behaviors()
    print(f"Total behaviors loaded: {len(behaviors)}\n")

    for name in _PROFILES:
        behavior = get_behavior(name)
        if behavior is not None:
            display_behavior_details(behavior)


def display_behavior_details(behavior: Behavior) -> None:
    """Print one profile's settings across the operation contexts it covers."""
    print(f"--- Behavior: {behavior.name} ---")
    if behavior.parent is not None and behavior.parent is not Behavior.DEFAULT:
        print(f"  parent: {behavior.parent.name}")

    read_ap = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
    print("  point reads (AP):")
    print(f"    total_timeout: {format_duration(read_ap.total_timeout)}")
    print(f"    max_retries: {read_ap.max_retries}")
    print(f"    retry_delay: {format_duration(read_ap.retry_delay)}")
    ttl_pct = read_ap.read_touch_ttl_percent
    print(f"    read_touch_ttl_percent: "
          f"{f'{ttl_pct}%' if ttl_pct is not None else '(not set)'}")

    read_sc = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.SC)
    print("  point reads (SC):")
    print(f"    total_timeout: {format_duration(read_sc.total_timeout)}")
    read_mode_sc = (str(read_sc.read_mode_sc).rsplit(".", 1)[-1]
                    if read_sc.read_mode_sc is not None else "(not set)")
    print(f"    read_mode_sc: {read_mode_sc}")

    retryable = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
    print("  retryable writes:")
    print(f"    total_timeout: {format_duration(retryable.total_timeout)}")
    print(f"    max_retries: {retryable.max_retries}")
    print(f"    durable_delete: {retryable.durable_delete}")

    non_retryable = behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP)
    print("  non-retryable writes:")
    print(f"    total_timeout: {format_duration(non_retryable.total_timeout)}")
    print(f"    durable_delete: {non_retryable.durable_delete}")

    batch_read = behavior.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
    print("  batch reads:")
    print(f"    total_timeout: {format_duration(batch_read.total_timeout)}")
    print(f"    max_concurrent_nodes: {batch_read.max_concurrent_nodes}")
    print(f"    allow_inline: {batch_read.allow_inline}")
    print(f"    allow_inline_ssd: {batch_read.allow_inline_ssd}")

    batch_write = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP)
    print("  batch writes:")
    print(f"    total_timeout: {format_duration(batch_write.total_timeout)}")
    print(f"    max_concurrent_nodes: {batch_write.max_concurrent_nodes}")

    query = behavior.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
    print("  query:")
    print(f"    total_timeout: {format_duration(query.total_timeout)}")
    print(f"    record_queue_size: {query.record_queue_size}")
    print()


def demonstrate_behavior_inheritance() -> None:
    """Compare a child profile against its parent, field by field.

    Settings resolve per operation *shape*, so a child that overrides only
    batch- and query-shaped fields matches its parent for a point read and
    diverges once the batch or query shape is resolved.
    """
    print("=== Behavior inheritance ===\n")
    _compare_with_parent("batch-optimized", (
        ("point reads - total_timeout", OpKind.READ, OpShape.POINT, "total_timeout"),
        ("batch reads - max_concurrent_nodes", OpKind.READ, OpShape.BATCH, "max_concurrent_nodes"),
        ("query - record_queue_size", OpKind.READ, OpShape.QUERY, "record_queue_size"),
    ))
    _compare_with_parent("analytics", (
        ("query - record_queue_size", OpKind.READ, OpShape.QUERY, "record_queue_size"),
        ("query - max_concurrent_nodes", OpKind.READ, OpShape.QUERY, "max_concurrent_nodes"),
    ))

    # A child can override a single field and inherit everything else.
    cache_refresh = get_behavior("cache-refresh")
    if cache_refresh is not None:
        settings = cache_refresh.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        ttl_pct = settings.read_touch_ttl_percent
        print("cache-refresh - read_touch_ttl_percent:")
        print(f"  {f'{ttl_pct}%' if ttl_pct is not None else '(not set)'} "
              f"(inherits the rest from {cache_refresh.parent.name})")
        print()


def _compare_with_parent(name: str, fields) -> None:
    """Print one child profile's fields beside its parent's, marking each verdict."""
    child = get_behavior(name)
    if child is None or child.parent is None:
        return

    parent = child.parent
    print(f"Comparing {child.name!r} (child) with {parent.name!r} (parent):\n")

    for label, kind, shape, field in fields:
        child_value = getattr(child.get_settings(kind, shape, Mode.AP), field)
        parent_value = getattr(parent.get_settings(kind, shape, Mode.AP), field)
        verdict = "overridden" if child_value != parent_value else "inherited"
        print(f"{label}:")
        print(f"  {parent.name}: {_render(parent_value)}")
        print(f"  {child.name}: {_render(child_value)} ({verdict})")
    print()


async def perform_cluster_operations(cluster) -> None:
    """Exercise each configured profile against the cluster."""
    print("=== Cluster operations ===\n")
    for name in _CLUSTER_PROFILES:
        await test_with_behavior(cluster, name, SET)


async def test_with_behavior(cluster, name: str, dataset: DataSet) -> None:
    """Run one round trip through a session bound to the named profile."""
    behavior = get_behavior(name)
    if behavior is None:
        print(f"Behavior {name!r} not found, skipping...")
        return

    print(f"Testing with behavior: {name}")
    session = cluster.create_session(behavior)
    key = dataset.id(f"k-{name}")

    await session.upsert(key).put({"behavior": name, "value": 42}).execute()
    print("  Write: OK")

    record = (await (await session.query(key).execute()).first_or_raise()).record
    print(f"  Read: OK - {record.bins['behavior']}")

    exists_stream = await (
        session.exists(dataset.ids(f"k-{name}", f"k-{name}-2")).include_missing_keys().execute()
    )
    results = [row.as_bool() async for row in exists_stream]
    exists_stream.close()
    print(f"  Batch exists: OK - {results}")

    await session.delete(key).execute()
    print("  Delete: OK")
    print()


def format_duration(value: Optional[timedelta]) -> str:
    """Render a duration in milliseconds, or note that it is unset."""
    if value is None:
        return "(not set)"
    return f"{value.total_seconds() * 1000:.0f}ms"


def _render(value: object) -> str:
    """Render a resolved setting, formatting durations like the rest of the output."""
    if value is None:
        return "(not set)"
    return format_duration(value) if isinstance(value, timedelta) else str(value)


async def main() -> None:
    print("=== Complete SDK configuration example ===\n")
    display_system_settings()

    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    try:
        async with _env.connect().connect() as cluster:
            display_all_behaviors()
            demonstrate_behavior_inheritance()
            await perform_cluster_operations(cluster)
    finally:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
