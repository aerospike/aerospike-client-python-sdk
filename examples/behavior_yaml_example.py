#!/usr/bin/env python3
"""Loading named behaviors from an SDK config file.

The config file (resolved from ``AEROSPIKE_SDK_CONFIG_URL`` at connect time)
defines a ``behaviors:`` section — named operation-policy profiles that form an
inheritance tree. This enumerates every registered behavior, shows its parent,
then drills into one profile and into a child that overrides part of it.

Settings resolve per operation *shape*: a profile that overrides only
batch- or query-shaped fields (e.g. ``batch_reads.max_concurrent_servers``,
``query.record_queue_size``) looks identical to its parent for a point read but
differs once the batch/query shape is resolved.
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional

import _env
from aerospike_sdk.policy import Behavior, Mode, OpKind, OpShape, get_all_behaviors, get_behavior

_CONFIG = Path(__file__).resolve().parent / "behavior-example.yaml"


def show_loaded_behaviors() -> None:
    """Enumerate every behavior the config file registered."""
    behaviors = get_all_behaviors()
    print("=== Loaded behaviors ===")
    print(f"Total behaviors loaded: {len(behaviors)}\n")

    for name, behavior in sorted(behaviors.items()):
        print(f"--- Behavior: {name} ---")
        if behavior.parent is not None and behavior.parent is not Behavior.DEFAULT:
            print(f"  parent: {behavior.parent.name}")
        demonstrate_settings(behavior)
        print()


def show_profile_detail() -> None:
    """Resolve one profile's settings for two different operation contexts."""
    behavior = get_behavior("high-performance")
    if behavior is None:
        return

    print("=== Detailed example: high-performance behavior ===")
    write = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
    print("Retryable write settings (POINT, AP):")
    print(f"  total_timeout: {format_duration(write.total_timeout)}")
    print(f"  max_retries: {write.max_retries}")
    print(f"  durable_delete: {write.durable_delete}")
    print(f"  retry_delay: {format_duration(write.retry_delay)}")

    query = behavior.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
    print("\nQuery settings (QUERY, AP):")
    print(f"  record_queue_size: {query.record_queue_size}")
    print(f"  max_retries: {query.max_retries}")
    print(f"  total_timeout: {format_duration(query.total_timeout)}\n")


def show_inheritance() -> None:
    """Show which fields a child profile overrides and which it inherits."""
    behavior = get_behavior("batch-optimized")
    if behavior is None:
        return

    print("=== Inheritance example: batch-optimized (child of high-performance) ===")
    batch = behavior.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
    print("Batch read settings (inherited + overridden):")
    print(f"  max_concurrent_nodes: {batch.max_concurrent_nodes} (overridden from parent)")
    print(f"  allow_inline: {batch.allow_inline} (overridden from parent)")
    print(f"  total_timeout: {format_duration(batch.total_timeout)} (inherited from parent)")
    print(f"  max_retries: {batch.max_retries} (inherited from parent)")


def demonstrate_settings(behavior: Behavior) -> None:
    """Print one behavior's settings across the three operation shapes."""
    batch = behavior.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
    print(f"  batch reads (AP): max_concurrent_nodes={batch.max_concurrent_nodes}")

    write = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
    print(f"  retryable writes: durable_delete={write.durable_delete}")

    query = behavior.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
    print(f"  query: record_queue_size={query.record_queue_size}")


def format_duration(value: Optional[timedelta]) -> str:
    """Render a duration in milliseconds, or note that it is unset."""
    if value is None:
        return "(not set)"
    return f"{value.total_seconds() * 1000:.0f}ms"


async def main() -> None:
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    try:
        async with _env.connect().connect():
            show_loaded_behaviors()
            show_profile_detail()
            show_inheritance()
    finally:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
