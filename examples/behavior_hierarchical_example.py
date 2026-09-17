#!/usr/bin/env python3
"""Resolving a hierarchical behavior, and re-resolving it after a config edit.

A ``Behavior`` resolves its settings per operation context — the (kind, shape,
mode) triple. Child behaviors inherit from a parent and override per field, so
the resolved value for a field depends on both the profile and the context.

The config file is read at connect and re-read when it changes, and the new
settings push into live sessions already bound to those behaviors. This prints
the resolved settings once, waits briefly, then prints them again so an edit
made to the file in between shows up in the second pass.

Output labels use the config file's own keys (``abandon_call_after``,
``max_concurrent_servers``, ...) so each printed value maps straight back to
the line in the YAML that produced it.
"""

import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import _env
from aerospike_sdk.policy import Behavior, Mode, OpKind, OpShape, get_behavior

_CONFIG = Path(__file__).resolve().parent / "behavior-hierarchical-example.yaml"

# The profiles walked below, in file order: two roots, a child of the first, and
# a loose development profile.
_PROFILES = ("high-performance", "high-reliability", "batch-optimized", "development")


async def demonstrate_dynamic_reloading() -> None:
    """Resolve the config twice, so an edit between passes is visible."""
    print("=== Behavior hierarchical example with dynamic reloading ===\n")
    print(f"Watching config file: {_CONFIG}")

    async with _env.connect().connect():
        print(f"\n{timestamp()} === Initial configuration ===")
        display_current_settings()

        print("\n" + "=" * 70)
        print("Monitoring for changes... Modify the config file to see dynamic reloading.")
        print("=" * 70)

        # The pause is the window to edit the config file and watch the second
        # pass pick the change up.
        await asyncio.sleep(2)

        print(f"\n{timestamp()} === Current configuration ===")
        display_current_settings()


def display_current_settings() -> None:
    """Print every profile the config file defines."""
    for name in _PROFILES:
        display_behavior_settings(name)


def display_behavior_settings(name: str) -> None:
    """Print one profile's resolved settings, grouped by operation context."""
    behavior = get_behavior(name)
    if behavior is None:
        print(f"  {name}: (not found, using DEFAULT)")
        return

    print(f"\n  {name}:")
    if behavior.parent is not None and behavior.parent is not Behavior.DEFAULT:
        print(f"    parent: {behavior.parent.name}")

    all_ops = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
    print("    all_operations:")
    print(f"      abandon_call_after: {format_duration(all_ops.total_timeout)}")
    print(f"      maximum_number_of_call_attempts: {_attempts(all_ops.max_retries)}")
    print(f"      delay_between_retries: {format_duration(all_ops.retry_delay)}")
    print(f"      wait_for_call_to_complete: {format_duration(all_ops.socket_timeout)}")

    writes = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
    print("    retryable_writes:")
    print(f"      use_durable_delete: {writes.durable_delete}")
    print(f"      maximum_number_of_call_attempts: {_attempts(writes.max_retries)}")
    print(f"      delay_between_retries: {format_duration(writes.retry_delay)}")

    batch_reads = behavior.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
    print("    batch_reads:")
    print(f"      max_concurrent_servers: {batch_reads.max_concurrent_nodes}")
    print(f"      allow_inline_memory_access: {batch_reads.allow_inline}")
    print(f"      allow_inline_ssd_access: {batch_reads.allow_inline_ssd}")

    batch_writes = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP)
    print("    batch_writes:")
    print(f"      max_concurrent_servers: {batch_writes.max_concurrent_nodes}")
    print(f"      abandon_call_after: {format_duration(batch_writes.total_timeout)}")

    query = behavior.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
    print("    query:")
    print(f"      record_queue_size: {query.record_queue_size}")
    print(f"      max_concurrent_servers: {query.max_concurrent_nodes}")

    sc_reads = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.SC)
    print("    consistency_mode_reads:")
    read_consistency = _enum_name(sc_reads.read_mode_sc)
    print(f"      read_consistency: {read_consistency}")
    print(f"      abandon_call_after: {format_duration(sc_reads.total_timeout)}")


def timestamp() -> str:
    """Wall-clock stamp, so the two passes below are distinguishable."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def format_duration(value: Optional[timedelta]) -> str:
    """Render a duration in milliseconds, or note that it is unset."""
    if value is None:
        return "(not set)"
    return f"{value.total_seconds() * 1000:.0f}ms"


def _attempts(max_retries: Optional[int]) -> object:
    """Convert resolved retries back to the config key's call-attempt count."""
    return max_retries + 1 if max_retries is not None else "(not set)"


def _enum_name(value: object) -> str:
    """Render an enum value by its bare member name (e.g. SESSION)."""
    return str(value).rsplit(".", 1)[-1] if value is not None else "(not set)"


async def main() -> None:
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    try:
        await demonstrate_dynamic_reloading()
    finally:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


if __name__ == "__main__":
    asyncio.run(main())
