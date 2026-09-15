#!/usr/bin/env python3
"""Reading both system settings and behaviors from an SDK config file.

An SDK config file has two sections: ``system:`` (per-cluster connection and
transaction settings, keyed by cluster name with a ``DEFAULT``) and
``behaviors:`` (named operation policies). Config text can come from a file on
disk or from a string, and the behaviors it names register at connect so a
session can be bound to one by name.
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional

import _env
from aerospike_sdk.policy import Behavior, Mode, OpKind, OpShape, get_behavior_or_default
from aerospike_sdk.policy.sdk_config_loader import parse_behaviors, parse_sdk_config

_CONFIG = Path(__file__).resolve().parent / "example-config.yaml"

# Config held in a string rather than a file. Kept unindented so it needs no
# dedent before parsing.
_INLINE_CONFIG = """
behaviors:
  my-custom-behavior:
    all_operations:
      abandon_call_after: 5s
      maximum_number_of_call_attempts: 3
    batch_reads:
      max_concurrent_servers: 8
      allow_inline_memory_access: true

  inherits-custom:
    parent: my-custom-behavior
    batch_reads:
      max_concurrent_servers: 16

system:
  DEFAULT:
    connections:
      minimum_connections_per_node: 20
      maximum_connections_per_node: 200
"""


def load_from_file() -> None:
    """Read both sections out of a config file on disk."""
    print("=== Loading from file ===")
    text = _CONFIG.read_text()

    behaviors = parse_behaviors(text)
    print(f"Loaded {len(behaviors)} behaviors:")
    for name in behaviors:
        print(f"  - {name}")

    print("System profiles:")
    for name, settings in parse_sdk_config(text).items():
        print(f"  {name}: "
              f"max_connections_per_node={settings.max_connections_per_node} "
              f"tend_interval={format_duration(settings.tend_interval)}")


def load_from_string() -> None:
    """Parse config text directly, which suits embedded or generated config."""
    print("\n=== Loading from string ===")
    print("Loaded behaviors from string:")
    for name, spec in parse_behaviors(_INLINE_CONFIG).items():
        print(f"  {name} (parent: {spec.parent or 'none'})")

    for name, settings in parse_sdk_config(_INLINE_CONFIG).items():
        print(f"  system {name}: "
              f"max_connections_per_node={settings.max_connections_per_node}")


async def use_with_cluster() -> None:
    """Bind a session to a behavior the config file named."""
    print("\n=== Using with cluster ===")
    # Behaviors register when the client reads the config at connect.
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(_CONFIG)
    try:
        async with await _env.connect().connect() as cluster:
            behavior = get_behavior_or_default("production")
            has_parent = behavior.parent is not None and behavior.parent is not Behavior.DEFAULT
            parent = behavior.parent.name if has_parent else "(none)"
            print(f"  behavior {behavior.name!r} parent={parent}")

            settings = behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
            print(f"  resolved point-read total_timeout: {format_duration(settings.total_timeout)}")

            cluster.create_session(behavior)
            print("  session created with the config behavior")
    finally:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


def format_duration(value: Optional[timedelta]) -> str:
    """Render a duration in milliseconds, or note that it is unset."""
    if value is None:
        return "(not set)"
    return f"{value.total_seconds() * 1000:.0f}ms"


async def main() -> None:
    load_from_file()
    load_from_string()
    await use_with_cluster()


if __name__ == "__main__":
    asyncio.run(main())
