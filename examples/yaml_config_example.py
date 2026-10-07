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
import tempfile
from dataclasses import fields, is_dataclass
from datetime import timedelta
from pathlib import Path

import _env
from aerospike_sdk import DataSet
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

system:
  DEFAULT:
    connections:
      minimum_connections_per_node: 20
      maximum_connections_per_node: 200
"""

_CLUSTER_CONFIG = """
behaviors:
  production:
    all_operations:
      abandon_call_after: 10s
      maximum_number_of_call_attempts: 5

system:
  DEFAULT:
    connections:
      minimum_connections_per_node: 50
      maximum_connections_per_node: 200
"""


def load_from_file() -> None:
    """Read both sections out of a config file on disk."""
    # --- 1) Load both sections from a config file ---
    print("=== Loading from file ===")
    text = _CONFIG.read_text()

    behaviors = parse_behaviors(text)
    print(f"Loaded {len(behaviors)} behaviors:")
    for name in behaviors:
        print(f"  - {name}")

    profiles = parse_sdk_config(text)
    print(f"Default system settings: {describe(profiles['DEFAULT'])}")
    print(f"Production cluster settings: {describe(profiles['production'])}")


def load_from_string() -> None:
    """Parse config text directly, which suits embedded or generated config."""
    # --- 2) Load config from a string ---
    print("\n=== Loading from string ===")
    print("Loaded behaviors from string:")
    for name, spec in parse_behaviors(_INLINE_CONFIG).items():
        # A behavior that names no parent inherits from DEFAULT.
        print(f"  {name} (parent: {spec.parent or Behavior.DEFAULT.name})")


async def use_with_cluster() -> None:
    """Bind a session to a behavior loaded from config text."""
    # --- 3) Use a loaded behavior with a cluster ---
    print("\n=== Using with cluster ===")
    # Behaviors register when the client reads its config file at connect, so
    # the config text is staged in a file the client is pointed at.
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as config:
        config.write(_CLUSTER_CONFIG)
        config.flush()
        os.environ["AEROSPIKE_SDK_CONFIG_URL"] = config.name
        try:
            async with _env.connect().connect() as cluster:
                production = get_behavior_or_default("production")
                settings = production.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
                print("Production behavior total_timeout: "
                      f"{format_duration(settings.total_timeout)}")

                session = cluster.create_session(production)
                key = DataSet.of("test", "mySet").id(1)
                await session.upsert(key).bin("name").set_to("example").execute()
                await session.delete(key).execute()
        finally:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)


def describe(settings: object) -> str:
    """Render the fields a settings object sets, skipping unset ones."""
    parts = []
    for field in fields(settings):
        value = getattr(settings, field.name)
        if value is None:
            continue
        if is_dataclass(value):
            nested = describe(value)
            if nested:
                parts.append(f"{field.name}=({nested})")
        elif isinstance(value, timedelta):
            parts.append(f"{field.name}={format_duration(value)}")
        else:
            parts.append(f"{field.name}={value}")
    return ", ".join(parts)


def format_duration(value: timedelta) -> str:
    """Render a duration in milliseconds."""
    return f"{value.total_seconds() * 1000:.0f}ms"


async def main() -> None:
    load_from_file()
    load_from_string()
    await use_with_cluster()


if __name__ == "__main__":
    asyncio.run(main())
