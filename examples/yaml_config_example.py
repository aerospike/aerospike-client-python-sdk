#!/usr/bin/env python3
"""Reading both system settings and behaviors from an SDK config file.

An SDK config file has two sections: ``system:`` (per-cluster connection and
transaction settings, keyed by cluster name with a ``DEFAULT``) and
``behaviors:`` (named operation policies). This reads both — the system
profiles by parsing the file, the behaviors from the registry populated at
connect.
"""

import os
from pathlib import Path

from _env import SdkConfigFileExample
from aerospike_sdk import Behavior
from aerospike_sdk.policy import get_all_behaviors
from aerospike_sdk.policy.sdk_config_loader import parse_sdk_config


class YamlConfigExample(SdkConfigFileExample):
    async def run(self) -> None:
        # System settings resolve from the file's `system:` section.
        profiles = parse_sdk_config(_CONFIG.read_text())
        print("System profiles:")
        for name, settings in profiles.items():
            print(f"  {name}: "
                  f"max_connections_per_node={settings.max_connections_per_node} "
                  f"tend_interval={settings.tend_interval}")

        # Behaviors register when the client reads the config at connect.
        file_behaviors = {
            name: b for name, b in get_all_behaviors().items()
            if b.parent is not None or name in ("high-performance",)
        }
        print("\nConfig behaviors:")
        for name, behavior in sorted(file_behaviors.items()):
            parent = behavior.parent.name if behavior.parent else "(none)"
            print(f"  {name:18s} parent={parent}")
