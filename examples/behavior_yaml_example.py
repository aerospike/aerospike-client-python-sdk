#!/usr/bin/env python3
"""Loading named behaviors from an SDK config file.

The config file (resolved from ``AEROSPIKE_SDK_CONFIG_URL`` at connect time)
defines a ``behaviors:`` section — named operation-policy profiles that form an
inheritance tree. This enumerates every registered behavior, shows its parent,
and prints the settings each resolves for a point read.
"""

import os
from pathlib import Path

from _env import SdkConfigFileExample
from aerospike_sdk import Behavior
from aerospike_sdk.policy import OpKind, OpShape, get_all_behaviors


class BehaviorYamlExample(SdkConfigFileExample):
    async def run(self) -> None:
        behaviors = get_all_behaviors()
        print(f"Registered behaviors: {len(behaviors)}")
        for name, behavior in sorted(behaviors.items()):
            parent = behavior.parent.name if behavior.parent else "(none)"
            read = behavior.get_settings(OpKind.READ, OpShape.POINT)
            print(f"  {name:18s} parent={parent:14s} "
                  f"read.total_timeout={read.total_timeout} "
                  f"retries={read.max_retries}")
