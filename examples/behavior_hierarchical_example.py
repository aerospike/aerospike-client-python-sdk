#!/usr/bin/env python3
"""Resolving a hierarchical behavior across the operation matrix.

A ``Behavior`` resolves its settings per operation context — the (kind, shape,
mode) triple. Child behaviors inherit from a parent and override per field.
This prints the resolved settings for a child behavior across that matrix,
alongside its parent, so the inheritance is visible.
"""

import os
from pathlib import Path

from _env import SdkConfigFileExample
from aerospike_sdk import Behavior
from aerospike_sdk.policy import Mode, OpKind, OpShape, get_behavior


def _show(behavior) -> None:
    print(f"\nBehavior {behavior.name!r}"
          + (f" (parent: {behavior.parent.name})" if behavior.parent else ""))
    for kind in (OpKind.READ, OpKind.WRITE_RETRYABLE):
        for shape in (OpShape.POINT, OpShape.BATCH):
            for mode in (Mode.AP, Mode.SC):
                s = behavior.get_settings(kind, shape, mode)
                print(f"  {kind.name:19s} {shape.name:5s} {mode.name}: "
                      f"total_timeout={s.total_timeout} retries={s.max_retries}")


class BehaviorHierarchicalExample(SdkConfigFileExample):
    async def run(self) -> None:
        child = get_behavior("batch-optimized")
        if child is None:
            print("Skipped: expected behavior 'batch-optimized' not in config.")
            return
        if child.parent is not None:
            _show(child.parent)
        _show(child)
