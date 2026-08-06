#!/usr/bin/env python3
"""Run every illustrative example class.

Discovers async example classes automatically from ``examples/*.py`` (excluding
test runners and this file) and runs each one that defines its own :meth:`run`.
The sync example (:class:`SyncBasicExample`) is run separately outside the
async loop.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import sys
from pathlib import Path

from _env import Example, SdkConfigFileExample
from sync_example import SyncBasicExample

EXCLUDED_MODULES = frozenset({
    "_env",
    "run_all_examples",
    "operation_differences",
    "ael_test_spec_runner",
    "sync_example",
})

ROOT_TYPES = (Example, SdkConfigFileExample)


def _discover_examples() -> list[type[Example]]:
    examples_dir = Path(__file__).resolve().parent
    discovered: list[type[Example]] = []

    for path in examples_dir.glob("*.py"):
        stem = path.stem
        if stem in EXCLUDED_MODULES:
            continue

        module = importlib.import_module(stem)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            if obj in ROOT_TYPES:
                continue
            if not issubclass(obj, Example):
                continue
            if "run" not in obj.__dict__:
                continue
            discovered.append(obj)

    return discovered


async def _run_example(cls: type[Example]) -> None:
    example = await cls()
    if example._skipped:
        await example.cleanup()
        return

    try:
        await example.run()
    finally:
        await example.cleanup()


async def run_all() -> None:
    for cls in _discover_examples():
        print(f"=== {cls.__name__} ===")
        await _run_example(cls)
        print()


def run_sync_example() -> None:
    print(f"=== {SyncBasicExample.__name__} ===")
    example = SyncBasicExample()
    if example._skipped:
        example.cleanup()
        return

    try:
        example.run()
    finally:
        example.cleanup()
    print()


def _print_run_plan() -> None:
    """Print the discovered run plan (for debugging)."""
    for cls in _discover_examples():
        print(f"async: {cls.__name__}")
    print(f"sync: {SyncBasicExample.__name__}")


if __name__ == "__main__":
    if "--list" in sys.argv:
        _print_run_plan()
    else:
        asyncio.run(run_all())
        run_sync_example()
