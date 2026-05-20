# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Command-line benchmark driver for the Aerospike Python SDK."""

from __future__ import annotations

import asyncio
import math
import sys
import threading
import tracemalloc
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import benchmarks._env  # noqa: E402, F401 — load aerospike.env before arg parsing
from benchmarks.config import WorkloadKind, build_arg_parser, config_from_args  # noqa: E402
from benchmarks.stats import StatsCollector, latency_column_labels  # noqa: E402
from benchmarks.workers import (  # noqa: E402
    run_async,
    run_async_pool,
    run_legacy_sync,
    run_pac_async,
    run_pac_blocking,
    run_sync,
)


async def _run_async_mode(cfg, runner=None) -> StatsCollector:
    cols, shift = cfg.latency_columns, cfg.latency_shift
    warmup = cfg.warmup_intervals if cfg.with_telemetry else 0
    cooldown = cfg.cooldown_intervals if cfg.with_telemetry else 0
    stats = StatsCollector(
        cols,
        shift,
        warmup,
        cooldown,
        latency_style=getattr(cfg, "latency_style", "columns"),
    )
    stop = asyncio.Event()
    connected = asyncio.Event()
    labels = latency_column_labels(cols, shift)
    n_iv = max(1, math.ceil(cfg.duration_sec))
    stats.set_planned_intervals(n_iv)
    stats.set_interval(0)

    async def ticker() -> None:
        for i in range(n_iv):
            await asyncio.sleep(1.0)
            if stop.is_set():
                return
            stats.sample_cpu()
            snap = stats.end_interval()
            if cfg.with_telemetry:
                stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(stats.format_interval_lines(snap, stamp, labels))
            stats.set_interval(i + 1)

    _runner = runner or run_async
    work = asyncio.create_task(_runner(cfg, stats, stop, connected))

    # Wait for the client to connect before starting the ticker and duration
    # timer.  If the connection fails, the work task completes with an
    # exception and we propagate it immediately instead of printing empty
    # intervals for the full timeout period.
    connect_wait = asyncio.create_task(connected.wait())
    done, _pending = await asyncio.wait(
        [work, connect_wait],
        return_when=asyncio.FIRST_COMPLETED,
    )
    if work in done:
        # Connection failed — propagate the error.
        connect_wait.cancel()
        await work  # raises
        return stats  # unreachable, but satisfies the return type

    connect_wait.cancel()
    print(f"Connected to {cfg.seeds}. Starting benchmark ...")

    tick = asyncio.create_task(ticker())
    sleep_task = asyncio.create_task(asyncio.sleep(cfg.duration_sec))
    await asyncio.wait(
        [work, sleep_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    stop.set()
    tick.cancel()
    try:
        await tick
    except asyncio.CancelledError:
        pass
    await work
    if not sleep_task.done():
        sleep_task.cancel()
        try:
            await sleep_task
        except asyncio.CancelledError:
            pass
    return stats


async def _run_sync_mode(cfg, runner=None) -> StatsCollector:
    """Drive a thread-pool worker (sync, pac-blocking, or legacy-sync)
    from the asyncio loop.

    The asyncio loop only runs the ticker + duration sleep; the actual work
    happens on OS threads driven by ``runner``. Defaults to ``run_sync``
    (PSDK sync); pass ``run_pac_blocking`` for ``--mode pac-blocking`` or
    ``run_legacy_sync`` for ``--mode legacy-sync``.
    """
    _runner = runner or run_sync
    cols, shift = cfg.latency_columns, cfg.latency_shift
    warmup = cfg.warmup_intervals if cfg.with_telemetry else 0
    cooldown = cfg.cooldown_intervals if cfg.with_telemetry else 0
    stats = StatsCollector(
        cols,
        shift,
        warmup,
        cooldown,
        latency_style=getattr(cfg, "latency_style", "columns"),
    )
    sync_stop = threading.Event()
    sync_connected = threading.Event()
    loop_stop = asyncio.Event()
    labels = latency_column_labels(cols, shift)
    n_iv = max(1, math.ceil(cfg.duration_sec))
    stats.set_planned_intervals(n_iv)
    stats.set_interval(0)

    sync_error: BaseException | None = None

    def sync_thread_main() -> None:
        nonlocal sync_error
        try:
            _runner(cfg, stats, sync_stop, sync_connected)
        except BaseException as exc:
            sync_error = exc
            # Unblock the main thread if we never connected.
            sync_connected.set()

    t = threading.Thread(target=sync_thread_main, name="bench-sync", daemon=True)
    t.start()

    # Wait for at least one worker to connect before starting the ticker.
    ok = await asyncio.to_thread(lambda: sync_connected.wait(timeout=60.0))
    if not ok or (sync_error is not None):
        sync_stop.set()
        await asyncio.to_thread(lambda: t.join(timeout=10.0))
        if sync_error is not None:
            raise sync_error
        raise RuntimeError("Sync workers failed to connect within 60 seconds")
    print(f"Connected to {cfg.seeds}. Starting benchmark ...")

    async def ticker() -> None:
        for i in range(n_iv):
            await asyncio.sleep(1.0)
            if loop_stop.is_set():
                return
            stats.sample_cpu()
            snap = stats.end_interval()
            if cfg.with_telemetry:
                stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(stats.format_interval_lines(snap, stamp, labels))
            stats.set_interval(i + 1)

    tick = asyncio.create_task(ticker())
    sleep_task = asyncio.create_task(asyncio.sleep(cfg.duration_sec))
    await sleep_task
    loop_stop.set()
    sync_stop.set()
    tick.cancel()
    try:
        await tick
    except asyncio.CancelledError:
        pass
    await asyncio.to_thread(lambda: t.join(timeout=120.0))
    return stats


async def _truncate_set(cfg) -> None:
    """Truncate the benchmark set to remove stale data."""
    from aerospike_sdk.aio.client import Client
    from aerospike_sdk.dataset import DataSet
    from aerospike_sdk.policy.behavior import Behavior

    from benchmarks._env import client_policy_from_config

    print(f"Truncating {cfg.namespace}.{cfg.set_name} ...")
    policy = client_policy_from_config(cfg)
    async with Client(cfg.seeds, policy=policy) as client:
        session = client.create_session(Behavior.DEFAULT)
        ds = DataSet.of(cfg.namespace, cfg.set_name)
        await session.truncate(ds)
    await asyncio.sleep(1.0)
    print("Truncation complete.")


async def async_main() -> int:
    parser = build_arg_parser()
    ns = parser.parse_args()
    cfg = config_from_args(ns)
    if cfg.duration_sec <= 0:
        print("--duration must be positive", file=sys.stderr)
        return 2
    if cfg.batch_size > 1 and cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        print(
            "batch-size > 1 is not supported for RMI/RMD workloads",
            file=sys.stderr,
        )
        return 2
    if cfg.truncate_before_run:
        await _truncate_set(cfg)

    if cfg.tracemalloc_enabled:
        tracemalloc.start()
    if cfg.mode == "async":
        runner = run_async_pool if cfg.pool_loops > 0 else None
        stats = await _run_async_mode(cfg, runner=runner)
    elif cfg.mode == "pac-async":
        stats = await _run_async_mode(cfg, runner=run_pac_async)
    elif cfg.mode == "pac-blocking":
        stats = await _run_sync_mode(cfg, runner=run_pac_blocking)
    elif cfg.mode == "legacy-sync":
        stats = await _run_sync_mode(cfg, runner=run_legacy_sync)
    elif cfg.mode == "sync":
        stats = await _run_sync_mode(cfg)
    else:
        stats = await _run_sync_mode(cfg)
    labels = latency_column_labels(cfg.latency_columns, cfg.latency_shift)
    for line in stats.summary_lines(labels):
        print(line)

    # Clean up if explicitly requested via --truncate-after.
    if cfg.truncate_after_run:
        await _truncate_set(cfg)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
