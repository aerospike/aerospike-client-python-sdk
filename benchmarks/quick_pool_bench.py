#!/usr/bin/env python3
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

"""Quick multi-loop perf check — single Client vs AsyncPool, RU,50 fast path.

Mirrors the per-op work of ``benchmarks.workers._one_op_async`` on the fast
path (one int bin by default, random key in ``[0, keys)``, 50/50 read/write
split, ``session.get`` and ``session.put`` primitives — no QueryBuilder).
Single Client at ``loop_count=1`` matches the PAR-357 async Fast Path
baseline; ``AsyncPool`` at higher counts measures the multi-loop scaling.

Concurrency model: tasks-per-loop is fixed (default 32, matching PAR-357's
``-z 32``), so total active tasks = ``tasks_per_loop * loop_count``.  This
matches the plan's "per-loop TPS" framing — each loop saturates independently
in the regime PAR-357 measured.

Example::

    # Single-loop baseline (should match PAR-357 ~52K TPS)
    python -m benchmarks.quick_pool_bench --host psdk-bench:3100 --loop-counts 1

    # Full quick sweep
    python -m benchmarks.quick_pool_bench --host psdk-bench:3100 --loop-counts 1,4,8
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from typing import List, Tuple

from aerospike_async import ClientPolicy
from aerospike_async.exceptions import RecordNotFound
from aerospike_sdk import AsyncPool
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from benchmarks.record_spec import full_bins, parse_bin_spec


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--host", default="psdk-bench:3100",
                   help="Aerospike seed (default: psdk-bench:3100).")
    p.add_argument("--namespace", default="test")
    p.add_argument("--set-name", default="bench")
    p.add_argument("-k", "--keys", type=int, default=100_000,
                   help="Random key space (default: 100000).")
    p.add_argument("-o", "--bins", default="I1",
                   help="Bin spec, e.g. I1, S128, B1024 (default: I1 = one int bin).")
    p.add_argument("--read-pct", type=int, default=50,
                   help="Read percent for RU workload (default: 50).")
    p.add_argument("-z", "--tasks-per-loop", type=int, default=32,
                   help="Concurrent asyncio tasks per loop (default: 32).")
    p.add_argument("-d", "--duration", type=float, default=10.0,
                   help="Measurement window seconds (default: 10).")
    p.add_argument("--loop-counts", default="1",
                   help="Comma-separated list of loop counts to test, e.g. 1,4,8.")
    return p.parse_args()


async def _worker(
    session,
    dataset: DataSet,
    fields,
    keys: int,
    read_pct: int,
    deadline: float,
) -> int:
    """Run get/put ops until ``deadline``; return op count.

    ``RecordNotFound`` from reads against unwritten keys is counted as a real
    round-trip (matches the existing benchmark's behavior — the wire cost is
    paid regardless of whether the record exists).
    """
    rng = random.Random()
    count = 0
    while time.monotonic() < deadline:
        kid = rng.randrange(keys)
        key = dataset.id(kid)
        is_read = rng.randint(1, 100) > (100 - read_pct)
        try:
            if is_read:
                await session.get(key)
            else:
                await session.put(key, full_bins(fields))
        except RecordNotFound:
            pass
        count += 1
    return count


async def _run_single_loop(
    args: argparse.Namespace, fields, dataset: DataSet
) -> Tuple[int, float]:
    async with Client(seeds=args.host, policy=ClientPolicy()) as client:
        session = client.create_session()
        t0 = time.monotonic()
        deadline = t0 + args.duration
        tasks = [
            asyncio.create_task(
                _worker(session, dataset, fields, args.keys, args.read_pct, deadline)
            )
            for _ in range(args.tasks_per_loop)
        ]
        results = await asyncio.gather(*tasks)
        return sum(results), time.monotonic() - t0


async def _run_pool(
    args: argparse.Namespace,
    fields,
    dataset: DataSet,
    loop_count: int,
) -> Tuple[int, float]:
    def factory() -> Client:
        return Client(seeds=args.host, policy=ClientPolicy())

    async with AsyncPool(factory, loop_count=loop_count) as pool:
        t0 = time.monotonic()
        deadline = t0 + args.duration

        async def loop_work(client: Client, _idx: int) -> int:
            session = client.create_session()
            tasks = [
                asyncio.create_task(
                    _worker(session, dataset, fields, args.keys, args.read_pct, deadline)
                )
                for _ in range(args.tasks_per_loop)
            ]
            results = await asyncio.gather(*tasks)
            return sum(results)

        per_loop = await pool.map(loop_work, list(range(loop_count)))
        return sum(per_loop), time.monotonic() - t0


async def _run_one(args: argparse.Namespace, fields, dataset: DataSet, loop_count: int):
    if loop_count == 1:
        return await _run_single_loop(args, fields, dataset)
    return await _run_pool(args, fields, dataset, loop_count)


async def main() -> None:
    args = parse_args()
    loop_counts = [int(s.strip()) for s in args.loop_counts.split(",") if s.strip()]
    if not loop_counts:
        raise SystemExit("--loop-counts must contain at least one integer")
    fields = parse_bin_spec(args.bins)
    dataset = DataSet.of(args.namespace, args.set_name)

    print(
        f"Host: {args.host}  namespace={args.namespace}  set={args.set_name}\n"
        f"Workload: RU,{args.read_pct}  keys={args.keys:,}  bins={args.bins}  "
        f"duration={args.duration}s  tasks/loop={args.tasks_per_loop}"
    )
    print()
    print(f"{'loops':>6}  {'total_tasks':>12}  {'ops':>14}  {'elapsed_s':>10}  {'TPS':>12}  {'TPS/loop':>10}")
    print("-" * 76)

    results: List[Tuple[int, int, float, float]] = []
    for n in loop_counts:
        ops, elapsed = await _run_one(args, fields, dataset, n)
        tps = ops / elapsed
        per_loop = tps / n
        total_tasks = args.tasks_per_loop * n
        print(f"{n:>6d}  {total_tasks:>12d}  {ops:>14,}  {elapsed:>10.2f}  {tps:>12,.0f}  {per_loop:>10,.0f}")
        results.append((n, ops, elapsed, tps))

    if len(results) > 1:
        base_n, _, _, base_tps = results[0]
        print()
        print(f"Scaling vs {base_n}-loop baseline ({base_tps:,.0f} TPS):")
        for n, _ops, _elapsed, tps in results[1:]:
            print(f"  {n:>2}-loop: {tps / base_tps:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())
