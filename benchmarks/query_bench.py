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

"""Secondary-index query benchmark — records per query vs client cost.

Every query is a range filter on an indexed integer bin that matches exactly
``--rows`` records, so the result size is the controlled variable. Each run
reports queries/s, records/s, per-query latency and whole-process CPU per
record (all threads, including the native runtime's workers).

Modes:

* ``pnc-async`` — ``await client.query(...)`` + ``async for`` over the
  recordset, ``-z`` concurrent tasks on one event loop.
* ``psdk-async`` — ``session.query(dataset).filter(...).execute()`` +
  ``async for`` over the record stream, ``-z`` concurrent tasks.
* ``pnc-batch-stream`` — ``await client.batch_stream(...)`` over ``--rows``
  consecutive keys + ``async for`` over the rows, ``-z`` concurrent tasks.
* ``psdk-batch-stream`` — ``session.query(*keys).stream()`` over ``--rows``
  consecutive keys, ``-z`` concurrent tasks.
* ``pnc-blocking`` — ``client.query_blocking(...)`` + ``for``, ``-z`` OS
  threads sharing one client.
* ``legacy-sync`` — legacy C-extension ``query().results()``, ``-z`` OS
  threads sharing one client.

``--point-readers N`` (``pnc-async`` and ``psdk-async``) adds N tasks doing
point reads on the same client and event loop, and reports their latency: a
stream consumer that never yields to the loop shows up as point-read tail
latency.

Example::

    # Populate 200k records with a 256-byte payload and index bin "n"
    python -m benchmarks.query_bench --load 200000 -H 10.138.0.5:3000

    # 1,000 records per query, 8 concurrent tasks, 20 seconds
    python -m benchmarks.query_bench --mode pnc-async --rows 1000 -z 8 -d 20 \\
        -H 10.138.0.5:3000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import threading
import time
from typing import Awaitable, Callable, List, Tuple

try:
    import aerospike
    from aerospike import predicates
except ImportError:  # the legacy client ships no free-threaded wheels
    aerospike = None
from aerospike_native import (
    BatchReadOp,
    ClientPolicy,
    Filter,
    IndexType,
    Key,
    PartitionFilter,
    QueryPolicy,
    ReadPolicy,
    Statement,
    WritePolicy,
    new_client,
    new_client_blocking,
)
from aerospike_native.exceptions import IndexFoundError
from aerospike_sdk import ClusterDefinition, DataSet

INDEX_BIN = "n"
PAYLOAD_BIN = "p"
BINS = [INDEX_BIN, PAYLOAD_BIN]

ASYNC_MODES = ("pnc-async", "psdk-async", "pnc-batch-stream", "psdk-batch-stream")
THREAD_MODES = ("pnc-blocking", "legacy-sync")

ALL_PARTITIONS = 4096
PARTITION_MODES = ("pnc-async", "pnc-blocking")

QueryFn = Callable[[int, int], Awaitable[int]]
CloseFn = Callable[[], Awaitable[None]]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-H", "--host", default="127.0.0.1:3000", help="seed host:port")
    p.add_argument("-n", "--namespace", default="test")
    p.add_argument("-s", "--set", dest="set_name", default="qbench")
    p.add_argument("--mode", choices=ASYNC_MODES + THREAD_MODES, default="pnc-async")
    p.add_argument("--rows", type=int, default=100,
                   help="records matched per query (keys per batch in batch-stream modes)")
    p.add_argument("-z", "--concurrency", type=int, default=1,
                   help="async tasks (async modes) or OS threads (others)")
    p.add_argument("--point-readers", type=int, default=0, metavar="N",
                   help="point-read tasks sharing the client and loop (pnc-async, psdk-async)")
    p.add_argument("-d", "--duration", type=float, default=20.0, help="measured seconds")
    p.add_argument("--warmup", type=float, default=3.0, help="unmeasured seconds first")
    p.add_argument("--records", type=int, default=200_000,
                   help="records in the dataset (query ranges are drawn from it)")
    p.add_argument("--partitions", type=int, default=ALL_PARTITIONS, metavar="N",
                   help="query only partitions 0..N-1 (pnc-async, pnc-blocking); each query "
                        "still matches exactly --rows records")
    p.add_argument("--load", type=int, default=0, metavar="N",
                   help="truncate, write N records, create the index, then exit")
    p.add_argument("--payload", type=int, default=256, help="payload bytes per record (load)")
    args = p.parse_args()
    if args.point_readers and args.mode not in ("pnc-async", "psdk-async"):
        p.error("--point-readers requires --mode pnc-async or psdk-async")
    if not 1 <= args.partitions <= ALL_PARTITIONS:
        p.error(f"--partitions must be between 1 and {ALL_PARTITIONS}")
    if args.partitions != ALL_PARTITIONS and args.mode not in PARTITION_MODES:
        p.error("--partitions requires --mode pnc-async or pnc-blocking")
    if args.mode == "legacy-sync" and aerospike is None:
        p.error("legacy-sync needs the legacy aerospike package")
    return args


def index_name(args: argparse.Namespace) -> str:
    return f"{args.set_name}_{INDEX_BIN}_idx"


async def load(args: argparse.Namespace) -> None:
    client = await new_client(ClientPolicy(), args.host)
    try:
        await client.truncate(args.namespace, args.set_name)
        payload = os.urandom(args.payload)
        wp = WritePolicy()
        sem = asyncio.Semaphore(256)

        async def put(i: int) -> None:
            async with sem:
                key = Key(args.namespace, args.set_name, i)
                await client.put(key, {INDEX_BIN: i, PAYLOAD_BIN: payload}, policy=wp)

        t0 = time.perf_counter()
        await asyncio.gather(*(put(i) for i in range(args.load)))
        print(f"loaded {args.load} records in {time.perf_counter() - t0:.1f}s", file=sys.stderr)
        try:
            task = await client.create_index(
                args.namespace, args.set_name, INDEX_BIN, index_name(args), IndexType.NUMERIC,
            )
            await task.wait_till_complete()
        except IndexFoundError:
            pass
        print(f"index {index_name(args)} ready", file=sys.stderr)
    finally:
        await client.close()


class Recorder:
    """Per-worker counters; merged after the run so the hot loop takes no lock."""

    __slots__ = ("queries", "records", "bad", "lat_ns")

    def __init__(self) -> None:
        self.queries = 0
        self.records = 0
        self.bad = 0
        self.lat_ns: List[int] = []


def records_in_partitions(args: argparse.Namespace) -> List[int] | None:
    """Index values of the records that live in partitions ``0..N-1``, in order.

    ``None`` when every partition is queried. With fewer, a query range is
    drawn from this list so it still matches exactly ``--rows`` records and
    the wrong-count check keeps its meaning.
    """
    if args.partitions == ALL_PARTITIONS:
        return None
    members = [
        i for i in range(args.records)
        if Key(args.namespace, args.set_name, i).partition_id < args.partitions
    ]
    if len(members) < args.rows:
        sys.exit(f"only {len(members)} records in {args.partitions} partitions; "
                 f"--rows {args.rows} cannot be matched")
    return members


def partition_filter_for(args: argparse.Namespace) -> Callable[[], PartitionFilter]:
    if args.partitions == ALL_PARTITIONS:
        return PartitionFilter.all
    return lambda: PartitionFilter.by_range(0, args.partitions)


def range_for(rng: random.Random, args: argparse.Namespace) -> tuple[int, int]:
    members = args.members
    if members is not None:
        first = rng.randrange(0, len(members) - args.rows + 1)
        return members[first], members[first + args.rows - 1]
    lo = rng.randrange(0, args.records - args.rows + 1)
    return lo, lo + args.rows - 1


async def pnc_async_query(args: argparse.Namespace) -> Tuple[QueryFn, CloseFn, object]:
    client = await new_client(ClientPolicy(), args.host)
    qp = QueryPolicy()
    partition_filter = partition_filter_for(args)

    async def one_query(lo: int, hi: int) -> int:
        stmt = Statement(args.namespace, args.set_name, BINS)
        stmt.filters = [Filter.range(INDEX_BIN, lo, hi)]
        rs = await client.query(stmt, partition_filter(), policy=qp)
        count = 0
        async for r in rs:
            r.bins[INDEX_BIN]
            count += 1
        return count

    return one_query, client.close, client


async def pnc_batch_stream_query(args: argparse.Namespace) -> Tuple[QueryFn, CloseFn, object]:
    client = await new_client(ClientPolicy(), args.host)

    async def one_query(lo: int, hi: int) -> int:
        ops = [BatchReadOp(Key(args.namespace, args.set_name, i), BINS) for i in range(lo, hi + 1)]
        stream = await client.batch_stream(ops)
        count = 0
        async for _idx, br in stream:
            br.record.bins[INDEX_BIN]
            count += 1
        return count

    return one_query, client.close, client


async def psdk_session(args: argparse.Namespace):
    host, port = args.host.rsplit(":", 1)
    cluster = await ClusterDefinition(host, int(port)).connect()
    return cluster, cluster.create_session(), DataSet.of(args.namespace, args.set_name)


async def psdk_async_query(args: argparse.Namespace) -> Tuple[QueryFn, CloseFn, object]:
    cluster, session, ds = await psdk_session(args)

    async def one_query(lo: int, hi: int) -> int:
        stream = await (
            session.query(ds).filter(Filter.range(INDEX_BIN, lo, hi)).bins(BINS).execute()
        )
        count = 0
        async for result in stream:
            # Checked per row, as SDK consumers do; a failed row is left
            # uncounted and shows up as a wrong count.
            if result.is_ok:
                result.record.bins[INDEX_BIN]
                count += 1
        return count

    # Point readers must share the session's PNC client: a stream's fairness
    # yield delivers finished operations for its own client only.
    return one_query, cluster.close, cluster._sdk_client.underlying_client


async def psdk_batch_stream_query(args: argparse.Namespace) -> Tuple[QueryFn, CloseFn, object]:
    cluster, session, ds = await psdk_session(args)

    async def one_query(lo: int, hi: int) -> int:
        count = 0
        async with session.query(ds.ids(list(range(lo, hi + 1)))).bins(BINS).stream() as stream:
            async for result in stream:
                if result.is_ok:
                    result.record.bins[INDEX_BIN]
                    count += 1
        return count

    return one_query, cluster.close, None


ASYNC_QUERIES = {
    "pnc-async": pnc_async_query,
    "psdk-async": psdk_async_query,
    "pnc-batch-stream": pnc_batch_stream_query,
    "psdk-batch-stream": psdk_batch_stream_query,
}


async def run_async(args: argparse.Namespace, measuring: Callable[[], bool],
                    stop: Callable[[], bool]) -> Tuple[List[Recorder], List[int]]:
    one_query, close, client = await ASYNC_QUERIES[args.mode](args)
    get_lat_ns: List[int] = []

    async def worker(wid: int) -> Recorder:
        rec = Recorder()
        rng = random.Random(wid)
        perf = time.perf_counter_ns
        while not stop():
            lo, hi = range_for(rng, args)
            t0 = perf()
            count = await one_query(lo, hi)
            dt = perf() - t0
            if measuring():
                rec.queries += 1
                rec.records += count
                rec.bad += count != args.rows
                rec.lat_ns.append(dt)
        return rec

    async def point_reader(rid: int) -> None:
        rng = random.Random(10_000 + rid)
        rp = ReadPolicy()
        perf = time.perf_counter_ns
        while not stop():
            key = Key(args.namespace, args.set_name, rng.randrange(args.records))
            t0 = perf()
            await client.get(key, [INDEX_BIN], policy=rp)
            dt = perf() - t0
            if measuring():
                get_lat_ns.append(dt)

    try:
        readers = [point_reader(i) for i in range(args.point_readers)]
        recs = await asyncio.gather(*(worker(i) for i in range(args.concurrency)), *readers)
        return list(recs[:args.concurrency]), get_lat_ns
    finally:
        await close()


def run_threads(args: argparse.Namespace, one_query: Callable[[int, int], int],
                measuring: Callable[[], bool], stop: Callable[[], bool]) -> List[Recorder]:
    recs = [Recorder() for _ in range(args.concurrency)]

    def worker(wid: int) -> None:
        rec = recs[wid]
        rng = random.Random(wid)
        perf = time.perf_counter_ns
        while not stop():
            lo, hi = range_for(rng, args)
            t0 = perf()
            count = one_query(lo, hi)
            dt = perf() - t0
            if measuring():
                rec.queries += 1
                rec.records += count
                rec.bad += count != args.rows
                rec.lat_ns.append(dt)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return recs


def pnc_blocking_query(args: argparse.Namespace) -> Callable[[int, int], int]:
    client = new_client_blocking(ClientPolicy(), args.host)
    qp = QueryPolicy()
    partition_filter = partition_filter_for(args)

    def one_query(lo: int, hi: int) -> int:
        stmt = Statement(args.namespace, args.set_name, BINS)
        stmt.filters = [Filter.range(INDEX_BIN, lo, hi)]
        count = 0
        for r in client.query_blocking(stmt, partition_filter(), policy=qp):
            r.bins[INDEX_BIN]
            count += 1
        return count

    return one_query


def legacy_query(args: argparse.Namespace) -> Callable[[int, int], int]:
    host, port = args.host.rsplit(":", 1)
    client = aerospike.client({"hosts": [(host, int(port))]}).connect()

    def one_query(lo: int, hi: int) -> int:
        q = client.query(args.namespace, args.set_name)
        q.select(*BINS)
        q.where(predicates.between(INDEX_BIN, lo, hi))
        count = 0
        for _key, _meta, bins in q.results():
            bins[INDEX_BIN]
            count += 1
        return count

    return one_query


def percentile(sorted_ns: List[int], pct: float) -> float:
    if not sorted_ns:
        return float("nan")
    idx = min(len(sorted_ns) - 1, int(round(pct / 100.0 * (len(sorted_ns) - 1))))
    return sorted_ns[idx] / 1e6


def main() -> None:
    args = parse_args()
    if args.load:
        asyncio.run(load(args))
        return
    args.members = records_in_partitions(args)

    window = {"measuring": False, "stop": False, "cpu0": 0.0, "t0": 0.0, "cpu1": 0.0, "t1": 0.0}

    def timer() -> None:
        time.sleep(args.warmup)
        window["cpu0"], window["t0"] = time.process_time(), time.perf_counter()
        window["measuring"] = True
        time.sleep(args.duration)
        window["measuring"] = False
        window["cpu1"], window["t1"] = time.process_time(), time.perf_counter()
        window["stop"] = True

    measuring = lambda: window["measuring"]  # noqa: E731
    stop = lambda: window["stop"]  # noqa: E731

    get_lat: List[int] = []
    if args.mode in ASYNC_MODES:
        async def go() -> Tuple[List[Recorder], List[int]]:
            threading.Thread(target=timer, daemon=True).start()
            return await run_async(args, measuring, stop)
        recs, get_lat = asyncio.run(go())
    else:
        one_query = pnc_blocking_query(args) if args.mode == "pnc-blocking" else legacy_query(args)
        threading.Thread(target=timer, daemon=True).start()
        recs = run_threads(args, one_query, measuring, stop)

    wall = window["t1"] - window["t0"]
    cpu = window["cpu1"] - window["cpu0"]
    queries = sum(r.queries for r in recs)
    records = sum(r.records for r in recs)
    lat = sorted(x for r in recs for x in r.lat_ns)
    get_lat.sort()
    point_reads = {
        "point_readers": args.point_readers,
        "gets_per_s": round(len(get_lat) / wall, 1),
        "get_p50_ms": round(percentile(get_lat, 50), 3),
        "get_p99_ms": round(percentile(get_lat, 99), 3),
    } if args.point_readers else {}
    print(json.dumps({
        "mode": args.mode,
        "rows": args.rows,
        "partitions": args.partitions,
        "concurrency": args.concurrency,
        "queries": queries,
        "records": records,
        "wrong_count": sum(r.bad for r in recs),
        "qps": round(queries / wall, 1),
        "records_per_s": round(records / wall, 0),
        "p50_ms": round(percentile(lat, 50), 3),
        "p99_ms": round(percentile(lat, 99), 3),
        "cpu_cores": round(cpu / wall, 2),
        "cpu_us_per_record": round(cpu / records * 1e6, 2) if records else None,
        "cpu_us_per_query": round(cpu / queries * 1e6, 1) if queries else None,
        **point_reads,
        "python": sys.version.split()[0],
        "gil": sys._is_gil_enabled(),
    }))


if __name__ == "__main__":
    main()
