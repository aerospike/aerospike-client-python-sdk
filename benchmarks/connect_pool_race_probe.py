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

"""Probe: does a high ``conn_pools_per_node`` make the first op beat cluster tend?

Background
----------
The sync entry path leaves ``conn_pools_per_node`` at the underlying default of
4. Raising it to 8 is attractive for many-threaded sync workloads (less per-node
pool mutex contention), but on a **local** 3-node strong-consistency dev cluster
every attempt at >= 8 failed immediately with ``Invalid namespace: Partition map
empty`` — while <= 6 passed, and 8 with a short settle delay passed. That shape
says ``connect()`` is returning before the partition map is populated, with the
extra connection-establishment work at higher pool counts letting the first
operation get there first.

That has never been reproduced on real hardware. This probe answers whether it
happens on a production-class cluster, which decides whether the tuning can be
adopted or whether ``connect()`` needs a readiness gate first.

Because this is a *race*, a single trial proves little — each (pools, delay)
cell is run ``--trials`` times and reported as a failure rate.

Usage::

    # Both namespaces, default sweep
    python -m benchmarks.connect_pool_race_probe \\
        --host <seed>:3000 --namespace test --sc-namespace test_sc

    # AP only, wider sweep, more trials
    python -m benchmarks.connect_pool_race_probe \\
        --host <seed>:3000 --namespace test \\
        --pools 4,6,8,16,32 --trials 20

Authentication mirrors the benchmark harness flags (``-U`` / ``-P``).
"""

from __future__ import annotations

import argparse
import time
from typing import List, Optional

from aerospike_sdk import DataSet
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sync import ClusterDefinition, Host

SET_NAME = "pool_race_probe"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--host", required=True, help="Seed address, e.g. node:3000.")
    p.add_argument("--namespace", default="test", help="AP namespace (default: test).")
    p.add_argument(
        "--sc-namespace",
        default=None,
        help="Strong-consistency namespace. Omit to skip the SC arm.",
    )
    p.add_argument(
        "--pools",
        default="4,6,8,16",
        help="Comma-separated conn_pools_per_node values (default: 4,6,8,16).",
    )
    p.add_argument(
        "--delays",
        default="0,1.5",
        help="Comma-separated settle delays in seconds between connect() and the "
             "first op (default: 0,1.5). A failure at 0 that clears at 1.5 "
             "identifies a startup race rather than a capacity limit.",
    )
    p.add_argument("--trials", type=int, default=10,
                   help="Repeats per cell; it's a race, so rate matters (default: 10).")
    p.add_argument("-U", "--user", default=None)
    p.add_argument("-P", "--password", default=None)
    return p.parse_args()


def _definition(args: argparse.Namespace, pools: int) -> ClusterDefinition:
    cd = ClusterDefinition(hosts=Host.parse_hosts(args.host, 3000))
    if args.user:
        cd.with_native_credentials(args.user, args.password or "")
    cd.with_system_settings(SystemSettings(conn_pools_per_node=pools))
    return cd


def _one_trial(args: argparse.Namespace, namespace: str, pools: int, delay: float):
    """Connect, optionally settle, then issue one write. Returns None or an error."""
    try:
        cluster = _definition(args, pools).connect()
    except Exception as e:  # connect itself failed — distinct from the race
        return f"connect: {type(e).__name__}: {str(e)[:70]}"
    try:
        with cluster:
            session = cluster.create_session()
            if delay:
                time.sleep(delay)
            key = DataSet.of(namespace, SET_NAME).id("probe")
            session.upsert(key).put({"v": 1}).execute()
            session.delete(key).execute()
        return None
    except Exception as e:
        return f"{type(e).__name__}: {str(e)[:70]}"


def _describe_cluster(args: argparse.Namespace) -> None:
    """Node count matters: total new connections scale as pools × nodes."""
    try:
        with _definition(args, 4).connect() as cluster:
            session = cluster.create_session()
            stats = session.info("statistics")
            raw = list(stats.values())[0] if isinstance(stats, dict) else stats
            kv = dict(p.split("=", 1) for p in str(raw).split(";") if "=" in p)
            print(f"cluster_size={kv.get('cluster_size')} "
                  f"client_connections={kv.get('client_connections')} "
                  f"uptime={kv.get('uptime')}s")
            print(f"namespaces={list(session.info().namespaces())}")
    except Exception as e:
        print(f"(cluster probe failed: {type(e).__name__}: {str(e)[:70]})")


def main() -> None:
    args = parse_args()
    pools: List[int] = [int(s) for s in args.pools.split(",") if s.strip()]
    delays: List[float] = [float(s) for s in args.delays.split(",") if s.strip()]

    print(f"host={args.host}  trials/cell={args.trials}")
    _describe_cluster(args)
    print()

    arms = [("AP", args.namespace)]
    if args.sc_namespace:
        arms.append(("SC", args.sc_namespace))

    for label, namespace in arms:
        print(f"--- {label} namespace: {namespace} ---")
        print(f"{'pools':>6}  {'delay_s':>8}  {'failures':>10}  first_error")
        for pool_count in pools:
            for delay in delays:
                errors: List[Optional[str]] = [
                    _one_trial(args, namespace, pool_count, delay)
                    for _ in range(args.trials)
                ]
                failed = [e for e in errors if e is not None]
                first = failed[0] if failed else ""
                print(f"{pool_count:>6}  {delay:>8.1f}  "
                      f"{len(failed):>4}/{args.trials:<5}  {first}")
        print()


if __name__ == "__main__":
    main()
