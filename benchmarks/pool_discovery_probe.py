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

"""Diagnose a client that discovers fewer cluster nodes than the cluster has.

A client holding a partial node set still reports connected (``is_connected``
is satisfied by a single node), but its partition map only covers the
partitions that node owns. Single-key reads then fail for every other
partition with "Cannot get appropriate node for partition N", while batch
reads keep working because they reach the one known node and the server
proxies the rest.

The probe builds clients three ways in ONE process — so environment, seeds,
and policy are provably identical across them — and reports what each one
discovered:

``direct``
    One client connected on the caller's own event loop. The shape a
    single-loop bench uses.
``threaded-seq``
    ``loops`` clients, each on its own thread and event loop, connected
    strictly ONE AT A TIME.
``pool``
    A real :class:`~aerospike_sdk.AsyncPool`: the same ``loops`` clients on
    the same threads, but connected CONCURRENTLY.

Reading the result:

* ``direct`` full, ``threaded-seq`` full, ``pool`` partial → concurrent
  connect is the trigger; the seed pass / peer harvest is losing peers when
  several clients discover at once.
* ``direct`` full, ``threaded-seq`` partial → nothing to do with concurrency;
  the off-main-thread event loop or the per-client runtime is the trigger.
* all three partial → not a client-construction issue at all; the cluster is
  advertising an empty or unreachable peer list for the configured
  ``use_services_alternate`` mode (compare the per-node ``peers-clear-std``
  and ``peers-clear-alt`` rows the probe prints).

Example::

    python -m benchmarks.pool_discovery_probe -H 10.0.0.5:3000,10.0.0.6:3000 --loops 4

See Also:
    ``benchmarks/README.md``: the benchmark harness these flags mirror.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import os
import sys
import threading
from typing import Any, Dict, List, Optional, Sequence

from aerospike_async.exceptions import RecordNotFound

from aerospike_sdk import AsyncPool, ClusterDefinition, Host
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior

from benchmarks._env import client_policy_from_config, default_host, ensure_env

# Info commands worth seeing per node. The peers variants are the payload the
# client's own discovery reads: an empty response is what collapses a seed
# pass to a single fallback node.
_NODE_INFO_CMDS = (
    "node",
    "cluster-name",
    "peers-generation",
    "partition-generation",
    "peers-clear-std",
    "peers-clear-alt",
    "services-clear-std",
    "services-clear-alt",
)

_ROUTING_MARKERS = ("cannot get appropriate node", "partition map empty")


class _Cfg:
    """Minimal stand-in for the bench ``WorkloadConfig`` fields the policy needs."""

    def __init__(self, seeds: str) -> None:
        self.seeds = seeds
        self.services_alternate: Optional[bool] = None
        self.seed_only_cluster = False
        self.conn_pools_per_node = 0
        self.tls_ca_file = None
        self.tls_cert_file = None
        self.tls_key_file = None
        self.auth_mode = None
        self.auth_user = None
        self.auth_password = None


class _LoopThread:
    """One daemon thread running a dedicated event loop, mirroring AsyncPool's."""

    def __init__(self, index: int) -> None:
        self._ready = threading.Event()
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread = threading.Thread(
            target=self._run, name=f"probe-{index}", daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    def submit(self, coro: Any) -> "concurrent.futures.Future[Any]":
        assert self.loop is not None
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self) -> None:
        if self.loop is not None and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5.0)


def _is_routing_error(exc: BaseException) -> bool:
    """True when *exc* means the client had no node for the key's partition."""
    text = str(exc).lower()
    return any(marker in text for marker in _ROUTING_MARKERS)


def _node_attr(node: Any, attr: str) -> Any:
    """Read *attr* off a PAC ``Node`` whether it is exposed as a getter or a value."""
    try:
        value = getattr(node, attr)
        return value() if callable(value) else value
    except Exception as exc:  # a node can go inactive mid-probe
        return f"<{type(exc).__name__}: {exc}>"


async def _node_rows(pac: Any) -> List[Dict[str, Any]]:
    """Per-node identity and generation state from a connected PAC client."""
    rows: List[Dict[str, Any]] = []
    for node in await pac.nodes():
        rows.append({
            attr: _node_attr(node, attr)
            for attr in ("name", "host", "partition_generation", "failures", "is_active")
        })
    return rows


async def _node_info(pac: Any) -> Dict[str, Dict[str, str]]:
    """Raw discovery-relevant info responses, per node."""
    out: Dict[str, Dict[str, str]] = {}
    for node in await pac.nodes():
        answers: Dict[str, str] = {}
        for cmd in _NODE_INFO_CMDS:
            try:
                resp = await node.info(cmd)
                answers[cmd] = resp.get(cmd, "") if isinstance(resp, dict) else str(resp)
            except Exception as exc:
                answers[cmd] = f"<{type(exc).__name__}: {exc}>"
        out[str(_node_attr(node, "name"))] = answers
    return out


async def _routing_probe(cluster_or_client: Any, dataset: DataSet, keys: int) -> Dict[str, int]:
    """Read *keys* spread-out keys, classifying each outcome.

    The records need not exist: a missing record still required client-side
    routing to the owning node, so ``RecordNotFound`` proves the partition was
    routable. Only ``routing`` counts a partition the client could not place.
    """
    session = cluster_or_client.create_session(Behavior.DEFAULT)
    tally = {"ok": 0, "not_found": 0, "routing": 0, "other": 0}
    for i in range(keys):
        try:
            await session.get(dataset.id(f"probe-{i}"))
        except RecordNotFound:
            tally["not_found"] += 1
        except Exception as exc:
            tally["routing" if _is_routing_error(exc) else "other"] += 1
        else:
            tally["ok"] += 1
    return tally


async def _report(
    label: str,
    pac: Any,
    cluster_or_client: Any,
    dataset: DataSet,
    keys: int,
    show_info: bool,
) -> int:
    """Print one client's discovery state; return its node count."""
    rows = await _node_rows(pac)
    tally = await _routing_probe(cluster_or_client, dataset, keys)
    routable = keys - tally["routing"]
    pct = (100.0 * tally["routing"] / keys) if keys else 0.0
    print(f"  {label:22} nodes={len(rows)}  routable={routable}/{keys}  "
          f"routing_errors={tally['routing']} ({pct:.1f}%)  {tally}")
    for row in rows:
        print(f"      - {row['name']}  host={row['host']}  "
              f"part_gen={row['partition_generation']}  "
              f"failures={row['failures']}  active={row['is_active']}")
    if show_info:
        for name, answers in (await _node_info(pac)).items():
            print(f"      info[{name}]:")
            for cmd, value in answers.items():
                shown = value if len(value) <= 300 else value[:300] + "…"
                print(f"          {cmd:22} = {shown!r}")
    return len(rows)


def _print_policy_comparison(cfg: _Cfg, cluster_def: ClusterDefinition) -> None:
    """Diff the direct-path ClientPolicy against the ClusterDefinition one.

    A partial node set caused by configuration would show up here; identical
    policies rule configuration out and point at construction instead.
    """
    direct = client_policy_from_config(cfg)
    pooled = cluster_def._get_policy(None)
    names = sorted(
        n for n in set(dir(direct)) | set(dir(pooled))
        if not n.startswith("_") and not callable(getattr(direct, n, None))
    )

    def value(policy: Any, name: str) -> str:
        try:
            return repr(getattr(policy, name))
        except Exception as exc:
            return f"<{type(exc).__name__}: {exc}>"

    diffs = [(n, value(direct, n), value(pooled, n))
             for n in names if value(direct, n) != value(pooled, n)]
    print("ClientPolicy: direct path vs ClusterDefinition path")
    if not diffs:
        print("  identical on all readable fields")
    for name, a, b in diffs:
        print(f"  {name:30} direct={a:34} definition={b}")
    print(f"  use_services_alternate = {direct.use_services_alternate} "
          f"(env AEROSPIKE_USE_SERVICES_ALTERNATE="
          f"{os.environ.get('AEROSPIKE_USE_SERVICES_ALTERNATE')!r})")
    print()


async def _run_direct(cfg: _Cfg, dataset: DataSet, keys: int, show_info: bool) -> int:
    print("[direct] one client on the caller's event loop")
    policy = client_policy_from_config(cfg)
    async with Client(cfg.seeds, policy=policy) as client:
        return await _report(
            "direct", client.underlying_client, client, dataset, keys, show_info,
        )


async def _run_threaded_seq(
    cluster_def: ClusterDefinition,
    loops: int,
    dataset: DataSet,
    keys: int,
    show_info: bool,
) -> List[int]:
    print(f"[threaded-seq] {loops} clients on {loops} threads, connected one at a time")
    members: List[Client] = cluster_def._build_pool_members(loops)
    threads = [_LoopThread(i) for i in range(loops)]
    clusters: List[Optional[Cluster]] = [None] * loops
    counts: List[int] = []
    try:
        for i, (client, thread) in enumerate(zip(members, threads)):
            # Block on each connect before starting the next: this is the only
            # difference from the pool, which starts all N at once.
            cluster = thread.submit(Cluster._connect_and_wrap(client)).result()
            clusters[i] = cluster
            counts.append(
                thread.submit(
                    _report(f"threaded-seq[{i}]", client.underlying_client,
                            cluster, dataset, keys, show_info)
                ).result()
            )
    finally:
        for cluster, thread in zip(clusters, threads):
            if cluster is not None:
                try:
                    thread.submit(cluster.close()).result(timeout=10)
                except Exception as exc:
                    print(f"  (close failed: {exc})")
            thread.stop()
    return counts


async def _run_pool(
    cluster_def: ClusterDefinition,
    loops: int,
    per_client_runtime: Optional[bool],
    dataset: DataSet,
    keys: int,
    show_info: bool,
) -> List[int]:
    print(f"[pool] AsyncPool({loops} loops), all clients connected concurrently")
    pool = AsyncPool(cluster_def, loop_count=loops, per_client_runtime=per_client_runtime)
    counts: List[int] = []
    async with pool:
        print(f"  per_client_runtime={pool._per_client_runtime} "
              f"use_uvloop={pool._use_uvloop}")
        for i in range(loops):
            counts.append(
                await pool.run(
                    lambda cluster, i=i: _report(
                        f"pool[{i}]", cluster._client.underlying_client,
                        cluster, dataset, keys, show_info,
                    ),
                    pick=i,
                )
            )
    return counts


def _verdict(direct: Optional[int], seq: Sequence[int], pool: Sequence[int]) -> None:
    print("=" * 78)
    expected = max([direct or 0, *seq, *pool], default=0)
    print(f"Largest node set any client discovered: {expected}")

    def state(counts: Sequence[int]) -> str:
        if not counts:
            return "not run"
        if all(c == expected for c in counts):
            return f"full ({expected})"
        return f"PARTIAL ({sorted(counts)})"

    print(f"  direct       : {state([direct] if direct is not None else [])}")
    print(f"  threaded-seq : {state(seq)}")
    print(f"  pool         : {state(pool)}")

    seq_partial = bool(seq) and any(c < expected for c in seq)
    pool_partial = bool(pool) and any(c < expected for c in pool)
    direct_partial = direct is not None and direct < expected

    if direct_partial and seq_partial and pool_partial:
        print("\n  => Every construction shape is partial. Not a client-construction "
              "bug:\n     compare the per-node peers-clear-std vs peers-clear-alt rows "
              "above.\n     An empty peers response for the configured mode collapses the "
              "seed\n     pass onto a single fallback node.")
    elif pool_partial and not seq_partial:
        print("\n  => Only the concurrent connect is partial. The seed pass / peer "
              "harvest\n     loses peers when several clients discover at once — "
              "instrument the\n     core seed pass, not the SDK.")
    elif seq_partial:
        print("\n  => Off-main-thread construction alone is partial, without any "
              "concurrency.\n     Suspect the event-loop / runtime the construction "
              "future runs on.")
    else:
        print("\n  => No partial discovery reproduced in this run. Re-run under the "
              "load\n     that provokes it (the failure is connect-time, so it must be "
              "provoked\n     while the box is busy).")
    print("=" * 78)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the probe's CLI parser."""
    p = argparse.ArgumentParser(
        description="Compare cluster-node discovery across client construction shapes.",
    )
    p.add_argument("-H", "--hosts", default=default_host(),
                   help="Seed hosts (default: %(default)s from aerospike.env).")
    p.add_argument("-n", "--namespace", default=os.environ.get("AEROSPIKE_NAMESPACE", "test"),
                   help="Namespace for the routing probe (default: %(default)s).")
    p.add_argument("-s", "--set-name", default="probe",
                   help="Set name for the routing probe (default: %(default)s).")
    p.add_argument("--loops", type=int, default=4,
                   help="Clients for the threaded-seq and pool shapes (default: %(default)s).")
    p.add_argument("--keys", type=int, default=300,
                   help="Keys per routing probe; spread over partitions (default: %(default)s).")
    p.add_argument("--modes", default="direct,threaded-seq,pool",
                   help="Comma-separated shapes to run (default: %(default)s).")
    p.add_argument("--per-client-runtime", choices=("auto", "on", "off"), default="auto",
                   help="AsyncPool per-Client Tokio runtime (default: %(default)s).")
    p.add_argument("--node-info", action="store_true",
                   help="Also dump each node's peers-*/services-* info responses.")
    return p


async def _main(argv: Optional[List[str]] = None) -> int:
    ensure_env()
    args = build_arg_parser().parse_args(argv)
    modes = {m.strip() for m in args.modes.split(",") if m.strip()}

    cfg = _Cfg(args.hosts)
    dataset = DataSet.of(args.namespace, args.set_name)
    pcr = {"auto": None, "on": True, "off": False}[args.per_client_runtime]

    print(f"seeds={args.hosts!r}  parsed_hosts="
          f"{[(h.name, h.port) for h in Host.parse_hosts(args.hosts, 3000)]}")
    print(f"namespace={args.namespace!r} set={args.set_name!r} loops={args.loops} "
          f"keys={args.keys}")
    print(f"python={sys.version.split()[0]} gil_enabled="
          f"{getattr(sys, '_is_gil_enabled', lambda: True)()}")
    print()
    _print_policy_comparison(cfg, ClusterDefinition(hosts=Host.parse_hosts(args.hosts, 3000)))

    direct: Optional[int] = None
    seq: List[int] = []
    pool: List[int] = []

    if "direct" in modes:
        direct = await _run_direct(cfg, dataset, args.keys, args.node_info)
        print()
    if "threaded-seq" in modes:
        # A fresh definition per shape: each builds its own policy, so no shape
        # can inherit a mutation (e.g. per-Client runtime workers) from another.
        seq = await _run_threaded_seq(
            ClusterDefinition(hosts=Host.parse_hosts(args.hosts, 3000)),
            args.loops, dataset, args.keys, args.node_info,
        )
        print()
    if "pool" in modes:
        pool = await _run_pool(
            ClusterDefinition(hosts=Host.parse_hosts(args.hosts, 3000)),
            args.loops, pcr, dataset, args.keys, args.node_info,
        )
        print()

    _verdict(direct, seq, pool)
    return 0


def main() -> int:
    """Entry point for ``python -m benchmarks.pool_discovery_probe``."""
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
