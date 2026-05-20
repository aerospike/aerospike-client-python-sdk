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

"""CLI configuration and workload parsing for the benchmark tool."""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ._env import default_host
from .record_spec import BinField, parse_bin_spec


class WorkloadKind(str, Enum):
    """Supported benchmark workloads."""

    INSERT = "I"
    READ_UPDATE = "RU"
    READ_REPLACE = "RR"
    READ_MODIFY_UPDATE = "RMU"
    READ_MODIFY_INCREMENT = "RMI"
    READ_MODIFY_DECREMENT = "RMD"


@dataclass
class WorkloadConfig:
    """Resolved benchmark settings after CLI parsing."""

    seeds: str
    namespace: str
    set_name: str
    key_count: int
    bin_fields: list[BinField]
    workload: WorkloadKind
    read_percent: int
    """For RU/RR: percent of operations that are reads (0..100)."""
    read_all_bins_percent: int
    """For RU: among reads, percent that read all bins (rest read one bin)."""
    write_all_bins_percent: int
    """For RU: among writes, percent that write all bins (rest write one bin)."""
    async_tasks: int
    """Number of concurrent async tasks (``-z``). Used in async mode."""
    threads: int
    """Number of OS threads (``--threads``). Used in sync mode."""
    duration_sec: float
    max_ops: Optional[int]
    batch_size: int
    latency_columns: int
    latency_shift: int
    mode: str
    warmup_intervals: int
    cooldown_intervals: int
    seed: int
    truncate_before_run: bool = False
    truncate_after_run: bool = False
    # TLS / authentication
    tls_ca_file: Optional[str] = None
    tls_cert_file: Optional[str] = None
    tls_key_file: Optional[str] = None
    auth_mode: Optional[str] = None
    auth_user: Optional[str] = None
    auth_password: Optional[str] = None
    services_alternate: bool = False
    latency_style: str = "columns"
    # Python allocation tracing. Off by default — `tracemalloc.start()` hooks
    # every PyObject alloc/free and walks the Python frame stack each time,
    # which historically consumed ~40% of the GIL thread on RU,50 profiles.
    # Enable only for memory investigations, not for TPS measurement.
    tracemalloc_enabled: bool = False
    fast_path: bool = False
    """Use Session.get/put shortcuts (bypass QueryBuilder / RecordStream) for
    single-key RU reads and full-bin writes. Prototype path for benchmarking
    the upper bound on PSDK single-key throughput."""
    lat_sample_every: int = 100
    """Latency sampling rate. Workers time every Nth op (default: 1-in-100)
    and feed the sampled latency to the histogram + summary percentiles.
    TPS / read-write / timeout / error counters are still incremented every
    op (cheap). Full-sample timing would add per-op overhead that compresses
    TPS by 2-4× on fast clients."""
    pool_loops: int = 0
    """When >0, use AsyncPool with this many event loops instead of a single
    Client.  Each loop gets ``async_tasks`` concurrent workers, so total
    concurrency = pool_loops × async_tasks."""
    with_telemetry: bool = False
    """When True, enable per-second TPS ticker, sampled latency histograms,
    and summary percentiles. Default is the lean path that runs straight to
    ``--duration`` and prints only a final TPS / errors / timeouts summary."""


def parse_latency_arg(value: str) -> tuple[int, int, str]:
    """Parse ``COLUMNS,SHIFT`` for latency histogram layout.

    Also accepts the bare token ``ycsb``, which selects the YCSB-style
    per-second latency output. Returns ``(columns, shift, style)`` where
    ``style`` is ``"columns"`` for the histogram layout and ``"ycsb"`` for
    the YCSB-style output.
    """
    v = value.strip().lower()
    if v == "ycsb":
        return 7, 1, "ycsb"
    parts = value.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            "expected COLUMNS,SHIFT (e.g. 7,1) or 'ycsb'")
    try:
        cols = int(parts[0].strip())
        shift = int(parts[1].strip())
    except ValueError as e:
        raise argparse.ArgumentTypeError("COLUMNS and SHIFT must be integers") from e
    if cols < 2:
        raise argparse.ArgumentTypeError("COLUMNS must be at least 2")
    if shift < 0:
        raise argparse.ArgumentTypeError("SHIFT must be non-negative")
    return cols, shift, "columns"


def parse_workload_arg(raw: str) -> tuple[WorkloadKind, int, int, int]:
    s = raw.strip().upper()
    if s == "I":
        return WorkloadKind.INSERT, 0, 100, 100
    if s == "RMU":
        return WorkloadKind.READ_MODIFY_UPDATE, 50, 100, 100
    if s == "RMI":
        return WorkloadKind.READ_MODIFY_INCREMENT, 50, 100, 100
    if s == "RMD":
        return WorkloadKind.READ_MODIFY_DECREMENT, 50, 100, 100
    if s.startswith("RU"):
        rest = s[2:].lstrip(",")
        if not rest:
            raise argparse.ArgumentTypeError("RU requires a read percent, e.g. RU,50")
        parts = [p.strip() for p in rest.split(",") if p.strip()]
        if len(parts) not in (1, 3):
            raise argparse.ArgumentTypeError(
                "use RU,read_pct or RU,read_pct,read_all_bins_pct,write_all_bins_pct"
            )
        try:
            nums = [int(p) for p in parts]
        except ValueError as e:
            raise argparse.ArgumentTypeError("RU workload percents must be integers") from e
        if len(nums) == 1:
            r, ra, wa = nums[0], 100, 100
        else:
            r, ra, wa = nums[0], nums[1], nums[2]
        if not 0 <= r <= 100 or not 0 <= ra <= 100 or not 0 <= wa <= 100:
            raise argparse.ArgumentTypeError("RU percents must be between 0 and 100")
        return WorkloadKind.READ_UPDATE, r, ra, wa
    if s.startswith("RR"):
        rest = s[2:].lstrip(",")
        if not rest:
            raise argparse.ArgumentTypeError("RR requires a read percent, e.g. RR,20")
        try:
            r = int(rest.split(",")[0].strip())
        except ValueError as e:
            raise argparse.ArgumentTypeError("RR read percent must be an integer") from e
        if not 0 <= r <= 100:
            raise argparse.ArgumentTypeError("RR read percent must be between 0 and 100")
        return WorkloadKind.READ_REPLACE, r, 100, 100
    raise argparse.ArgumentTypeError(f"unknown workload {raw!r}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Aerospike Python SDK benchmark driver (async or sync).",
    )
    p.add_argument(
        "-H",
        "--hosts",
        default=default_host(),
        help="Cluster seed addresses (default: %(default)s from aerospike.env).",
    )
    p.add_argument("-n", "--namespace", default="test", help="Namespace (default: %(default)s).")
    p.add_argument("-s", "--set", dest="set_name", default="testset", help="Set name.")
    p.add_argument(
        "-k",
        "--keys",
        type=int,
        default=100_000,
        help="Key space size for random workloads (default: %(default)s).",
    )
    p.add_argument(
        "-o",
        "--bins",
        default="I1",
        help="Bin spec, e.g. I1 or I1,S128,B1024 (default: %(default)s).",
    )
    p.add_argument(
        "-w",
        "--workload",
        default="RU,50",
        type=str,
        help="Workload: I, RU,50, RU,80,60,30, RR,20, RMU, RMI, RMD.",
    )
    p.add_argument(
        "-z",
        "--async-tasks",
        type=int,
        default=32,
        dest="async_tasks",
        help="Number of concurrent async tasks (default: %(default)s). "
        "Used in async mode; overridden by --threads in sync mode.",
    )
    p.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of OS threads for sync mode. "
        "If not set, falls back to -z value.",
    )
    p.add_argument(
        "-d",
        "--duration",
        type=float,
        default=10.0,
        help="Run duration in seconds (default: %(default)s).",
    )
    p.add_argument(
        "-c",
        "--max-ops",
        type=int,
        default=None,
        help="Stop after this many successful operations (optional).",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="If >1, each operation touches this many keys in one batch command.",
    )
    p.add_argument(
        "--latency",
        default="7,1",
        type=parse_latency_arg,
        metavar="COLUMNS,SHIFT",
        help="Histogram column count and shift (default: %(default)s).",
    )
    p.add_argument(
        "--mode",
        choices=("async", "sync", "pac-blocking", "pac-async", "legacy-sync"),
        default="async",
        help="Client API style. 'async' / 'sync' use PSDK sessions. "
        "'pac-blocking' calls PAC's `_blocking` entries directly. "
        "'pac-async' uses PAC's async client directly, bypassing PSDK. "
        "'legacy-sync' uses the legacy `aerospike` C client. "
        "(default: %(default)s)",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=4,
        metavar="N",
        help="Full-second intervals excluded from summary at the start.",
    )
    p.add_argument(
        "--cooldown",
        type=int,
        default=4,
        metavar="N",
        help="Full-second intervals excluded from summary at the end.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=0,
        help="RNG seed; 0 picks a random seed.",
    )
    p.add_argument(
        "--truncate",
        action="store_true",
        default=False,
        help="Truncate the set before running (removes stale data from prior runs).",
    )
    p.add_argument(
        "--truncate-after",
        action="store_true",
        default=False,
        help="Truncate the set after running (frees server memory).",
    )
    # TLS options
    p.add_argument(
        "--tls-ca-file",
        default=None,
        metavar="PATH",
        help="Path to CA certificate file for TLS.",
    )
    p.add_argument(
        "--tls-cert-file",
        default=None,
        metavar="PATH",
        help="Path to client certificate file for mutual TLS.",
    )
    p.add_argument(
        "--tls-key-file",
        default=None,
        metavar="PATH",
        help="Path to client private key file for mutual TLS.",
    )
    # Authentication options
    p.add_argument(
        "-U",
        "--user",
        default=None,
        help="User name for authentication.",
    )
    p.add_argument(
        "-P",
        "--password",
        default=None,
        help="Password for authentication.",
    )
    p.add_argument(
        "--auth-mode",
        choices=("INTERNAL", "EXTERNAL", "PKI"),
        default=None,
        help="Authentication mode (default: INTERNAL when user is set).",
    )
    p.add_argument(
        "--latency-style",
        choices=("columns", "ycsb"),
        default="columns",
        help="Latency output style: 'columns' (histogram buckets) or "
        "'ycsb' (avg/min/max/percentile per op type) (default: %(default)s).",
    )
    p.add_argument(
        "--services-alternate",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use services-alternate for cluster discovery (default: False).",
    )
    p.add_argument(
        "--tracemalloc",
        dest="tracemalloc",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable Python allocation tracing (tracemalloc). Off by default "
        "because it consumes ~40%% of the GIL thread on hot paths; enable only "
        "for memory-leak investigations.",
    )
    p.add_argument(
        "--fast-path",
        dest="fast_path",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use Session.get/put shortcut methods for single-key RU reads "
        "and full-bin writes (bypasses QueryBuilder/RecordStream). "
        "Experimental upper-bound bench mode.",
    )
    p.add_argument(
        "--pool-loops",
        type=int,
        default=0,
        metavar="N",
        help="Use AsyncPool with N event loops instead of a single Client. "
        "Each loop gets -z concurrent tasks (total = N × z). "
        "0 (default) uses the single-Client async path.",
    )
    p.add_argument(
        "--lat-sample-every",
        type=int,
        default=100,
        metavar="N",
        help="Latency sampling rate: time every Nth op (default: %(default)s). "
        "TPS / error / timeout counters are still updated every op. "
        "Sampling preserves percentile accuracy via uniform random sampling "
        "and avoids the per-op timing overhead that compresses TPS on fast "
        "clients. Set to 1 for full-sample timing (slower, only useful for "
        "p99.99+ tail analysis). Only applies when --with-telemetry is set.",
    )
    p.add_argument(
        "--with-telemetry",
        action="store_true",
        default=False,
        help="Enable per-second TPS ticker output, sampled latency "
        "histograms, and the warmup / cooldown windowing. Off by default; "
        "the lean path runs straight to --duration and prints only a final "
        "TPS / errors / timeouts summary.",
    )
    return p


def config_from_args(ns: argparse.Namespace) -> WorkloadConfig:
    cols, shift, lat_style_from_latency = ns.latency
    # Explicit --latency-style wins over the bare-`ycsb` form of --latency.
    if getattr(ns, "latency_style", "columns") == "columns" and lat_style_from_latency != "columns":
        ns.latency_style = lat_style_from_latency
    fields = parse_bin_spec(ns.bins)
    kind, rp, rap, wap = parse_workload_arg(ns.workload)
    seed = int(ns.seed)
    if seed == 0:
        seed = random.randint(1, 2**31 - 1)
    auth_mode = getattr(ns, "auth_mode", None)
    auth_user = getattr(ns, "user", None)
    auth_password = getattr(ns, "password", None)
    if auth_user and not auth_mode:
        auth_mode = "INTERNAL"
    return WorkloadConfig(
        seeds=ns.hosts,
        namespace=ns.namespace,
        set_name=ns.set_name,
        key_count=max(1, ns.keys),
        bin_fields=fields,
        workload=kind,
        read_percent=rp,
        read_all_bins_percent=rap,
        write_all_bins_percent=wap,
        async_tasks=max(1, ns.async_tasks),
        threads=max(1, ns.threads if ns.threads is not None else ns.async_tasks),
        duration_sec=float(ns.duration),
        max_ops=ns.max_ops,
        batch_size=max(0, int(ns.batch_size)),
        latency_columns=cols,
        latency_shift=shift,
        mode=ns.mode,
        warmup_intervals=max(0, ns.warmup),
        cooldown_intervals=max(0, ns.cooldown),
        seed=seed,
        truncate_before_run=getattr(ns, "truncate", False),
        truncate_after_run=getattr(ns, "truncate_after", False),
        tls_ca_file=getattr(ns, "tls_ca_file", None),
        tls_cert_file=getattr(ns, "tls_cert_file", None),
        tls_key_file=getattr(ns, "tls_key_file", None),
        auth_mode=auth_mode,
        auth_user=auth_user,
        auth_password=auth_password,
        services_alternate=getattr(ns, "services_alternate", False),
        latency_style=getattr(ns, "latency_style", "columns"),
        tracemalloc_enabled=bool(getattr(ns, "tracemalloc", False)),
        fast_path=bool(getattr(ns, "fast_path", False)),
        pool_loops=max(0, int(getattr(ns, "pool_loops", 0))),
        lat_sample_every=max(1, int(getattr(ns, "lat_sample_every", 100))),
        # --latency-style ycsb only makes sense with per-second output, so
        # treat it as implicit --with-telemetry.
        with_telemetry=(
            bool(getattr(ns, "with_telemetry", False))
            or getattr(ns, "latency_style", "columns") != "columns"
        ),
    )
