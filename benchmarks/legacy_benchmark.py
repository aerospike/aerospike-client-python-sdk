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

"""Benchmark for the legacy Aerospike Python client (C extension).

Uses the same operations as the PSDK benchmark (get for reads, operate
with OPERATOR_WRITE for writes) so TPS and latency numbers are directly
comparable.  Output format matches the PSDK benchmark so the compare
tool can parse it with the same regex.

Usage::

    python legacy_benchmark.py -H 127.0.0.1:3100 -k 100000 -d 15 -z 32 -w RU,50
"""

from __future__ import annotations

import argparse
import math
import os
import random
import resource
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path


def _load_env() -> None:
    """Load aerospike.env from the PSDK or legacy repo for connection defaults."""
    for candidate in [
        Path(__file__).resolve().parent.parent / "aerospike.env",
        Path(__file__).resolve().parent.parent / "aerospike.env.example",
    ]:
        if candidate.exists():
            with open(candidate) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("export "):
                        line = line[7:]
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("\"'")
                        if k not in os.environ:
                            os.environ[k] = v
            break


_load_env()


def _default_host() -> str:
    return os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3000")


def _parse_host_port(hosts: str) -> tuple:
    if ":" in hosts:
        h, p = hosts.rsplit(":", 1)
        return h, int(p)
    return hosts, 3000


# ---------------------------------------------------------------------------
# Bin spec parsing (same format as PSDK: I1, S128, B1024)
# ---------------------------------------------------------------------------

_rng = random.Random()
import re
_TOKEN_RE = re.compile(r"^([ISB])(\d+)$", re.IGNORECASE)


def _parse_bin_spec(spec: str) -> list:
    """Parse into [(name, kind, size), ...]."""
    fields = []
    for i, tok in enumerate(spec.split(",")):
        m = _TOKEN_RE.match(tok.strip())
        if not m:
            raise ValueError(f"invalid bin token {tok!r}")
        ch, n = m.group(1).upper(), int(m.group(2))
        kind = {"I": "int", "S": "str", "B": "bytes"}[ch]
        fields.append((f"b{i}", kind, n))
    return fields


def _random_value(kind: str, size: int):
    if kind == "int":
        return _rng.randrange(1 << 30)
    if kind == "str":
        return _rng.randbytes(max(1, (size + 1) // 2)).hex()[:size]
    return bytearray(_rng.randbytes(size))


# ---------------------------------------------------------------------------
# Stats (same lightweight collector as PAC benchmark)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self, warmup: int, cooldown: int) -> None:
        self._lock = threading.Lock()
        self._reads = 0
        self._writes = 0
        self._errors = 0
        self._prev_reads = 0
        self._prev_writes = 0
        self._warmup = warmup
        self._cooldown = cooldown
        self._planned = 0
        self._current = 0
        self._latencies: list = []
        self._intervals: list = []

    def set_planned(self, n: int) -> None:
        self._planned = n

    def set_current(self, i: int) -> None:
        self._current = i

    def total_ops(self) -> int:
        return self._reads + self._writes

    def record(self, is_read: bool, latency_ms: float, is_error: bool) -> None:
        include = (
            self._planned > 0
            and self._warmup <= self._current < self._planned - self._cooldown
        )
        with self._lock:
            if is_read:
                self._reads += 1
            else:
                self._writes += 1
            if is_error:
                self._errors += 1
            if include and not is_error:
                self._latencies.append(latency_ms)

    def end_interval(self) -> tuple:
        with self._lock:
            dr = self._reads - self._prev_reads
            dw = self._writes - self._prev_writes
            self._prev_reads = self._reads
            self._prev_writes = self._writes
            self._intervals.append((dr, dw))
            return dr, dw, self._errors

    def summary(self) -> list:
        ivs = self._intervals
        n = len(ivs)
        lo, hi = self._warmup, n - self._cooldown
        mid = ivs[lo:hi] if hi > lo else ivs

        def avg(xs):
            return sum(xs) / len(xs) if xs else 0.0

        def median(xs):
            if not xs:
                return 0.0
            ys = sorted(xs)
            m = len(ys) // 2
            return float(ys[m]) if len(ys) % 2 else (ys[m - 1] + ys[m]) / 2.0

        r = [x[0] for x in mid]
        w = [x[1] for x in mid]
        t = [x[0] + x[1] for x in mid]

        lines = [
            f"Summary (excluding {self._warmup} warmup + {self._cooldown} cooldown intervals):",
            f"  Read  TPS: avg={avg(r):.0f}  median={median(r):.0f}",
            f"  Write TPS: avg={avg(w):.0f}  median={median(w):.0f}",
            f"  Total TPS: avg={avg(t):.0f}  median={median(t):.0f}",
        ]

        lat = sorted(self._latencies)
        if lat:
            def pct(p):
                k = max(1, int(math.ceil(p / 100.0 * len(lat))))
                return lat[k - 1]
            lines.append(
                f"  Latency p50={pct(50):.1f}ms  p90={pct(90):.1f}ms  "
                f"p99={pct(99):.1f}ms  p99.9={pct(99.9):.1f}ms  "
                f"max={lat[-1]:.1f}ms"
            )

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            rss_mb = rss / (1024 * 1024)
        else:
            rss_mb = rss / 1024.0
        lines.append(f"  Peak RSS: {rss_mb:.1f} MB")

        return lines


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def _worker(
    client,
    worker_id: int,
    namespace: str,
    set_name: str,
    key_count: int,
    fields: list,
    read_pct: int,
    workload: str,
    seed: int,
    max_ops,
    stats: _Stats,
    stop: threading.Event,
) -> None:
    # Deferred import: the legacy `aerospike` package is an optional dep,
    # only needed at runtime for `--mode legacy-sync`. Bench-test environments
    # install it; dev / IDE venvs typically don't. Suppression keeps IDEs
    # (PyCharm "Unresolved reference") quiet without forcing the install.
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    import aerospike  # type: ignore[import-not-found]

    rng = random.Random((seed + worker_id + 1) % (2**32))
    ns, sn = namespace, set_name

    while not stop.is_set():
        if max_ops is not None and stats.total_ops() >= max_ops:
            return

        kid = rng.randint(1, key_count)
        key = (ns, sn, kid)

        if workload == "I":
            is_read = False
        else:
            is_read = rng.randint(1, 100) <= read_pct

        t0 = time.perf_counter()
        try:
            if is_read:
                client.get(key)
            else:
                ops = [
                    {
                        "op": aerospike.OPERATOR_WRITE,
                        "bin": name,
                        "val": _random_value(kind, size),
                    }
                    for name, kind, size in fields
                ]
                client.operate(key, ops)
        except Exception:
            dt = (time.perf_counter() - t0) * 1000.0
            stats.record(is_read, dt, True)
        else:
            dt = (time.perf_counter() - t0) * 1000.0
            stats.record(is_read, dt, False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description="Legacy Aerospike Python client benchmark (C extension). "
        "Uses get/operate for apples-to-apples comparison with PSDK.",
    )
    p.add_argument("-H", "--hosts", default=_default_host(),
                   help="Cluster seed host:port (default: %(default)s).")
    p.add_argument("-n", "--namespace", default="test")
    p.add_argument("-s", "--set", dest="set_name", default="testset")
    p.add_argument("-k", "--keys", type=int, default=100_000)
    p.add_argument("-o", "--bins", default="I1", help="Bin spec (e.g. I1, I1,S128).")
    p.add_argument("-w", "--workload", default="RU,50",
                   help="Workload: I or RU,<read_pct>.")
    p.add_argument("-z", "--concurrency", type=int, default=1,
                   help="Number of threads (default: 1).")
    p.add_argument("-d", "--duration", type=float, default=10.0)
    p.add_argument("-c", "--max-ops", type=int, default=None)
    p.add_argument("--warmup", type=int, default=4)
    p.add_argument("--cooldown", type=int, default=4)
    p.add_argument("--seed", type=int, default=0)

    args = p.parse_args()

    # Parse workload
    wl = args.workload.strip().upper()
    if wl == "I":
        read_pct = 0
    elif wl.startswith("RU"):
        parts = wl.split(",")
        read_pct = int(parts[1]) if len(parts) > 1 else 50
    else:
        print(f"Unknown workload: {args.workload}", file=sys.stderr)
        return 2

    if args.seed == 0:
        args.seed = random.randint(1, 2**31 - 1)

    fields = _parse_bin_spec(args.bins)
    host, port = _parse_host_port(args.hosts)
    n_iv = max(1, math.ceil(args.duration))

    stats = _Stats(args.warmup, args.cooldown)
    stats.set_planned(n_iv)
    stop = threading.Event()

    # Connect
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    import aerospike  # type: ignore[import-not-found]
    use_alt = os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower() in ("true", "1", "yes")
    config = {"hosts": [(host, port)], "use_services_alternate": use_alt}
    client = aerospike.client(config).connect()
    print(f"Connected to {args.hosts}. Starting legacy benchmark ...")

    # Launch workers
    pool = ThreadPoolExecutor(max_workers=max(1, args.concurrency))
    futures = [
        pool.submit(
            _worker, client, i,
            args.namespace, args.set_name, args.keys,
            fields, read_pct, wl, args.seed, args.max_ops,
            stats, stop,
        )
        for i in range(max(1, args.concurrency))
    ]

    # Ticker
    from datetime import datetime
    for iv in range(n_iv):
        time.sleep(1.0)
        if stop.is_set():
            break
        stats.set_current(iv + 1)
        dr, dw, errs = stats.end_interval()
        total = dr + dw
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{stamp} write(tps={dw}) read(tps={dr}) total(tps={total} errors={errs})")

    stop.set()
    wait(futures)
    pool.shutdown(wait=False)
    client.close()

    for line in stats.summary():
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
