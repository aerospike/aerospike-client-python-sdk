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

"""Side-by-side benchmark comparison between the Aerospike Python SDK and the
legacy Python client.

Runs identical workloads on both clients via subprocess, parses the results,
and prints a comparison table.  Optionally repeats for statistical averaging
(``--runs N``) and can export to CSV (``--csv FILE``).

Usage::

    python -m benchmarks.compare --help
    python -m benchmarks.compare -k 1000 -d 10 -z 4 --legacy-path ~/code/aerospike-client-python
    python -m benchmarks.compare -k 100000 -d 30 --runs 3 --csv results.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

# Load aerospike.env so default_host() reads AEROSPIKE_HOST.
from ._env import default_host


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class RunResult:
    """Parsed metrics from a single benchmark run."""

    client: str = ""
    mode: str = ""
    workload: str = ""
    tps: float = 0.0
    read_tps: float = 0.0
    write_tps: float = 0.0
    p50_ms: float = 0.0
    p90_ms: float = 0.0
    p99_ms: float = 0.0
    p999_ms: float = 0.0
    max_ms: float = 0.0
    peak_rss_mb: float = 0.0
    error: str = ""


@dataclass
class AggregateResult:
    """Averaged metrics across multiple runs."""

    client: str = ""
    mode: str = ""
    workload: str = ""
    runs: int = 0
    tps_avg: float = 0.0
    tps_stdev: float = 0.0
    read_tps_avg: float = 0.0
    write_tps_avg: float = 0.0
    p50_avg: float = 0.0
    p99_avg: float = 0.0
    p999_avg: float = 0.0
    max_avg: float = 0.0
    peak_rss_avg: float = 0.0


# ---------------------------------------------------------------------------
# PSDK output parser
# ---------------------------------------------------------------------------

_PSDK_TPS_RE = re.compile(
    r"Read\s+TPS:\s+avg=(\d+\.?\d*)\s+median=(\d+\.?\d*)",
)
_PSDK_WRITE_TPS_RE = re.compile(
    r"Write\s+TPS:\s+avg=(\d+\.?\d*)\s+median=(\d+\.?\d*)",
)
_PSDK_TOTAL_TPS_RE = re.compile(
    r"Total\s+TPS:\s+avg=(\d+\.?\d*)\s+median=(\d+\.?\d*)",
)
_PSDK_LATENCY_RE = re.compile(
    r"Latency\s+p50=(\d+\.?\d*)ms\s+p90=(\d+\.?\d*)ms\s+"
    r"p99=(\d+\.?\d*)ms\s+p99\.9=(\d+\.?\d*)ms\s+max=(\d+\.?\d*)ms",
)
_PSDK_RSS_RE = re.compile(r"Peak RSS:\s+(\d+\.?\d*)\s+MB")


def parse_psdk_output(text: str) -> RunResult:
    """Extract summary metrics from PSDK benchmark stdout."""
    r = RunResult(client="PSDK")
    for line in text.splitlines():
        m = _PSDK_TOTAL_TPS_RE.search(line)
        if m:
            r.tps = float(m.group(1))
        m = _PSDK_TPS_RE.search(line)
        if m:
            r.read_tps = float(m.group(1))
        m = _PSDK_WRITE_TPS_RE.search(line)
        if m:
            r.write_tps = float(m.group(1))
        m = _PSDK_LATENCY_RE.search(line)
        if m:
            r.p50_ms = float(m.group(1))
            r.p90_ms = float(m.group(2))
            r.p99_ms = float(m.group(3))
            r.p999_ms = float(m.group(4))
            r.max_ms = float(m.group(5))
        m = _PSDK_RSS_RE.search(line)
        if m:
            r.peak_rss_mb = float(m.group(1))
    return r


# ---------------------------------------------------------------------------
# Subprocess runners
# ---------------------------------------------------------------------------

def _find_python_for_repo(repo_path: Path) -> str:
    """Resolve the Python interpreter for a repo using its pyenv config.

    If the repo has a ``.python-version`` file, look up the corresponding
    pyenv virtualenv.  Otherwise fall back to ``"python"`` (relies on the
    caller's active environment).
    """
    pv = repo_path / ".python-version"
    if pv.exists():
        pyenv_name = pv.read_text().strip().splitlines()[0].strip()
        pyenv_root = os.environ.get("PYENV_ROOT", os.path.expanduser("~/.pyenv"))
        candidate = Path(pyenv_root) / "versions" / pyenv_name / "bin" / "python"
        if candidate.exists():
            return str(candidate)
    return "python"


def _tls_auth_args(
    tls_ca_file: Optional[str],
    tls_cert_file: Optional[str],
    tls_key_file: Optional[str],
    user: Optional[str],
    password: Optional[str],
    auth_mode: Optional[str],
) -> List[str]:
    """Build CLI fragments for TLS/auth options."""
    args: List[str] = []
    if tls_ca_file:
        args.extend(["--tls-ca-file", tls_ca_file])
    if tls_cert_file:
        args.extend(["--tls-cert-file", tls_cert_file])
    if tls_key_file:
        args.extend(["--tls-key-file", tls_key_file])
    if user:
        args.extend(["-U", user])
    if password:
        args.extend(["-P", password])
    if auth_mode:
        args.extend(["--auth-mode", auth_mode])
    return args


def run_psdk(
    *,
    psdk_path: Path,
    python: str,
    hosts: str,
    namespace: str,
    set_name: str,
    key_count: int,
    workload: str,
    async_tasks: int,
    threads: Optional[int],
    duration: float,
    mode: str,
    latency: str,
    warmup: int,
    cooldown: int,
    populate: bool,
    tls_ca_file: Optional[str] = None,
    tls_cert_file: Optional[str] = None,
    tls_key_file: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    auth_mode: Optional[str] = None,
    truncate: bool = False,
    batch_size: int = 0,
) -> RunResult:
    """Run the PSDK benchmark tool and parse its output."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(psdk_path)
    extra = _tls_auth_args(tls_ca_file, tls_cert_file, tls_key_file, user, password, auth_mode)
    concurrency_args = ["-z", str(async_tasks)]
    if threads is not None:
        concurrency_args.extend(["--threads", str(threads)])

    # Populate phase (insert keys) if requested.
    if populate:
        pop_cmd = [
            python, "-m", "benchmarks.benchmark",
            "-H", hosts, "-n", namespace, "-s", set_name,
            "-k", str(key_count), *concurrency_args,
            "-w", "I", "-c", str(key_count), "-d", "120",
            "--warmup", "0", "--cooldown", "0",
            *extra,
        ]
        if truncate:
            pop_cmd.append("--truncate")
        proc = subprocess.run(
            pop_cmd, capture_output=True, text=True, cwd=str(psdk_path),
            env=env, timeout=300,
        )
        if proc.returncode != 0:
            r = RunResult(client="PSDK", error=f"populate failed: {proc.stderr[:500]}")
            return r

    cmd = [
        python, "-m", "benchmarks.benchmark",
        "-H", hosts, "-n", namespace, "-s", set_name,
        "-k", str(key_count), *concurrency_args,
        "-w", workload, "-d", str(duration),
        "--mode", mode,
        "--latency", latency,
        "--warmup", str(warmup), "--cooldown", str(cooldown),
        *extra,
    ]
    if batch_size and batch_size > 1:
        cmd.extend(["--batch-size", str(batch_size)])
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(psdk_path),
            env=env, timeout=duration + 60,
        )
    except subprocess.TimeoutExpired:
        return RunResult(client="PSDK", mode=mode, error="timeout")

    result = parse_psdk_output(proc.stdout)
    result.mode = mode
    result.workload = workload
    if proc.returncode != 0 and not result.tps:
        result.error = proc.stderr[:500]
    return result


def run_legacy(
    *,
    legacy_python: str,
    benchmark_script: Path,
    hosts: str,
    namespace: str,
    set_name: str,
    key_count: int,
    workload: str,
    concurrency: int,
    duration: float,
    warmup: int,
    cooldown: int,
) -> RunResult:
    """Run the legacy benchmark (get/operate, same ops as PSDK)."""
    cmd = [
        legacy_python, str(benchmark_script),
        "-H", hosts, "-n", namespace, "-s", set_name,
        "-k", str(key_count), "-z", str(concurrency),
        "-w", workload, "-d", str(duration),
        "--warmup", str(warmup), "--cooldown", str(cooldown),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=duration + 60,
        )
    except subprocess.TimeoutExpired:
        return RunResult(client="Legacy", mode="sync", error="timeout")

    # Output format matches PSDK, so reuse the same parser.
    result = parse_psdk_output(proc.stdout)
    result.client = "Legacy"
    result.mode = "sync"
    result.workload = workload
    if proc.returncode != 0 and not result.tps:
        result.error = proc.stderr[:500]
    return result


# ---------------------------------------------------------------------------
# PAC benchmark runner
# ---------------------------------------------------------------------------

def _resolve_pac_path(args) -> Path:
    if args.pac_path:
        return args.pac_path.expanduser()
    env_path = os.environ.get("PAC_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return Path(os.environ.get(
        "PAC_PATH",
        str(Path.home() / "tmp" / "aerospike-client-python-async"),
    )).expanduser()


def run_pac_benchmark(
    *,
    pac_path: Path,
    python: str,
    hosts: str,
    namespace: str,
    set_name: str,
    key_count: int,
    workload: str,
    async_tasks: int,
    duration: float,
    warmup: int,
    cooldown: int,
    batch_size: int = 0,
) -> RunResult:
    """Run the standalone PAC benchmark and parse its output."""
    cmd = [
        python, "-m", "benchmarks.benchmark",
        "-H", hosts, "-n", namespace, "-s", set_name,
        "-k", str(key_count), "-z", str(async_tasks),
        "-w", workload, "-d", str(duration),
        "--warmup", str(warmup), "--cooldown", str(cooldown),
    ]
    if batch_size and batch_size > 1:
        cmd.extend(["--batch-size", str(batch_size)])
    env = os.environ.copy()
    env["PYTHONPATH"] = str(pac_path)
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(pac_path),
            env=env, timeout=duration + 60,
        )
    except subprocess.TimeoutExpired:
        return RunResult(client="PAC", mode="async", error="timeout")

    result = parse_psdk_output(proc.stdout)
    result.client = "PAC"
    result.mode = "async"
    result.workload = workload
    if proc.returncode != 0 and not result.tps:
        result.error = proc.stderr[:500]
    return result


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate(results: List[RunResult]) -> AggregateResult:
    """Compute averages and stdev over multiple runs."""
    if not results:
        return AggregateResult()
    first = results[0]
    tps_vals = [r.tps for r in results if not r.error]
    agg = AggregateResult(
        client=first.client,
        mode=first.mode,
        workload=first.workload,
        runs=len(tps_vals),
        tps_avg=statistics.mean(tps_vals) if tps_vals else 0.0,
        tps_stdev=statistics.stdev(tps_vals) if len(tps_vals) > 1 else 0.0,
        read_tps_avg=statistics.mean([r.read_tps for r in results if not r.error]) if tps_vals else 0.0,
        write_tps_avg=statistics.mean([r.write_tps for r in results if not r.error]) if tps_vals else 0.0,
        p50_avg=statistics.mean([r.p50_ms for r in results if not r.error]) if tps_vals else 0.0,
        p99_avg=statistics.mean([r.p99_ms for r in results if not r.error]) if tps_vals else 0.0,
        p999_avg=statistics.mean([r.p999_ms for r in results if not r.error]) if tps_vals else 0.0,
        max_avg=statistics.mean([r.max_ms for r in results if not r.error]) if tps_vals else 0.0,
        peak_rss_avg=statistics.mean([r.peak_rss_mb for r in results if not r.error]) if tps_vals else 0.0,
    )
    return agg


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

_COL_WIDTHS = {
    "Client": 10, "Mode": 10, "Workload": 10, "Runs": 4,
    "TPS avg": 10, "TPS stdev": 10,
    "Read TPS": 10, "Write TPS": 10,
    "p50 ms": 8, "p99 ms": 8, "p99.9 ms": 8, "max ms": 8,
    "RSS MB": 8,
}


def _fmt_row(vals: Dict[str, str]) -> str:
    parts = []
    for col, width in _COL_WIDTHS.items():
        parts.append(vals.get(col, "").rjust(width))
    return "  ".join(parts)


def print_comparison(aggregates: List[AggregateResult]) -> None:
    """Print a formatted comparison table to stdout."""
    header = {col: col for col in _COL_WIDTHS}
    print()
    print("=" * 120)
    print("  BENCHMARK COMPARISON")
    print("=" * 120)
    print(_fmt_row(header))
    print("-" * 120)
    for agg in aggregates:
        vals = {
            "Client": agg.client,
            "Mode": agg.mode,
            "Workload": agg.workload,
            "Runs": str(agg.runs),
            "TPS avg": f"{agg.tps_avg:.0f}",
            "TPS stdev": f"{agg.tps_stdev:.0f}" if agg.tps_stdev else "-",
            "Read TPS": f"{agg.read_tps_avg:.0f}" if agg.read_tps_avg else "-",
            "Write TPS": f"{agg.write_tps_avg:.0f}" if agg.write_tps_avg else "-",
            "p50 ms": f"{agg.p50_avg:.1f}" if agg.p50_avg else "-",
            "p99 ms": f"{agg.p99_avg:.1f}" if agg.p99_avg else "-",
            "p99.9 ms": f"{agg.p999_avg:.1f}" if agg.p999_avg else "-",
            "max ms": f"{agg.max_avg:.1f}" if agg.max_avg else "-",
            "RSS MB": f"{agg.peak_rss_avg:.1f}" if agg.peak_rss_avg else "-",
        }
        print(_fmt_row(vals))
    print("-" * 120)

    # Speedup summary against legacy.
    non_legacy = [a for a in aggregates if a.client != "Legacy"]
    legacy_rows = [a for a in aggregates if a.client == "Legacy"]
    if non_legacy and legacy_rows:
        legacy_tps = legacy_rows[0].tps_avg
        if legacy_tps > 0:
            for row in non_legacy:
                ratio = row.tps_avg / legacy_tps
                label = f"{row.client} {row.mode}"
                print(f"\n  {label} / Legacy TPS ratio: {ratio:.2f}x")
    print()


def write_csv(path: str, aggregates: List[AggregateResult]) -> None:
    """Export comparison results to a CSV file."""
    cols = list(_COL_WIDTHS.keys())
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for agg in aggregates:
            writer.writerow([
                agg.client, agg.mode, agg.workload, agg.runs,
                f"{agg.tps_avg:.0f}", f"{agg.tps_stdev:.0f}",
                f"{agg.read_tps_avg:.0f}", f"{agg.write_tps_avg:.0f}",
                f"{agg.p50_avg:.1f}", f"{agg.p99_avg:.1f}",
                f"{agg.p999_avg:.1f}", f"{agg.max_avg:.1f}",
                f"{agg.peak_rss_avg:.1f}",
            ])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Compare Aerospike Python SDK benchmark results against "
        "the legacy Python client.",
    )
    p.add_argument(
        "--psdk-path",
        type=Path,
        default=None,
        help="Path to PSDK repo root (default: this repo's root).",
    )
    p.add_argument(
        "--psdk-python",
        default=None,
        help="Python interpreter for PSDK (default: resolved from repo's .python-version).",
    )
    p.add_argument(
        "--pac-path",
        type=Path,
        default=None,
        help="Path to PAC repo root (default: $PAC_PATH or ~/tmp/aerospike-client-python-async).",
    )
    p.add_argument(
        "--pac-python",
        default=None,
        help="Python interpreter for PAC (default: resolved from repo's .python-version).",
    )
    p.add_argument(
        "--legacy-path",
        type=Path,
        default=None,
        help="Path to legacy aerospike-client-python repo "
        "(default: $LEGACY_CLIENT_PATH or ~/code/aerospike-client-python).",
    )
    p.add_argument(
        "--legacy-python",
        default=None,
        help="Python interpreter for legacy client (default: resolved from repo's .python-version).",
    )
    p.add_argument("-H", "--hosts", default=default_host(), help="Cluster seed (default: %(default)s from aerospike.env).")
    p.add_argument("-n", "--namespace", default="test")
    p.add_argument("-s", "--set", dest="set_name", default="testset")
    p.add_argument("-k", "--keys", type=int, default=1000, help="Key space size.")
    p.add_argument(
        "-w", "--workload", default="RU,50",
        help="Workload for PSDK (legacy always runs as reads/writes).",
    )
    p.add_argument("-z", "--async-tasks", type=int, default=32, dest="async_tasks",
                   help="Number of concurrent async tasks (default: %(default)s).")
    p.add_argument("--batch-size", dest="batch_size", type=int, default=0,
                   help="If >1, each operation issues one batch command of N keys "
                   "(drives batch_read/batch_write paths). Default: 0 (single-key).")
    p.add_argument("--threads", type=int, default=None,
                   help="OS threads for sync mode (default: falls back to -z).")
    p.add_argument("-d", "--duration", type=float, default=10.0, help="Run duration in seconds.")
    p.add_argument("--latency", default="7,1", help="Latency histogram config (COLUMNS,SHIFT).")
    p.add_argument("--warmup", type=int, default=0, help="PSDK warmup intervals.")
    p.add_argument("--cooldown", type=int, default=0, help="PSDK cooldown intervals.")
    p.add_argument("--runs", type=int, default=1, help="Number of runs for averaging.")
    p.add_argument(
        "--modes", default="pac,async,sync",
        help="Comma-separated PSDK modes to test (default: pac,async,sync).",
    )
    p.add_argument("--csv", default=None, metavar="FILE", help="Export results to CSV.")
    p.add_argument(
        "--skip-populate", action="store_true",
        help="Skip the populate (insert) phase before benchmarking.",
    )
    p.add_argument(
        "--skip-legacy", action="store_true",
        help="Skip the legacy client run (PSDK-only comparison across modes).",
    )
    p.add_argument(
        "--truncate", action="store_true",
        help="Truncate the set before populating (removes stale data from prior runs).",
    )
    # TLS options
    p.add_argument("--tls-ca-file", default=None, metavar="PATH", help="CA certificate for TLS.")
    p.add_argument("--tls-cert-file", default=None, metavar="PATH", help="Client certificate for mutual TLS.")
    p.add_argument("--tls-key-file", default=None, metavar="PATH", help="Client private key for mutual TLS.")
    # Authentication options
    p.add_argument("-U", "--user", default=None, help="User name for authentication.")
    p.add_argument("-P", "--password", default=None, help="Password for authentication.")
    p.add_argument("--auth-mode", choices=("INTERNAL", "EXTERNAL", "PKI"), default=None, help="Authentication mode.")
    return p


def _resolve_legacy_path(args: argparse.Namespace) -> Path:
    if args.legacy_path:
        return args.legacy_path.expanduser()
    env_path = os.environ.get("LEGACY_CLIENT_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return Path(os.environ.get(
        "LEGACY_CLIENT_PATH",
        str(Path.home() / "code" / "aerospike-client-python"),
    )).expanduser()



def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    psdk_path = args.psdk_path or Path(__file__).resolve().parent.parent
    psdk_python = args.psdk_python or _find_python_for_repo(psdk_path)
    pac_path = _resolve_pac_path(args)
    pac_python = args.pac_python or _find_python_for_repo(pac_path)
    legacy_path = _resolve_legacy_path(args)
    legacy_python = args.legacy_python or _find_python_for_repo(legacy_path)

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    populate = not args.skip_populate
    has_populated = False

    all_aggregates: List[AggregateResult] = []

    # --- PAC runs (standalone subprocess in PAC repo) ---
    if "pac" in modes:
        results: List[RunResult] = []
        for run_idx in range(args.runs):
            label = f"PAC async run {run_idx + 1}/{args.runs}"
            print(f"\n>>> {label} ...")
            # Populate via PSDK on the first run if needed.
            if populate and not has_populated:
                r_pop = run_psdk(
                    psdk_path=psdk_path, python=psdk_python,
                    hosts=args.hosts, namespace=args.namespace,
                    set_name=args.set_name, key_count=args.keys,
                    workload="I", async_tasks=args.async_tasks, threads=args.threads,
                    duration=120, mode="async", latency=args.latency,
                    warmup=0, cooldown=0, populate=True,
                    truncate=args.truncate,
                )
                if r_pop.error:
                    print(f"    POPULATE ERROR: {r_pop.error}")
                has_populated = True
            if not pac_path.exists():
                print(f"    WARNING: PAC path not found: {pac_path}")
                print("    Set --pac-path or $PAC_PATH. Skipping PAC runs.")
                break
            r = run_pac_benchmark(
                pac_path=pac_path,
                python=pac_python,
                hosts=args.hosts,
                namespace=args.namespace,
                set_name=args.set_name,
                key_count=args.keys,
                workload=args.workload,
                async_tasks=args.async_tasks,
                duration=args.duration,
                warmup=args.warmup,
                cooldown=args.cooldown,
                batch_size=args.batch_size,
            )
            if r.error:
                print(f"    ERROR: {r.error}")
            else:
                print(f"    TPS={r.tps:.0f}  p99={r.p99_ms:.1f}ms  RSS={r.peak_rss_mb:.1f}MB")
            results.append(r)
        if results:
            all_aggregates.append(aggregate(results))

    # --- PSDK runs ---
    psdk_modes = [m for m in modes if m != "pac"]
    for mode in psdk_modes:
        results = []
        for run_idx in range(args.runs):
            label = f"PSDK {mode} run {run_idx + 1}/{args.runs}"
            print(f"\n>>> {label} ...")
            do_populate = populate and not has_populated and run_idx == 0
            r = run_psdk(
                psdk_path=psdk_path,
                python=psdk_python,
                hosts=args.hosts,
                namespace=args.namespace,
                set_name=args.set_name,
                key_count=args.keys,
                workload=args.workload,
                async_tasks=args.async_tasks, threads=args.threads,
                duration=args.duration,
                mode=mode,
                latency=args.latency,
                warmup=args.warmup,
                cooldown=args.cooldown,
                populate=do_populate,
                tls_ca_file=args.tls_ca_file,
                tls_cert_file=args.tls_cert_file,
                tls_key_file=args.tls_key_file,
                user=args.user,
                password=args.password,
                auth_mode=args.auth_mode,
                truncate=(args.truncate and not has_populated and run_idx == 0),
                batch_size=args.batch_size,
            )
            if do_populate:
                has_populated = True
            r.mode = mode
            r.workload = args.workload
            if r.error:
                print(f"    ERROR: {r.error}")
            else:
                print(f"    TPS={r.tps:.0f}  p99={r.p99_ms:.1f}ms  RSS={r.peak_rss_mb:.1f}MB")
            results.append(r)
        agg = aggregate(results)
        all_aggregates.append(agg)

    # --- Legacy runs ---
    if not args.skip_legacy:
        legacy_script = Path(__file__).resolve().parent / "legacy_benchmark.py"
        if not legacy_script.exists():
            print(f"\nWARNING: legacy_benchmark.py not found at {legacy_script}")
        elif legacy_python == "python":
            print("\nWARNING: No legacy Python environment found. Skipping legacy runs.")
            print("Set --legacy-path to a repo with a .python-version, or --legacy-python.")
        else:
            results: List[RunResult] = []
            for run_idx in range(args.runs):
                label = f"Legacy sync run {run_idx + 1}/{args.runs}"
                print(f"\n>>> {label} ...")
                # Legacy C-extension client does not support concurrent
                # threads; always run single-threaded.
                r = run_legacy(
                    legacy_python=legacy_python,
                    benchmark_script=legacy_script,
                    hosts=args.hosts,
                    namespace=args.namespace,
                    set_name=args.set_name,
                    key_count=args.keys,
                    workload=args.workload,
                    concurrency=1,
                    duration=args.duration,
                    warmup=args.warmup,
                    cooldown=args.cooldown,
                )
                if r.error:
                    print(f"    ERROR: {r.error}")
                else:
                    print(f"    TPS={r.tps:.0f}  p99={r.p99_ms:.1f}ms  RSS={r.peak_rss_mb:.1f}MB")
                results.append(r)
            agg = aggregate(results)
            all_aggregates.append(agg)

    # --- Report ---
    print_comparison(all_aggregates)

    if args.csv:
        write_csv(args.csv, all_aggregates)
        print(f"Results written to {args.csv}")

    # Clean up benchmark data at the end so repeated compare runs
    # don't accumulate stale records that fill the server's memory.
    if populate:
        cleanup_cmd = [
            psdk_python, "-m", "benchmarks.benchmark",
            "-H", args.hosts, "-n", args.namespace, "-s", args.set_name,
            "-k", "1", "-w", "I", "-c", "0", "-d", "1",
            "--warmup", "0", "--cooldown", "0", "--truncate-after",
        ]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(psdk_path)
        subprocess.run(
            cleanup_cmd, capture_output=True, text=True,
            cwd=str(psdk_path), env=env, timeout=30,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
