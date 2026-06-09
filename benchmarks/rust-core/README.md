# Rust-core bench

Pure-Rust bench against `aerospike-core` — same workload shape as
`python -m benchmarks.benchmark` but with zero Python in the path. Sampled
latency, env-var-driven config, single-line output.

Use this to establish the Rust-only ceiling on the same hardware the
Python clients run on. The gap between this and PAC sync direct is the
cost of the PyO3 + Python boundary.

## Build

```bash
cd benchmarks/rust-core
cargo build --release
# binary at target/release/rust-core
```

First build downloads and compiles `aerospike-core` and Tokio
(~5 minutes on bench-client); subsequent builds use the cache.

## Run

```bash
# Async mode (default): one Tokio runtime, N concurrent tasks
TASKS=32 DURATION=15 WARMUP=3 ./target/release/rust-core

# Sync mode: N OS threads, each calling client.get/put via Handle::block_on
MODE=sync THREADS=32 DURATION=15 WARMUP=3 ./target/release/rust-core

# Single-thread sync (lowest per-op latency, no concurrency)
MODE=sync THREADS=1 DURATION=15 WARMUP=3 ./target/release/rust-core
```

Output:

```
rust mode=async workers=32 duration=15s warmup=3s read_pct=50
measured: total_ops=…, duration=15s, TPS=…, wall=…s,
          lat_samples=…, p50=…us, p99=…us, p999=…us
```

## Env vars

- `AEROSPIKE_HOST` — seed (default `10.138.0.3:3100`)
- `NAMESPACE`, `SET` — default `test`/`test`
- `KEYS` — key space size (default 100000)
- `THREADS` (sync) or `TASKS` (async) — concurrency (default 32)
- `DURATION` — measured seconds (default 15)
- `WARMUP` — warmup seconds (default 3)
- `READ_PCT` — % reads (default 50, rest are writes)
- `LAT_SAMPLE_EVERY` — sample 1-in-N ops for latency (default 100)
- `MODE` — `async` (default) or `sync`
