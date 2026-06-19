# Performance modes — which API and Python build should I use?

PSDK exposes several execution modes. The right one depends on (1) whether you can run a free-threaded CPython build (e.g., 3.14t) with the GIL disabled, and (2) what your workload looks like — predominantly single-key reads/writes, or complex queries with builders, batches, and error handlers.

This guide is the short, user-facing decision tree. The full numbers and methodology behind every recommendation are in [`benchmarking.md`](benchmarking.md).

## TL;DR decision tree

1. **Single-key reads/writes, want max throughput?** Use [`session.get()` / `session.put()`](#fast-path-sessionget--sessionput) — the fast-path API.
2. **Complex queries (secondary index, AEL filters, batch ops, error handlers)?** Use [chained builders](#chained-builder-api) — `session.query(...).where(...).execute()` and friends.
3. **Sync or async?** If you have an existing sync codebase, use `SyncClient`. For new code or web servers, async is the standard.
4. **Free-threaded Python (e.g. 3.14t)?** Yes if you need high throughput across many threads. No if you depend on C extensions that aren't FT-safe.
5. **AsyncPool?** Only on free-threaded Python. Slower than single-client on non-FT.

## Free-threaded vs regular Python

PSDK works on both standard CPython and a free-threaded build (e.g., `3.14t`). The choice matters a lot for high-throughput workloads.

| | Regular CPython | Free-threaded CPython (e.g. 3.14t) |
|---|---|---|
| **GIL** | Always on. Threads serialize through one interpreter. | Off when invoked with `PYTHON_GIL=0`. Multiple threads run Python in true parallel. |
| **Single-thread perf** | Same | Same (slightly slower for some workloads due to atomic refcounts) |
| **Multi-thread perf** | Capped by GIL — usually 1.5-2× single-thread no matter how many threads | Scales near-linearly with cores for I/O-bound work |
| **C extension support** | Universal | Limited — extensions must declare `Py_mod_gil = Py_MOD_GIL_NOT_USED` |
| **Recommended for PSDK?** | If GIL-on simplicity is fine for your workload | If you want PSDK's high-TPS modes |

### Setup for free-threaded mode

```bash
# Install the free-threaded build (uv or pyenv)
uv python install 3.14.5+freethreaded

# Always launch with PYTHON_GIL=0
PYTHON_GIL=0 python my_app.py
```

**Critical gotcha:** if any imported C extension hasn't opted into free-threading, the interpreter silently re-enables the GIL. Verify with `sys._is_gil_enabled()` returning `False` after all imports. PSDK's dependency PAC (`aerospike-async`) is FT-safe; many other libraries aren't yet.

If `PYTHON_GIL=0` is unset on the free-threaded build, the GIL stays on by default — which negates the entire point of using it.

(fast-path-sessionget--sessionput)=
## Fast-path: `session.get` / `session.put`

For single-key operations where you don't need filters, error handlers, projections, batch semantics, secondary indexes, etc., the fast-path methods bypass the builder + stream wrapping and call PAC's native blocking/async APIs directly with the session-cached policy.

### Sync example

```python
from aerospike_sdk import Behavior, SyncClient
from aerospike_async import Key

with SyncClient("localhost:3000") as client:
    session = client.create_session(Behavior.DEFAULT)
    k = Key("test", "users", "alice")
    session.put(k, {"name": "Alice", "age": 28})
    record = session.get(k)
    print(record.bins)
```

### Async example

```python
import asyncio
from aerospike_sdk import Behavior, Client
from aerospike_async import Key

async def main():
    async with Client("localhost:3000") as client:
        session = client.create_session(Behavior.DEFAULT)
        k = Key("test", "users", "alice")
        await session.put(k, {"name": "Alice", "age": 28})
        record = await session.get(k)
        print(record.bins)

asyncio.run(main())
```

The fast-path APIs accept an optional `bins=` projection for reads and an arbitrary `bins` dict for writes. Errors raise directly (no `RecordResult` wrapping).

**When NOT to use fast-path:**
- Anything that needs `where(...)` filters, `expire_record_after_seconds`, `with_durable_delete`, generation checks, or `record_exists_action` overrides — use the builder.
- Reads from a `DataSet` with a secondary-index query — use the builder.
- Batch reads/writes across multiple keys — use the builder or the `session.batch()` chain.
- `RecordResult.is_ok` / `error` introspection per record — use the builder, which yields wrapped `RecordResult` instances.

(chained-builder-api)=
## Chained builder API

The full-featured chainable API that mirrors the Aerospike SDK shape across languages.

```python
from aerospike_sdk import Behavior, Client, DataSet, ErrorStrategy

async with Client("localhost:3000") as client:
    session = client.create_session(Behavior.DEFAULT)
    users = DataSet.of("test", "users")

    # Filtered query — AEL filter expression
    results = await (
        session.query(users)
        .where("$.age > %s and $.country == '%s'", 25, "US")
        .execute()
    )
    async for r in results:
        if r.is_ok:
            print(r.record.bins)

    # Write with TTL + error handler
    stream = await (
        session.upsert(users.id(1))
        .put({"name": "Alice"})
        .expire_record_after_seconds(3600)
        .execute(on_error=ErrorStrategy.IN_STREAM)
    )
    await stream.collect()
```

Use the builder when you need filter expressions, batch operations, secondary-index queries, error handlers, TTL overrides, or generation checks. For plain single-key reads and writes, prefer the fast-path.

## AsyncPool — multi-loop async on free-threaded Python only

`AsyncPool` runs N event loops on N OS threads with one PAC client each, so async work can use multiple CPU cores in parallel. **It only helps under free-threaded Python.**

```python
from aerospike_sdk import AsyncPool, Behavior
from aerospike_sdk.aio.client import Client

def factory():
    return Client("localhost:3000")

async def per_loop(client, loop_idx):
    session = client.create_session(Behavior.DEFAULT)
    # ... do work, e.g. asyncio.gather of session.get/put calls ...

async with AsyncPool(factory, loop_count=4) as pool:
    await pool.map(per_loop, range(4))
```

**Scaling**: at `loop_count >= 4`, AsyncPool automatically gives each Client
its own PAC Tokio runtime (per-Client runtime isolation). This eliminates the
cross-loop scheduler contention that previously capped throughput at 4 loops,
so TPS scales monotonically. Measured on 8-core hardware, FT Python (with
uvloop enabled by default and PAC's drainer thread serializing
`call_soon_threadsafe` wakeups across all pooled Clients):

| Pool size | TPS | p99 latency |
|---|---|---|
| 4 × 64 tasks | **~246K** | 2.5 ms |
| 8 × 64 tasks | **~273K** | 4.5 ms |

The ceiling at ~270-280K is now essentially the same as PSDK sync `ct_runtime`
(~265K) — async no longer carries a structural gap below sync on free-threaded
Python. Past 8 loops, additional loops trade p99 latency for marginal TPS;
pick `loop_count` based on the tail-latency budget your workload tolerates.

You can override the auto-enable threshold via `AsyncPool(..., per_client_runtime=True|False)`.
Forcing it on at low loop counts may be useful on smaller hardware; forcing
it off reverts to the shared global Tokio runtime path. Worker count is
auto-derived as `max(2, os.cpu_count() // loop_count)`.

**Do not use AsyncPool on regular (GIL-on) Python.** Empirically it's 17-26% *slower* than a single-client async setup because:
- The GIL still serializes all Python code across the 4 OS threads
- The pool's task orchestration adds Python work that has nowhere to escape to under the GIL

On regular Python, use a single `Client` + `asyncio.gather` instead.

## Sync vs async — when to pick which

- **Sync (`SyncClient`)** is best when:
  - You're integrating into an existing sync codebase (Django views, scripts, etc.)
  - Per-op latency matters more than concurrency depth
  - You want the absolute lowest per-op overhead — PSDK sync fast-path is roughly at parity with PAC's direct blocking API

- **Async (`Client`)** is best when:
  - You already have an asyncio event loop (FastAPI, aiohttp, etc.)
  - You need to overlap I/O across many concurrent operations
  - You're willing to use uvloop for higher throughput (default in modern asyncio + free-threaded Python setups)

Both modes share the same `Session` API surface (chained builders + fast-path shortcuts), the same `Behavior` policy model, and the same error semantics.

:::{note}
When you construct a `SyncClient` without supplying your own `ClientPolicy`,
PSDK sets `conn_pools_per_node = 8` (PAC's default is 4). The async-tuned PAC
default works well for single-loop or per-Client-runtime workloads where the
event loop serializes pool access naturally, but sync wrappers drive PAC from
many caller threads and see real connection-pool mutex contention at 4 — the
p99 tail roughly doubles. Pass your own `ClientPolicy` if you need a different
value (e.g. lower for memory-constrained deployments).
:::

## Performance summary table

Numbers from the [Benchmarking Guide](benchmarking.md) — 8-vCPU isolated client VM → 3× 8-vCPU isolated server VMs over a low-latency private network, 100K keys, 50/50 RW, 50-byte payload.

### Single-key dispatch (batch size 1)

| Mode | Threads / Tasks | Free-threaded TPS | Non-FT TPS |
|---|---|---|---|
| **Sync fast-path** (`session.get`/`put`) | 32 | **~210K** | ~51K |
| Sync builder (`session.query(k).execute()`) | 32 | ~148K | ~31K |
| **Async fast-path, AsyncPool 8×64** | 512 tasks | **~273K** | (FT only) |
| **Async fast-path, AsyncPool 4×64** | 256 tasks | **~246K** | ~94K |
| Async fast-path, single client | 32 tasks | ~119K | ~104K |
| Async builder, AsyncPool 4×64 | 256 tasks | ~179K | ~57K |
| Async builder, single client | 32 tasks | ~67K | ~62K |

:::{admonition} Experimental: `current_thread_runtime` (ct_runtime)
:class: warning

`SyncClient` accepts a `current_thread_runtime=True` flag that gives each Python thread its own PAC `_LocalClient` (per-thread Tokio current-thread runtime). **It boosts measured TPS to ~265K (sync fp) / ~187K (sync builder)** on free-threaded Python — but it comes with non-trivial operational baggage:

- **N× cluster-tend threads.** Each per-thread runtime owns its own `Cluster` and runs its own cluster-tend loop. At 32 worker threads that's 32 tend loops polling the cluster every second.
- **N× connection pools.** Each thread's runtime maintains its own pool. PSDK auto-defaults `conn_pools_per_node=1` when you opt in (so total per-node connections stay around `N threads × 1 pool` ≈ the non-ct_runtime default of 8), but if you pass your own `ClientPolicy` you take responsibility for the connection count.
- **Incomplete `_with_overrides` surface.** Not every PAC method routes through the ct_runtime path; some operations still hit the shared multi-thread runtime even when ct_runtime is on.

Usage (opt-in):

```python
from aerospike_sdk import Behavior, SyncClient

# Auto-default `conn_pools_per_node = 1` applies because we didn't pass a policy.
# Cluster-tend multiplication is NOT mitigated by the default — each
# worker thread that calls session.get/put will lazily create its own
# _LocalClient, each with its own tend loop.
with SyncClient("localhost:3000", current_thread_runtime=True) as client:
    session = client.create_session(Behavior.DEFAULT)
    # ... worker threads each call session.get / session.put as normal ...
```

Treat ct_runtime as an experimental performance lever for benchmarking and tightly-controlled deployments. The default sync path (one shared Tokio multi-thread runtime + one shared connection pool) is the recommended production setup.
:::

### With batching (`--batch-size > 1`, free-threaded)

When the workload can group keys per call, the chained-builder API amortizes its per-op overhead and surpasses every single-key number above.

| Mode | Batch size | Peak TPS |
|---|---|---|
| **Sync builder** | 128 | **~484K** |
| AsyncPool builder, 4×64 | 64 | ~336K |
| Async single-loop builder, 32 tasks | 128 | ~191K |

**Practical reading:**
- If your workload can batch keys, the **sync builder with `session.batch()` or multi-key `session.query([keys])`** is the highest-throughput mode — scales to ~484K TPS at batch=128. Doubling the batch size keeps amortizing the per-call cost.
- For single-key workloads on free-threaded Python, the **sync fast-path** (~210K) is the highest non-experimental single-key sync mode. If you need async, **AsyncPool fast-path** at 4-8 loops reaches ~246-273K — equal to or higher than sync on the same hardware.
- On regular Python (GIL on), single-client async (~104K) is actually faster than sync (~51K) because of GIL contention across many sync threads. AsyncPool helps less under non-FT (~94K at 4×64) — close to single-loop with extra orchestration cost.

## Why sync and async perform similarly now

The cost stacks for sync and async used to diverge sharply — async historically lost ~50% to the asyncio ↔ Tokio bridge per op. With PAC's drainer thread (a single persistent waker thread handling all Tokio→asyncio wakeups) plus uvloop installed by default under FT, the async ceiling has closed substantially:

- **Sync clients pay only the PyO3 boundary cost** plus a per-op thread-handoff between caller and Tokio (~71 µs per op). PSDK fast-path adds ~3-5% on top of PAC direct — the SDK layer is essentially free.
- **Async clients pay PyO3 + asyncio event-loop scheduling**. The drainer thread eliminates per-batch `Python::attach` churn on Tokio workers; uvloop reduces per-op loop-thread cost. With both, single-loop async tops out around 130K TPS (the asyncio loop thread is now the single-threaded bottleneck, doing per-op `set_result` and task wakeup).
- **AsyncPool with N loops** breaks past the single-loop ceiling by parallelizing the loop work across N Python threads. 4-8 loops scale to 246-273K TPS — equal to or above the production sync ceiling.
- **The chained-builder API pays an additional Python-interpreter cost** on single-key calls — per-op object allocation, validation, and stream-wrap cost. On batch calls, that cost amortizes across keys; at batch=128 the sync builder reaches ~484K TPS — much higher than any single-key cell. Use the fast-path (`session.get`/`session.put`) for single-key dispatch without filters; use the builder with batching for high-throughput bulk workloads.
