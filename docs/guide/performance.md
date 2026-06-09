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

## Performance summary table

Numbers from the [Benchmarking Guide](benchmarking.md) — 8-vCPU isolated client VM → 8-vCPU isolated server VM over a low-latency private network, 100K keys, 50/50 RW, 50-byte payload.

### Single-key dispatch (batch size 1)

| Mode | Threads / Tasks | Free-threaded TPS | Non-FT TPS |
|---|---|---|---|
| Sync fast-path (`session.get`/`put`) | 32 | **~200K** | ~45K |
| Sync builder (`session.query(k).execute()`) | 32 | ~116K | ~22K |
| Async fast-path, single client | 32 tasks | ~93K | ~64K |
| Async fast-path, AsyncPool 4×64 | 256 tasks | **~149K** | ~48K (slower than single-loop) |
| Async builder, single client | 32 tasks | ~38K | ~31K |
| Async builder, AsyncPool 4×64 | 256 tasks | ~101K | ~25K (slower than single-loop) |

### With batching (`--batch-size > 1`, free-threaded)

When the workload can group keys per call, the chained-builder API amortizes its per-op overhead and surpasses every single-key number above.

| Mode | Batch size | Peak TPS |
|---|---|---|
| **Sync builder** | 128 | **~495K** |
| AsyncPool builder, 4×64 | 64 | ~325K |
| Async single-loop builder, 32 tasks | 128 | ~186K |

**Practical reading:**
- If your workload can batch keys, the **sync builder with `session.batch()` or multi-key `session.query([keys])`** is the highest-throughput mode — scales monotonically to ~495K TPS at batch=128, **75% above Rust-core async direct (~283K)**. Doubling the batch size keeps amortizing the per-call cost.
- For single-key workloads, the **sync fast-path** (~200K) is the highest mode. If you need async, **AsyncPool fast-path** scales to ~149K.
- On regular Python (GIL on), *async single-client fast-path* (~64K) is the simplest high-throughput mode; sync fast-path (~45K) is slightly lower because of GIL contention across the 32 worker threads.

## Why sync and async perform so differently

The cost stacks for sync and async are not the same. From the [benchmarking guide](benchmarking.md)'s stack analysis:

- **Sync clients pay only the PyO3 boundary cost** (~13%). The SDK layer on top of PAC adds ~5%. PSDK sync builder routes through PAC's `_blocking` entries directly — no asyncio loop in the path.
- **Async clients pay PyO3 + asyncio event-loop scheduling + a Tokio worker bounce on each op** — roughly a 65% drop vs Rust async direct. Every async op crosses Tokio ↔ asyncio twice (submit, then complete), which is the fundamental cost of bridging two async runtimes. `AsyncPool` recovers some of that by running multiple event loops on multiple OS threads in parallel, but only on free-threaded Python.
- **The chained-builder API pays an additional Python-interpreter cost** on single-key calls — per-op object allocation, validation, and stream-wrap cost. On batch calls, that cost amortizes across keys; at batch=128 the sync builder reaches ~495K TPS — *75% above* Rust-core async direct (~283K) and the highest single-loop number in the matrix. Use the fast-path (`session.get`/`session.put`) for single-key dispatch without filters; use the builder with batching for high-throughput bulk workloads.
