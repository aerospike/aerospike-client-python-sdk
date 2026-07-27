# Reading Data

All reads go through `session.query()`, which returns a
[`QueryBuilder`](../api/query.md). Chain methods to configure the query, then
call `.execute()` to get a [`RecordStream`](../api/record-stream.md).

## Point Read (Single Key)

```python
users = DataSet.of("test", "users")

stream = await session.query(users.id(1)).execute()
result = await stream.first_or_raise()   # takes one row, then closes the stream
print(result.record.bins)  # {'name': 'Alice', 'age': 30}
```

`first_or_raise()` is **terminal** — it returns the first row and closes the
stream (releasing the producer), which is exactly what you want for a point read.
See [Taking a Single Row](#single-row-access) for the full set of single-row
accessors.

## Set Scan (All Records)

```python
async with await session.query(users).execute() as stream:
    async for result in stream:
        print(result.record.bins)
```

## Batch Read (Multiple Keys)

```python
async with await session.query(*users.ids(1, 2, 3)).execute() as stream:
    async for result in stream:
        print(result.record.key, result.record.bins)
```

Draining a stream fully releases it automatically; the `async with` above also
guarantees release if you leave the loop early. See
[Closing a Stream](#closing-streams).

## Buffered vs. Lazy Delivery

Every query-path builder exposes two terminals with different result-delivery
semantics. **`execute()` is the default** — reach for `stream()` only
when a large result set makes buffering expensive.

- **`execute()`** — buffered. Awaits every result, then returns a
  `RecordStream` backed by a fully-materialized list. Results arrive in input
  order, and for a chain that includes writes those writes are guaranteed
  complete server-side by the time the call returns. Use this for most
  workloads.

- **`stream()`** — lazy. Returns a `RecordStream` that yields one
  `RecordResult` per `__anext__` (`__next__` on sync) as each node responds.
  The first results are available as soon as the first node responds, without
  waiting for the rest, so peak memory stays bounded to the in-flight node
  responses — useful for large batches and scans.

  Caveats:

  - **Yields completion order, not input order.** Each `RecordResult` carries
    its input position in `.index`; sort after collecting if you need
    positional order.
  - **No writes-complete-on-return guarantee.** If a chain includes writes and
    you discard the stream without iterating, per-node work may still be
    in-flight. Use `execute()` when you need writes done on return.
  - **Per-key errors land inline** on `RecordResult`; cluster-level errors
    raise mid-iteration.
  - **Close it if you abandon it early** (see below).

```python
# Bounded-memory scan — process records as they arrive
async with await session.query(users).stream() as stream:
    async for result in stream:
        handle(result.record)
```

Keyless dataset queries and scans already stream lazily from the server, so
`stream()` and `execute()` deliver the same incremental behavior there;
the distinction matters most for multi-key batch shapes. The same two terminals
exist on the chained write builders — see
[Writing Data](#execute-vs-stream). Sync builders have the
identical contract; iterate with `for result in stream`.

(closing-streams)=
### Closing a Stream

A `RecordStream` that is drained to exhaustion releases its underlying producer
automatically. If you **abandon a lazy stream early** — `break` out of the loop,
take only the first row, or hit an exception — call `close()` (or use the
context manager) so the producer is released promptly instead of at
garbage-collection time.

`close()`:

- stops further iteration (subsequent iteration ends immediately), and
- releases the underlying producer: for a batch stream it drops the receiver and
  any buffered-but-unconsumed results; for a scan it tears down the server-side
  query. In-flight per-node requests still complete in the background and
  release their connections as they finish — `close()` reclaims the consumer
  side. It is idempotent.

The recommended pattern is the context manager, which closes on **every** exit
path — normal completion, early `break`, or exception:

```python
# async — `async with`
async with await session.query(*users.ids(1, 2, 3)).stream() as stream:
    async for result in stream:
        if done(result):
            break            # close() still runs on the way out

# sync — `with`
with session.query(*users.ids(1, 2, 3)).stream() as stream:
    for result in stream:
        ...
```

A buffered `execute()` stream over a single key or a materialized list holds no
external producer, so closing it is a harmless no-op — but the context-manager
habit is safe to apply uniformly.

(single-row-access)=
### Taking a Single Row

Four accessors take a single row. They split on two independent axes —
**closing** (does it release the stream afterward?) and **error handling**
(return a non-OK `RecordResult` as data, or raise):

|                | keeps stream open | closes stream (terminal) |
| -------------- | ----------------- | ------------------------ |
| **returns envelope** | `pop()`          | `first()`               |
| **raises on error**  | `pop_or_raise()` | `first_or_raise()`      |

- **`first()` / `first_or_raise()`** — take one row, then `close()` the stream.
  Use these for a point read or any "I want exactly one record" case; the
  producer is released immediately.
- **`pop()` / `pop_or_raise()`** — take one row and leave the rest available for
  further iteration. Use these to peek at a head element and keep going.
- The `_or_raise` variants raise on an empty stream and on a non-OK row (via
  `RecordResult.or_raise()`); the plain variants return `None` for empty and
  hand back non-OK rows as `RecordResult` data (`is_ok=False`) so you can
  branch on partial success.

```python
# Point read — first_or_raise takes one and closes
rec = await (await session.query(users.id(1)).execute()).first_or_raise()

# Peek the head, then process the remainder — pop keeps the stream open
stream = await session.query(*users.ids(1, 2, 3)).stream()
head = await stream.pop()
async for rest in stream:
    ...
stream.close()   # or wrap the whole thing in `async with`
```

Sync builders expose the same four methods; call them without `await`.

## Selecting Bins

Return only specific bins to reduce network transfer:

```python
stream = await session.query(users).bins(["name", "age"]).execute()
```

Or exclude all bins (metadata only):

```python
stream = await session.query(users).with_no_bins().execute()
```

## Filtering with AEL

Use the [Aerospike Expression Language](expression-ael.md) to filter records server-side:

```python
stream = await (
    session.query(users)
    .where("$.age > 25 and $.status == 'active'")
    .execute()
)
```

Or with a pre-built `FilterExpression`:

```python
from aerospike_sdk import Exp

expr = Exp.and_([
    Exp.gt(Exp.int_bin("age"), Exp.int_val(25)),
    Exp.eq(Exp.string_bin("status"), Exp.string_val("active")),
])
stream = await session.query(users).where(expr).execute()
```

## Partition Filtering

Query specific partitions for parallel consumption:

```python
stream = await (
    session.query(users)
    .on_partitions(0, 1, 2)
    .execute()
)
```

Or a contiguous range:

```python
stream = await (
    session.query(users)
    .on_partition_range(begin=0, count=1024)
    .execute()
)
```

## Query Policies

Fine-tune query behavior:

```python
from aerospike_sdk import QueryDuration

stream = await (
    session.query(users)
    .where("$.age > 18")
    .expected_duration(QueryDuration.LONG)
    .chunk_size(500)
    .execute()
)
```

## RecordResult

Each item in the stream is a [`RecordResult`](../api/record-result.md):

```python
async for result in stream:
    if result.is_ok:
        record = result.record
        print(record.key, record.bins, record.generation, record.expiration)
    else:
        print(f"Error: {result.result_code}")
```

Use `record_or_raise()` to raise on error results:

```python
async for result in stream:
    record = result.record_or_raise()
```

## Query Hints

Influence secondary index selection with [`QueryHint`](../api/query-hint.md):

```python
from aerospike_sdk import QueryHint

stream = await (
    session.query(users)
    .where("$.age > 25")
    .with_hint(QueryHint(index_name="age_idx"))
    .execute()
)
```
