# Reading Data

All reads go through `session.query()`, which returns a
[`QueryBuilder`](../api/query.md). Chain methods to configure the query, then
call `.execute()` to get a [`RecordStream`](../api/record-stream.md).

## Point Read (Single Key)

```python
users = DataSet.of("test", "users")

stream = await session.query(users.id(1)).execute()
result = await stream.first_or_raise()
print(result.record.bins)  # {'name': 'Alice', 'age': 30}
```

## Set Scan (All Records)

```python
stream = await session.query(users).execute()
async for result in stream:
    print(result.record.bins)
stream.close()
```

## Batch Read (Multiple Keys)

```python
stream = await session.query(*users.ids(1, 2, 3)).execute()
async for result in stream:
    print(result.record.key, result.record.bins)
stream.close()
```

## Buffered vs. Lazy Delivery

Every query-path builder exposes two terminals with different result-delivery
semantics. **`execute()` is the default** — reach for `execute_stream()` only
when a large result set makes buffering expensive.

- **`execute()`** — buffered. Awaits every result, then returns a
  `RecordStream` backed by a fully-materialized list. Results arrive in input
  order, and for a chain that includes writes those writes are guaranteed
  complete server-side by the time the call returns. Use this for most
  workloads.

- **`execute_stream()`** — lazy. Returns a `RecordStream` that yields one
  `RecordResult` per `__anext__` (`__next__` on sync) as the cluster responds.
  The first record arrives at first-RTT rather than after all keys complete, so
  peak memory stays bounded — useful for large batches and scans.

  Caveats:

  - **Yields completion order, not input order.** Each `RecordResult` carries
    its input position in `.index`; sort after collecting if you need
    positional order.
  - **No writes-complete-on-return guarantee.** If a chain includes writes and
    you discard the stream without iterating, per-node work may still be
    in-flight. Use `execute()` when you need writes done on return.
  - **Per-key errors land inline** on `RecordResult`; cluster-level errors
    raise mid-iteration.

```python
# Bounded-memory scan — process records as they arrive
stream = await session.query(users).execute_stream()
async for result in stream:
    handle(result.record)
stream.close()
```

Keyless dataset queries and scans already stream lazily from the server, so
`execute_stream()` and `execute()` deliver the same incremental behavior there;
the distinction matters most for multi-key batch shapes. The same two terminals
exist on `session.batch()` and the chained write builders — see
[Writing Data](#execute-vs-execute_stream). Sync builders have the
identical contract; iterate with `for result in stream`.

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
from aerospike_async import QueryDuration

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
