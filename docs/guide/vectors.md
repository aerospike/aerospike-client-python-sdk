# Vectors and Top-K Queries

A vector bin stores a fixed-length array of numbers, such as an embedding.
Expressions compute the distance between a stored vector and a query vector,
and a Top-K query ranks a dataset query by a bin and returns only the best
`k` records. Both need every node on Aerospike Server 8.2.1 or later; check
with `cluster.supports_vector()` and `cluster.supports_query_top_k()`.

## Storing Vectors

Write a {class}`~aerospike_sdk.Vector` like any other bin value. It reads back
as a `Vector` with the same values and element type.

```python
from aerospike_sdk import DataSet, OperationResult, Vector, VectorElementType

products = DataSet.of("test", "products")
key = products.id("sku-42")

await session.upsert(key).put({
    "category": "shoes",
    "embedding": Vector([0.12, 0.88, 0.47]),
}).execute()

stream = await session.query(key).execute()
record = (await stream.first_or_raise()).record_or_raise()
embedding = OperationResult(record.bins["embedding"]).get_vector()
```

The element type defaults to `VectorElementType.FLOAT32`. The others are
`FLOAT16`, `FLOAT64`, and `INT32`, which takes Python `int` values:

```python
Vector([3, -1, 7], VectorElementType.INT32)
```

A list of floats is a list, not a vector: the server stores the two as
different bin types, and only a vector bin works with the distance
expressions below. A vector must be a top-level bin value. The server
refuses a write that puts one inside a list or map with
`ResultCode.PARAMETER_ERROR`.

## Vector Distance

`Exp.vector_distance(metric, query, bin)` returns the distance between
`query` and the vector in `bin` as a float. Each metric has its own
direction for "nearest":

| Metric | Nearest is | Rank with |
|--------|-----------|-----------|
| `VectorDistanceMetric.EUCLIDEAN_SQUARED` | smallest | `Order.ASC` |
| `VectorDistanceMetric.COSINE_SIMILARITY` | largest | `Order.DESC` |
| `VectorDistanceMetric.DOT_PRODUCT` | largest | `Order.DESC` |

A stored value that is not a vector, or whose element type or dimension
count differs from `query`, evaluates to unknown. As a filter, the record
does not match:

```python
from aerospike_sdk import Exp, Vector, VectorDistanceMetric

probe = Vector([0.12, 0.88, 0.47])
close = Exp.lt(
    Exp.vector_distance(
        VectorDistanceMetric.EUCLIDEAN_SQUARED, probe, Exp.vector_bin("embedding"),
    ),
    Exp.float_val(0.25),
)
stream = await session.query(products).where(close).execute()
```

Vector distance is not part of the AEL string syntax, so pass it to
`where()` as an `Exp`.

To return the distance itself, project it into a bin of the returned record
with `select_from`:

```python
similarity = Exp.vector_distance(
    VectorDistanceMetric.COSINE_SIMILARITY, probe, Exp.vector_bin("embedding"),
)
stream = await (
    session.query(products)
    .bin("similarity").select_from(similarity, ignore_eval_failure=True)
    .execute()
)
```

Without `ignore_eval_failure=True`, a record whose distance is unknown is
left out of the results. With it, the record comes back with no value in
the projected bin.

A projection returns only the bins the query reads, and `bins([...])` does
not combine with `.bin(...)` reads. To return another bin next to the
distance, read it in the same chain: `.bin("sku").get()`.

## Top-K Queries

`order_by(bin_name, data_type, order)` names the bin to rank by and
`top_k(k)` how many records to return. Every node ranks its own matches and
the client merges them, so the stream holds at most `k` records, best first.
Ranking runs after the index filter and `where()`: it decides which matches
come back, not which records match.

A hybrid query filters on ordinary bins and ranks by vector similarity:

```python
from aerospike_sdk import BinDataType, Order

stream = await (
    session.query(products)
    .where("$.category == 'shoes' and $.in_stock == true")
    .bin("similarity").select_from(similarity, ignore_eval_failure=True)
    .order_by("similarity", BinDataType.DOUBLE, Order.DESC)
    .top_k(10)
    .execute()
)
async for row in stream:
    print(row.record_or_raise().bins["similarity"])
```

The rules for order keys:

- The key names a bin of the returned record: one listed in `bins()`, or
  one a `.bin(...)` read returns, like the `similarity` bin above.
- `data_type` is `BinDataType.INTEGER`, `BinDataType.DOUBLE`, or
  `BinDataType.BYTES`. Bytes compare as unsigned bytes, and a value ranks
  before any longer value it is a prefix of.
- A record whose key bin is missing, or holds another type, ranks last in
  either direction. That includes a record whose projected distance is
  unknown.
- `then_order_by` adds a tie-breaker, compared only when the first key ties.
  A query takes at most two keys, on different bins.
- Calling `order_by` again replaces every key set before.

`k` runs from 1 to 1000. Because `k` is the whole result's size, a Top-K
query refuses `limit()`, `chunk_size()`, and `with_no_bins()` with
`ResultCode.PARAMETER_ERROR`, and so does a key query or a cluster with a
node that cannot rank. The sync builder has the same methods.

### Timeouts on Large Sets

A node ranks every match before it sends its first record, so the first
socket read waits for that node's whole share of the set. On a large set,
raise the query socket timeout:

```python
from datetime import timedelta

from aerospike_sdk import Behavior
from aerospike_sdk.policy import Settings

ranked = Behavior.DEFAULT.derive_with_changes(
    "ranked-queries",
    reads_query=Settings(socket_timeout=timedelta(minutes=2)),
)
session = cluster.create_session(ranked)
```

## Memory Pressure

A vector bin holds a 6-byte header plus its elements, so a 1536-dimension
`FLOAT32` vector takes about 6 KB per record, and `FLOAT16` halves that.
When a node reaches its stop-writes threshold, it refuses writes to the
records whose master copy it holds, vector writes included, and the client
raises {class}`~aerospike_sdk.CapacityError` with
`ResultCode.SERVER_MEM_ERROR`. Writes to records mastered on other nodes
still succeed, even when their replica lives on the stopped node, so a
single node in stop-writes fails only part of an ingestion run. The client
does not retry the error: the write is refused until memory is freed or
capacity is added.

Reads keep working under stop-writes, and so do deletes, dataset queries,
and Top-K queries. Deleting records is one way to free memory.

```python
from aerospike_sdk import CapacityError, ResultCode

try:
    await session.upsert(key).put({"embedding": Vector(values)}).execute()
except CapacityError as e:
    if e.result_code == ResultCode.SERVER_MEM_ERROR:
        ...  # stop or slow ingestion; retry after memory is freed
    else:
        raise  # another capacity limit
```

`CapacityError` also covers device overload, a busy key
({class}`~aerospike_sdk.exceptions.KeyBusyError`), full query and batch
queues, and a batch over the server's request limit, so check
`result_code` before treating the error as out of memory.

A Top-K query keeps up to `k` records with every bin the query returns.
Each node sends its own best `k`, so the client receives up to `k` records
per node before it keeps the best `k` overall. Return the distance and
leave out the vector bin unless the caller needs it: a 128-dimension
`FLOAT32` vector adds 518 bytes to every returned record, while the
distance adds an 8-byte double plus its bin name.
