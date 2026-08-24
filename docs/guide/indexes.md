# Secondary Indexes

Secondary indexes enable efficient queries on bin values. Create and manage
indexes explicitly; the server selects among them when you run dataset queries
with AEL `.where()` on clusters that support query selection (field 44).

## Creating Indexes

```python
users = DataSet.of("test", "users")

# Numeric index. create() returns an IndexTask: the server builds the index
# asynchronously, so wait on it before querying through the index.
task = await (
    session.index(dataset=users)
    .on_bin("age")
    .named("users_age_idx")
    .numeric()
    .create()
)
await task.wait_till_complete()

# Every create() below returns the same kind of task; the waits are omitted
# here for brevity. See "Waiting for a build".

# String index
await (
    session.index(dataset=users)
    .on_bin("city")
    .named("users_city_idx")
    .string()
    .create()
)

# Collection index (list elements). collection() selects the container shape;
# pair it with the element type.
from aerospike_sdk import CollectionIndexType

await (
    session.index(dataset=users)
    .on_bin("tags")
    .named("users_tags_idx")
    .string()
    .collection(CollectionIndexType.LIST)
    .create()
)

# GEO2DSPHERE index (for GeoJSON bins)
places = DataSet.of("test", "places")
await (
    session.index(dataset=places)
    .on_bin("loc")
    .named("places_loc_idx")
    .geo2dsphere()
    .create()
)

# Blob index (for bytes bins; server 7.0+)
await (
    session.index(dataset=users)
    .on_bin("avatar_hash")
    .named("users_avatar_hash_idx")
    .blob()
    .create()
)
```

## Expression-Based Indexes

On server 8.1.2+, an index can cover the value an expression computes per
record instead of a plain bin. Replace `on_bin()` with `on_expression()`
(they are mutually exclusive). The expression's result type must match the
index type — index a value-producing expression, not a boolean predicate:

```python
from aerospike_sdk import Exp, Filter

# Index the value of the "age" bin computed through an expression
expr = Exp.int_bin("age")

await (
    session.index(dataset=users)
    .on_expression(expr)
    .named("users_age_exp_idx")
    .numeric()
    .create()
)
```

To query through an expression index, attach the same expression to the
filter:

```python
flt = Filter.range("age", 25, 40).expression(expr)
stream = await session.query(users).filter(flt).execute()
```

`context()` is not supported with expression indexes — encode CDT
navigation inside the expression instead.

### From an AEL string

On server 8.1.3+, `on_expression()` also accepts an AEL string. The client
sends the string as-is and the server parses and compiles it when the index
is created, so the AEL dialect is the server's:

```python
from aerospike_async import FilterExpression

ael = "$.age + 1"

await (
    session.index(dataset=users)
    .on_expression(ael)
    .named("users_age_ael_idx")
    .numeric()
    .create()
)

# Query through it with the same AEL, server-compiled on the filter:
flt = Filter.range("age", 26, 41).expression(
    FilterExpression.from_server_compiled_ael(ael),
)
stream = await session.query(users).filter(flt).execute()
```

The same rules apply as for prebuilt expressions: the AEL must produce a
value of the index's type, so a boolean predicate like `"$.age > 21"` is
rejected by the server. If any node is older than 8.1.3, `create()` raises
with result code `OP_NOT_APPLICABLE` — build the expression with `Exp`
instead on those clusters.

## Waiting for an index to build

`create()` returns as soon as the server accepts the request; the index is built
in the background. Querying through an index that is still building can miss
records that are already written, so wait on the returned task first:

```python
task = await session.index(dataset=users).on_bin("age").named("users_age_idx").numeric().create()
await task.wait_till_complete()          # raises TimeoutError past the budget
await task.wait_till_complete(timeout=None)   # or wait indefinitely
```

The synchronous builder returns the same task; call
`wait_till_complete_blocking()` on it. `wait_till_complete` takes a `timeout` in
seconds (default 60) and raises `TimeoutError` if the build has not finished by
then — pass `timeout=None` to wait as long as it takes.

## Dropping Indexes

```python
task = await session.index(dataset=users).named("users_age_idx").drop()
await task.wait_till_complete()
```

## Listing Indexes

`list_indexes()` returns the secondary indexes defined on the cluster, one dict
per index with `namespace`, `set`, `bin` and `name` keys (plus `type`,
`index_type`, and `context` for CDT indexes when the server reports them). It is
available on the session, cluster, and client:

```python
for idx in await session.list_indexes():
    print(idx["name"], idx["namespace"], idx["bin"])
```

## Query hints

On clusters with **query selection** (field 44), the server chooses which index
to use when you pass an AEL string to `.where()`. Influence that choice with
[`QueryHint`](../api/query-hint.md):

```python
from aerospike_sdk import QueryHint

# Force a specific index
stream = await (
    session.query(users)
    .where("$.age > 25 and $.city == 'NYC'")
    .with_hint(QueryHint(index_name="users_city_idx"))
    .execute()
)
```

### Blocking primary-index (full-set) scans

By default a `.where()` query that no secondary index can satisfy is **rejected**
rather than allowed to fall back to a primary-index (full-set) scan — a full-set
scan is dangerous at scale. This is the `allow_scans_with_where` query setting,
which defaults to `False` in `Behavior.DEFAULT`. Queries **without** a `.where()`
clause (intentional scans) are unaffected, and this only applies on clusters with
query selection (field 44).

Allow the fallback per query with a hint, or change it on the `Behavior`:

```python
# Permit the primary-index fallback for this one query
stream = await (
    session.query(users)
    .where("$.age > 25")
    .with_hint(QueryHint(allow_scans_with_where=True))
    .execute()
)
```

`QueryHint.allow_scans_with_where` is tri-state: `None` (default) inherits the
`Behavior`, `True` permits the fallback, `False` rejects it. A per-query hint
always wins over the `Behavior` setting.

### Opting out of server-led selection

`bin_name` skips the server's explain step and sends the AEL as a plain filter
expression instead. It is mutually exclusive with `index_name`:

```python
stream = await (
    session.query(users)
    .where("$.age > 25")
    .with_hint(QueryHint(bin_name="age"))
    .execute()
)
```

!!! warning "Deprecated in alpha"
    `bin_name` is a legacy opt-out during alpha, and the bin name itself is not
    sent to the server — it only selects the route. The Query Optimizer PRD
    specifies the index *name* as the sole hint shape, so this is expected to
    be removed. To bypass the planner deliberately, prefer an explicit
    `.filter(...)`, which the server honors when the index is available.

See the [AEL guide](expression-ael.md) for string filter syntax and capability
checks (`cluster.supports_ael()`, `cluster.supports_query_selection()`).
