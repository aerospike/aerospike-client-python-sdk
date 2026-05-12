# Secondary Indexes

Secondary indexes enable efficient queries on bin values. The SDK
provides both manual index management and automatic index discovery.

## Creating Indexes

```python
users = DataSet.of("test", "users")

# Numeric index
await (
    session.index(users)
    .on_bin("age")
    .named("users_age_idx")
    .numeric()
    .create()
)

# String index
await (
    session.index(users)
    .on_bin("city")
    .named("users_city_idx")
    .string()
    .create()
)

# Collection index (list elements)
from aerospike_async import CollectionIndexType

await (
    session.index(users)
    .on_bin("tags")
    .named("users_tags_idx")
    .collection(CollectionIndexType.LIST)
    .create()
)

# GEO2DSPHERE index (for GeoJSON bins)
places = DataSet.of("test", "places")
await (
    session.index(places)
    .on_bin("loc")
    .named("places_loc_idx")
    .geo2dsphere()
    .create()
)
```

## Dropping Indexes

```python
await session.index(users).named("users_age_idx").drop()
```

## Auto-Index Discovery

The [`IndexesMonitor`](../api/indexes-monitor.md) runs as a background task,
periodically fetching secondary index metadata from the cluster. When you use
`.where()` with an AEL expression, the client automatically generates an optimal
secondary index `Filter` if a matching index exists.

This is transparent — no code changes needed:

```python
# If "users_age_idx" exists on the "age" bin, this query
# automatically uses it as a secondary index filter
stream = await (
    session.query(users)
    .where("$.age > 25")
    .execute()
)
```

### Configuration

The refresh interval defaults to 5 seconds:

```python
client = Client("localhost:3000", index_refresh_interval=2.0)
```

### Explicit Override

Use [`with_index_context()`](../api/query.md) to explicitly provide index
metadata, bypassing auto-discovery:

```python
from aerospike_sdk import IndexContext, Index, IndexTypeEnum

ctx = IndexContext.of("test", [
    Index(
        bin="age",
        index_type=IndexTypeEnum.INTEGER,
        namespace="test",
        name="age_idx",
    ),
])

stream = await (
    session.query(users)
    .with_index_context(ctx)
    .where("$.age > 25")
    .execute()
)
```

### Indexes on Sets

Secondary indexes may be defined on a specific Aerospike set or be cross-set
(no set name). When auto-discovery is on, the SDK scopes filter selection to
the query's set automatically — an index on set `orders` is never used to
plan a filter for a query on set `customers`. Cross-set indexes (those
defined without a set name) remain eligible for any query.

To configure this manually, use [`IndexContext.with_query_set()`](../api/ael-filter-gen.md):

```python
from aerospike_sdk import IndexContext, Index, IndexTypeEnum

ctx = IndexContext.with_query_set(
    "test",
    "customers",  # query set
    [
        Index(bin="age", index_type=IndexTypeEnum.INTEGER,
              namespace="test", set_name="customers"),
        Index(bin="total", index_type=IndexTypeEnum.INTEGER,
              namespace="test", set_name="orders"),  # excluded
    ],
)
```

The `total` index is on `orders` and won't be considered for queries on
`customers`. Only the `age` index is selectable. Pass `query_set=None` (or
omit it via `IndexContext.of`) to disable set-based filtering entirely.

## Query Hints

Influence which index the server uses with [`QueryHint`](../api/query-hint.md):

```python
from aerospike_sdk import QueryHint

# Force a specific index
stream = await (
    session.query(users)
    .where("$.age > 25 and $.city == 'NYC'")
    .with_hint(QueryHint(index_name="users_city_idx"))
    .execute()
)

# Hint by bin name
stream = await (
    session.query(users)
    .where("$.age > 25 and $.city == 'NYC'")
    .with_hint(QueryHint(bin_name="city"))
    .execute()
)
```
