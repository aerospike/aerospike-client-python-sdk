# Secondary Indexes

Secondary indexes enable efficient queries on bin values. Create and manage
indexes explicitly; the server selects among them when you run dataset queries
with AEL `.where()` on clusters that support query selection (field 44).

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
from aerospike_sdk import CollectionIndexType

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

# Blob index (for bytes bins; server 7.0+)
await (
    session.index(users)
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
    session.index(users)
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

## Dropping Indexes

```python
await session.index(users).named("users_age_idx").drop()
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
