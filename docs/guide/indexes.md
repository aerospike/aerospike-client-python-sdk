# Secondary Indexes

Secondary indexes enable efficient queries on bin values. Create and manage
indexes explicitly; the server selects among them when you run dataset queries
with AEL `.where()` on clusters that support query selection (field 44).

## Creating Indexes

```python
users = DataSet.of("test", "users")

# Integer index. create() returns an IndexTask: the server builds the index
# asynchronously, so wait on it before querying through the index.
task = await (
    session.index(users)
    .on_bin("age")
    .named("users_age_idx")
    .integer()
    .create()
)
await task.wait_till_complete()

# Every create() below returns the same kind of task; the waits are omitted
# here for brevity. See "Waiting for a build".

# String index
await (
    session.index(users)
    .on_bin("city")
    .named("users_city_idx")
    .string()
    .create()
)

# Collection index (list elements). collection() selects the container shape;
# pair it with the element type.
from aerospike_sdk import CollectionIndexType

await (
    session.index(users)
    .on_bin("tags")
    .named("users_tags_idx")
    .string()
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

# Blob index (for bytes bins)
await (
    session.index(users)
    .on_bin("avatar_hash")
    .named("users_avatar_hash_idx")
    .blob()
    .create()
)
```

### One-call form

`create_index()` on the session takes the same facts as positional and keyword
arguments and drives the builder for you. Each argument stands in for one chain
step, so the same rules apply:

```python
from aerospike_sdk import IndexType

task = await session.create_index(users, "users_city_idx", "city", IndexType.STRING)
await task.wait_till_complete()

# Collection and context arguments map to collection() and context().
await session.create_index(
    users, "users_tags_idx", "tags", IndexType.STRING,
    collection_type=CollectionIndexType.LIST,
)

# An expression index passes expression= instead of a bin.
await session.create_index(users, "users_age_ael_idx", index_type=IndexType.INTEGER,
                           expression="$.age + 1")
```

## Namespace-Wide Indexes (No Set)

An index created without a set covers **every record in the namespace**, across
all sets and the null set. Build one from a set-less `DataSet` — pass only the
namespace, or `None` for the set:

```python
everything = DataSet.of("test")          # same as DataSet.of("test", None)

await session.index(everything).on_bin("created").named("ns_created_idx").integer().create()
```

Query through it with the same handle:

```python
stream = await (
    session.query(everything)
    .where("$.created >= %s and $.created <= %s", one_week_ago, now)
    .execute()
)
```

That query returns matching records from every set **and** the null set. A
set-scoped index cannot serve it, because no such index spans more than its own
set.

A set-less `DataSet` means different things to different verbs, because the
server does:

| Used with | Meaning |
|---|---|
| `session.query(...)` | every set in the namespace, plus the null set |
| `session.index(...)` | an index covering the whole namespace |
| `.id()` / `.ids()` | keys in the **null set** — the set records carry when written without one |
| `session.background_task()` | ⚠️ every record in the namespace, every set and the null set |
| `session.truncate(...)` | ⚠️ the **entire namespace**, every set and the null set |

```{warning}
`session.truncate(DataSet.of("test"))` truncates the whole namespace and cannot
be undone, and `session.background_task().delete(DataSet.of("test"))` deletes
every record in it. Name the set unless that is what you intend.
```

## Set Indexes

A set index covers record presence in a set rather than any value in the
records, so it takes no bin, index type, collection variant, or CDT context.
The server uses it to serve set-scoped queries without walking the whole
namespace, and creating one needs only the `sindex-admin` privilege. The
builder must name a set; a namespace-wide set index is rejected.

```python
task = await (
    session.index(users)
    .on_set()
    .named("users_set_idx")
    .create()
)
await task.wait_till_complete()
```

`on_set()` is mutually exclusive with `on_bin()` and `on_expression()`.
Dropping a set index is the same `drop()` call as any other index. In the
one-call form, a dataset and a name with nothing else is a set index:

```python
await session.create_index(users, "users_set_idx")
```

## Expression-Based Indexes

An index can cover the value an expression computes per record instead of a
plain bin. Replace `on_bin()` with `on_expression()`
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
    .integer()
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

`on_expression()` also accepts an AEL string. The client
sends the string as-is and the server parses and compiles it when the index
is created, so the AEL dialect is the server's:

```python
from aerospike_sdk import Exp

ael = "$.age + 1"

await (
    session.index(users)
    .on_expression(ael)
    .named("users_age_ael_idx")
    .integer()
    .create()
)

# Query through it with the same AEL, server-compiled on the filter:
flt = Filter.range("age", 26, 41).expression(
    Exp.from_server_compiled_ael(ael),
)
stream = await session.query(users).filter(flt).execute()
```

The same rules apply as for prebuilt expressions: the AEL must produce a
value of the index's type, so a boolean predicate like `"$.age > 21"` is
rejected by the server. While any node runs a build older than 8.2.0 (during
a rolling upgrade, for example), `create()` raises with result code
`OP_NOT_APPLICABLE`.

### Indexing only some records (sparse indexes)

A record whose expression evaluates to `unknown` — equivalently `error` — is
left out of the index. That is the mechanism for indexing a *subset* of a set:
return a value for the records worth indexing, and `unknown` for the rest.

```python
# Index adults in selected countries on their age; skip every other record.
ael = (
    "when ($.age >= 18 and $.country in ['Australia', 'Canada', 'USA'] => $.age, "
    "default => unknown)"
)

await (
    session.index(users)
    .on_expression(ael)
    .named("users_adult_age_idx")
    .integer()
    .create()
)
```

The index then holds only the matching records, so it stays smaller and a query
through it never has to consider the rest. Records excluded this way are not
errors — nothing fails, they simply do not appear in the index, and a query
served by it will not return them even if they would satisfy the filter.

## Waiting for an index to build

`create()` returns as soon as the server accepts the request; the index is built
in the background. Querying through an index that is still building can miss
records that are already written, so wait on the returned task first:

```python
task = await session.index(users).on_bin("age").named("users_age_idx").integer().create()
await task.wait_till_complete()          # raises TimeoutError past the budget
await task.wait_till_complete(timeout=None)   # or wait indefinitely
```

The synchronous builder returns the same task; call
`wait_till_complete_blocking()` on it. `wait_till_complete` takes a `timeout` in
seconds (default 60) and raises `TimeoutError` if the build has not finished by
then — pass `timeout=None` to wait as long as it takes.

## Dropping Indexes

```python
task = await session.index(users).named("users_age_idx").drop()
await task.wait_till_complete()

# Or in one call:
task = await session.drop_index(users, "users_age_idx")
```

## Listing Indexes

`list_indexes()` returns the secondary indexes defined on the cluster, one dict
per index with `namespace`, `set`, `bin` and `name` keys (plus `type`,
`index_type`, and `context` for CDT indexes when the server reports them). A set
index lists with an empty `bin`, no `type`, and `index_type` of `set`. It is
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

### Choosing the access path yourself

To bypass server-led selection, attach an explicit index filter. It is sent
unchanged as the access path, and any `.where()` clause travels beside it as a
residual filter expression:

```python
stream = await (
    session.query(users)
    .filter(Filter.range("age", 25, 40))
    .where("$.city == 'NYC'")
    .execute()
)
```

See the [AEL guide](expression-ael.md) for string filter syntax and capability
checks (`cluster.supports_ael()`, `cluster.supports_query_selection()`).
