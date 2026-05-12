# Writing Data

All writes go through session entry points that return a
[`WriteSegmentBuilder`](../api/write-segment.md). Chain bin operations, then
call `.execute()`.

## Write Verbs

| Method | Behavior | Record must exist? |
|--------|----------|--------------------|
| `upsert()` | Create or update | No |
| `insert()` | Create only | No (fails if exists) |
| `update()` | Update only | Yes (fails if missing) |
| `replace()` | Replace all bins | Yes (fails if missing) |
| `delete()` | Remove record | No |
| `touch()` | Reset TTL | Yes |
| `exists()` | Check existence | No |

## Bin Chaining

Set individual bins with the chainable `.bin().set_to()` pattern:

```python
users = DataSet.of("test", "users")

await (
    session.upsert(users.id(1))
    .bin("name").set_to("Alice")
    .bin("age").set_to(30)
    .bin("active").set_to(True)
    .execute()
)
```

## Dict Pattern

Set multiple bins at once with `.put()`:

```python
await (
    session.upsert(users.id(1))
    .put({"name": "Alice", "age": 30, "active": True})
    .execute()
)
```

## Increment

```python
await (
    session.update(users.id(1))
    .bin("login_count").increment_by(1)
    .execute()
)
```

## GeoJSON Bins

Use `set_to_geo_json(...)` to write a bin as a GeoJSON value. The bin's
server-side particle type is GEOJSON, not STRING, which makes it eligible for
GEO2DSPHERE indexing and `geoCompare(...)` queries.

```python
places = DataSet.of("test", "places")

await (
    session.upsert(places.id("space_needle"))
    .bin("loc").set_to_geo_json('{"type":"Point","coordinates":[-122.349,47.620]}')
    .execute()
)
```

AeroCircle and Polygon values use the same method — only the GeoJSON string
differs.

## Insert (Fail if Exists)

```python
stream = await (
    session.insert(users.id(99))
    .put({"name": "New User"})
    .execute()
)
result = await stream.first_or_raise()
```

## Replace (Overwrite All Bins)

```python
await (
    session.replace(users.id(1))
    .put({"name": "Alice Updated"})
    .execute()
)
# Only "name" bin remains; "age" and "active" are removed
```

## Delete

```python
# Single key
await session.delete(users.id(1)).execute()

# Multiple keys
await session.delete(*users.ids(1, 2, 3)).execute()
```

### Durable delete

Aerospike supports two delete modes:

* a normal delete that removes the record (and its lineage) outright, and
* a *durable* delete that leaves a tombstone so a strongly-consistent (SC)
  cluster can resolve the deletion across partitions.

The SDK exposes both *per-operation overrides* and *builder defaults*:

| Method | Scope | Effect |
|---|---|---|
| `with_durable_delete()` | one operation | Force durable delete for this delete only |
| `without_durable_delete()` | one operation | Force a non-durable delete for this delete only |
| `default_with_durable_delete()` | builder | Prefer durable when resolving Behavior defaults — typical for SC namespaces |
| `default_without_durable_delete()` | builder | Prefer non-durable when resolving Behavior defaults |

The override (`with_*` / `without_*`) wins over the default; the default
folds into [`Behavior`](../api/behavior.md) settings resolution.

```python
# Force durable on this single delete (per-op override)
await session.delete(users.id(5)).with_durable_delete().execute()

# Use durable as the default for every delete in this segment (SC-friendly)
await (
    session.delete(*users.ids(1, 2, 3))
    .default_with_durable_delete()
    .execute()
)
```

## Conditional Writes

Filter writes server-side with `.where()`:

```python
await (
    session.update(users.id(1))
    .where("$.age >= 18")
    .bin("verified").set_to(True)
    .execute()
)
```

Records that don't match the filter are skipped. Use `.fail_on_filtered_out()`
to raise an error instead:

```python
await (
    session.update(users.id(1))
    .where("$.age >= 18")
    .fail_on_filtered_out()
    .bin("verified").set_to(True)
    .execute()
)
```

## Generation Check (Optimistic Locking)

```python
await (
    session.update(users.id(1))
    .ensure_generation_is(5)
    .bin("balance").set_to(100)
    .execute()
)
```

## TTL / Expiration

```python
await (
    session.upsert(users.id(1))
    .expire_record_after_seconds(3600)
    .put({"session_token": "abc123"})
    .execute()
)
```

## Batch Writes

Multiple keys with the same operation:

```python
await (
    session.upsert(*users.ids(1, 2, 3))
    .bin("status").set_to("migrated")
    .execute()
)
```

Mixed operations across different keys are handled automatically when you chain
multiple write segments.
