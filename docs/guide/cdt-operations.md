# CDT Operations

Complex Data Type (CDT) operations let you read and modify nested lists and maps
within a single bin, server-side, without fetching the whole record.

## Reading CDT Data

Navigate into a bin's structure using `on_list()` and `on_map_key()`:

```python
users = DataSet.of("test", "users")

# Read a map value by key
stream = await (
    session.query(users.id(1))
    .bin("settings").on_map_key("theme").get()
    .execute()
)

# Read a list element by index
stream = await (
    session.query(users.id(1))
    .bin("scores").on_list_index(0).get()
    .execute()
)

# Read a nested value: $.profile.address.city
stream = await (
    session.query(users.id(1))
    .bin("profile").on_map_key("address").on_map_key("city").get()
    .execute()
)
```

### List Ranges

```python
# Get items by index range
stream = await (
    session.query(users.id(1))
    .bin("scores").on_list_index_range(0, 3).get()
    .execute()
)

# Get items by value range
stream = await (
    session.query(users.id(1))
    .bin("scores").on_list_value_range(10, 100).get()
    .execute()
)
```

### Map Ranges

```python
# Get map entries by key range
stream = await (
    session.query(users.id(1))
    .bin("metrics").on_map_key_range("a", "m").get()
    .execute()
)

# Get by rank (top N values)
stream = await (
    session.query(users.id(1))
    .bin("scores").on_map_value_rank_range(-3).get()
    .execute()
)
```

### Collection Metadata

```python
# Get list size
stream = await (
    session.query(users.id(1))
    .bin("scores").on_list().size()
    .execute()
)

# Check if key exists in map
stream = await (
    session.query(users.id(1))
    .bin("settings").on_map_key("theme").exists()
    .execute()
)
```

## Writing CDT Data

### Set a Value

```python
# Set a map key
await (
    session.update(users.id(1))
    .bin("settings").on_map_key("theme").set_to("dark")
    .execute()
)

# Set a list element by index
await (
    session.update(users.id(1))
    .bin("scores").on_list_index(0).set_to(99)
    .execute()
)
```

### Add / Increment

```python
# Increment a map value
await (
    session.update(users.id(1))
    .bin("counters").on_map_key("views").add(1)
    .execute()
)
```

### List Operations

```python
# Append to a list
await (
    session.update(users.id(1))
    .bin("scores").list_append(95)
    .execute()
)

# Add item (insert-sorted for ordered lists)
await (
    session.update(users.id(1))
    .bin("scores").list_add(95)
    .execute()
)

# Append multiple items
await (
    session.update(users.id(1))
    .bin("tags").list_append_items(["python", "aerospike"])
    .execute()
)

# Clear a list
await (
    session.update(users.id(1))
    .bin("scores").list_clear()
    .execute()
)

# Sort a list
await (
    session.update(users.id(1))
    .bin("scores").list_sort()
    .execute()
)
```

### Map Operations

```python
# Upsert map entries
await (
    session.update(users.id(1))
    .bin("settings").map_upsert_items({"theme": "dark", "lang": "en"})
    .execute()
)

# Clear a map
await (
    session.update(users.id(1))
    .bin("settings").map_clear()
    .execute()
)
```

### Key-ordered maps

A map written as a plain `dict` is stored **unordered**. The server sorts the
entries either way, so a read looks identical — but it will not binary-search a
map that was not *declared* ordered, and keyed or range access on one falls back
to a scan. On a large map that is the difference between a lookup and a walk.

Declare the order by wrapping the dict in [`SortedMap`](../api/sorted-map.md):

```python
from aerospike_sdk import SortedMap

await (
    session.upsert(users.id(1))
    .put({"scores": SortedMap({"zoe": 3, "amy": 1})})
    .execute()
)
```

The flag is stored with the record and survives later modification, so it
governs the cost of every subsequent access — by any client — until the map is
rewritten unordered.

`SortedMap` subclasses `dict`, so it behaves as one everywhere, and a
key-ordered map reads back as a `SortedMap` rather than a plain `dict`:

```python
scores = record.bins["scores"]

scores["amy"]                     # 1
scores == {"amy": 1, "zoe": 3}    # True
isinstance(scores, dict)          # True
```

Maps created through the CDT surface take their order from the operation
instead, so `SortedMap` is not needed there:

```python
await (
    session.update(users.id(1))
    .bin("settings").map_upsert_items({"theme": "dark"}, order=MapOrder.KEY_ORDERED)
    .execute()
)
```

### Remove

```python
# Remove a map key
await (
    session.update(users.id(1))
    .bin("settings").on_map_key("deprecated_key").remove()
    .execute()
)

# Remove a list element
await (
    session.update(users.id(1))
    .bin("scores").on_list_index(-1).remove()
    .execute()
)
```

## Nested Navigation

CDT navigation composes — navigate into arbitrarily nested structures:

```python
# $.users_map["alice"].scores[0] = 100
await (
    session.update(dataset.id(1))
    .bin("users_map").on_map_key("alice").on_map_key("scores").on_list_index(0).set_to(100)
    .execute()
)
```

## AEL expressions on CDT

AEL supports CDT paths for filtering. A collection predicate like the ones
below is generally not satisfiable from a secondary index, so it falls back to
a primary-index (full-set) scan — which is rejected by default. Opt in with
`allow_scans_with_where` when the scan is intended:

```python
from aerospike_sdk import QueryHint

# Filter records where the list has more than 5 items
stream = await (
    session.query(users)
    .where("$.scores.count() > 5")
    .with_hint(QueryHint(allow_scans_with_where=True))
    .execute()
)

# Filter on a nested map value
stream = await (
    session.query(users)
    .where('$.settings.["theme"] == "dark"')
    .with_hint(QueryHint(allow_scans_with_where=True))
    .execute()
)
```

A collection index can serve some of these predicates directly; see
[Secondary Indexes](indexes.md).
