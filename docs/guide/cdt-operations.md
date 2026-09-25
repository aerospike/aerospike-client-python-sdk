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

### Joining String Lists

`list_join` concatenates the string items of a list into a single string.
The separator is optional; the list must hold only strings, and an empty
list joins to an empty string.

```python
# ["one", "two", "three"] → "one,two,three"
stream = await (
    session.query(users.id(1))
    .bin("tags").list_join(",")
    .execute()
)

# Join a list nested under a map key
stream = await (
    session.query(users.id(1))
    .bin("profile").on_map_key("nicknames").list_join(", ")
    .execute()
)
```

### String Operations on a Nested Leaf

When a navigated path lands on a string, the `str_*` family is available on
the navigation builder itself: reads such as `str_strlen` and `str_contains`
on either verb, modifies such as `str_append` and `str_upper` on write verbs.
See the nested-strings section of [String Operations](string-ops.md).

```python
# Uppercase one nickname in place
await (
    session.upsert(users.id(1))
    .bin("profile").on_map_key("nicknames").on_list_index(0).str_upper()
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

## Paths: acting on every child at once

The navigation above picks out *one* element. A **path** selects a whole level —
every child, or only those matching a predicate — and applies a single server
operation across the selection.

`on_each_child()` walks every element; `on_each_child_where(pred)` keeps the
matches. The predicate is an `Exp` over the current element's *loop variable*:

```python
from aerospike_sdk import Exp, LoopVarPart

# add 10 to every element of the list
add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
await session.update(key).bin("nums").on_each_child().modify_by(add_10).execute()

# drop just the elements over 5
over_5 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))
await (
    session.update(key).bin("nums").on_each_child_where(over_5).remove_matches().execute()
)
```

Reads use the `collect_*` terminals — named apart from `get_*` because they
return a selection rather than one element:

| Terminal | Returns |
|---|---|
| `collect_values()` | the value of every selected element |
| `collect_map_keys()` | the key of every selected map entry |
| `collect_map_entries()` | selected map entries as key/value pairs |
| `collect_matching_tree()` | the selection, still nested as it was found |

Paths nest, so a predicate can apply a level down:

```python
# every value over 5, in every list under the map
result = await (
    await session.query(key)
    .bin("m").on_each_child().on_each_child_where(over_5).collect_values()
    .execute()
).first_or_raise()
```

### Selecting map entries by key

`on_map_keys_in(keys)` selects the entries of a map whose key is in `keys`; keys
the map does not hold are skipped. It opens a path from a bin, or continues one
after any navigation step. `and_filter(pred)` then narrows that selection with a
predicate over each entry:

```python
# the values under "alpha" and "gamma"
result = await (
    await session.query(key)
    .bin("m").on_map_keys_in(["alpha", "gamma"]).collect_values()
    .execute()
).first_or_raise()

# entries a, b, c whose value is over 10, as key/value pairs
over_10 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10))
result = await (
    await session.query(key)
    .bin("m").on_map_keys_in(["a", "b", "c"]).and_filter(over_10).collect_map_entries()
    .execute()
).first_or_raise()

# add 10 to just the selected entries
await session.update(key).bin("m").on_map_keys_in(["a", "c"]).modify_by(add_10).execute()
```

`and_filter` refines a key selection only, so it must directly follow
`on_map_keys_in`; the builder raises `TypeError` anywhere else. To filter the
children of a collection use `on_each_child_where(pred)`, and to apply several
conditions combine them with `Exp.and_` in one call.

Writes have `modify_by(expr)`, `modify_no_fail(expr)` — which tolerates elements
the expression cannot be applied to — and `remove_matches()`. For explicit
`SelectFlags`, `collect_by_path(flags)` is the unsugared form.

When the shape varies between records, pass `no_fail=True` so a path that does
not resolve yields nothing instead of failing the operation:

```python
# an absent or empty "items" collection is not an error here
.bin("d").on_map_key("items").on_each_child().collect_values(no_fail=True)
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
