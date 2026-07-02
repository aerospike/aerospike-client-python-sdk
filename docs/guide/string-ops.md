# String Operations

Server-side string operations let you compute and mutate UTF-8 string values
in a bin without fetching the record. All character indexes are codepoint
indexes — not byte indexes — so the operations are Unicode-aware, matching
the canonical particle semantics on the server.

**Server requirement**: 8.1.3 or later. Older servers reject the
``STRING_READ`` / ``STRING_MODIFY`` op types at the wire layer.

## Reading String Properties

Read-shaped operations chain like any other read; each call records a
projection on the bin without changing its value.

```python
profile = DataSet.of("test", "profile")
key = profile.id("user-42")
await session.upsert(key).bin("name").set_to("hello world").execute()

# Codepoint length
stream = await session.query(key).bin("name").str_strlen().execute()
assert (await stream.first_or_raise()).record_or_raise().bins["name"] == 11

# Substring (start, end), end-exclusive
stream = await session.query(key).bin("name").str_substr(0, 5).execute()
assert (await stream.first_or_raise()).record_or_raise().bins["name"] == "hello"

# Find — returns the codepoint index, or -1 when absent
stream = await session.query(key).bin("name").str_find("world").execute()
assert (await stream.first_or_raise()).record_or_raise().bins["name"] == 6
```

### Predicate Reads

Several reads return booleans suitable for filtering or guarding subsequent ops.

```python
await session.upsert(key).bin("email").set_to("alice@example.com").execute()

stream = await session.query(key).bin("email").str_contains("@").execute()
assert (await stream.first_or_raise()).record_or_raise().bins["email"] is True

stream = await session.query(key).bin("email").str_ends_with(".com").execute()
assert (await stream.first_or_raise()).record_or_raise().bins["email"] is True
```

Boolean-returning reads: ``str_contains``, ``str_starts_with``,
``str_ends_with``, ``str_is_numeric``, ``str_is_upper``, ``str_is_lower``,
``str_regex_compare``.

### Chained Reads on One Bin

Multiple reads on the same bin in one ``execute()`` come back as a list
on the bin value, in op-arrival order:

```python
await session.upsert(key).bin("s").set_to("hello").execute()

stream = await (session.query(key)
                .bin("s").str_strlen()
                .bin("s").str_substr(1, 4)
                .bin("s").str_find("ll")
                .execute())
rec = (await stream.first_or_raise()).record_or_raise()
assert rec.bins["s"] == [5, "ell", 2]
```

## Modifying String Bins

Modify operations mutate the bin in place. Chain freely; the bin's
post-modify state is reflected in subsequent reads on the same key.

```python
await session.upsert(key).bin("name").set_to("alice").execute()

# Case change
await session.upsert(key).bin("name").str_upper().execute()
stream = await session.query(key).bin("name").get().execute()
assert (await stream.first_or_raise()).record_or_raise().bins["name"] == "ALICE"
```

### Appending, Prepending, and Inserting

```python
# Append a single value to the end:
await session.upsert(key).bin("name").str_append(" Smith").execute()
# "ALICE" → "ALICE Smith"

# Prepend a single value to the start:
await session.upsert(key).bin("name").str_prepend("Ms. ").execute()
# → "Ms. ALICE Smith"

# Concat is the multi-value append — takes a list appended in order:
await session.upsert(key).bin("name").str_concat([" Jr.", " III"]).execute()
# → "Ms. ALICE Smith Jr. III"

# Insert at an arbitrary codepoint index (negative counts from the end):
await session.upsert(key).bin("name").str_insert(4, " B.").execute()
# → "Ms. B. ALICE Smith Jr. III"
```

``str_append`` / ``str_prepend`` are the single-value forms; use ``str_concat``
for the list form, and ``str_insert`` when you need an arbitrary position rather
than the start or end.

### Replace, Trim, Pad

```python
await session.upsert(key).bin("greeting").set_to("  hi there  ").execute()

await session.upsert(key).bin("greeting").str_trim().execute()
# → "hi there"

await session.upsert(key).bin("greeting").str_replace("hi", "hello").execute()
# → "hello there"

await session.upsert(key).bin("greeting").str_pad_end(20, ".").execute()
# → "hello there........."
```

### Regex Replace

ICU regex syntax. Set the ``GLOBAL`` flag to replace every match
(default replaces only the first):

```python
from aerospike_sdk import StringRegexFlags

await session.upsert(key).bin("text").set_to("a1 b2 c3").execute()

await (session.upsert(key)
       .bin("text").str_regex_replace(r"\d", "X", flags=StringRegexFlags.GLOBAL)
       .execute())
# → "aX bX cX"
```

## Filter Expressions

Use ``Exp.string_*`` to compose filter expressions for queries or
single-key reads. The conventional last argument is the source string
expression (typically ``Exp.string_bin(...)``).

```python
from aerospike_sdk import Exp

# Find records where bin "email" ends with "@aerospike.com"
stream = await (session.query(profile)
                .where(Exp.string_ends_with(
                    Exp.val("@aerospike.com"), Exp.string_bin("email")))
                .execute())
async for result in stream:
    print(result.record.bins)
stream.close()
```

### Projecting Computed Values

``select_from`` lifts an expression result into a synthetic bin on the
returned record — useful for derived projections.

```python
stream = await (session.query(key)
                .bin("name_length").select_from(
                    Exp.string_strlen(Exp.string_bin("name")))
                .execute())
rec = (await stream.first_or_raise()).record_or_raise()
assert rec.bins["name_length"] == 17
```

## Operating on Nested Strings (CTX)

String operations apply to any string-typed value reachable via a CDT
path. Use the low-level :class:`~aerospike_async.StringOperation` with
``ctx=[...]`` and ``add_operation`` for nested targets:

```python
from aerospike_sdk import StringOperation, CTX

await session.upsert(key).bin("tags").set_to(["alpha", "beta", "gamma"]).execute()

# Uppercase the element at list index 1
await (session.upsert(key)
       .add_operation(StringOperation.upper("tags", ctx=[CTX.list_index(1)]))
       .execute())

stream = await session.query(key).bin("tags").get().execute()
assert (await stream.first_or_raise()).record_or_raise().bins["tags"] == [
    "alpha", "BETA", "gamma",
]
```

Map keys work the same way:

```python
await session.upsert(key).bin("attrs").set_to({"k1": "abcd", "k2": "xyz"}).execute()

stream = await (session.upsert(key)
                .add_operation(StringOperation.strlen("attrs", ctx=[CTX.map_key("k1")]))
                .execute())
assert (await stream.first_or_raise()).record_or_raise().bins["attrs"] == 4
```

The ``to_string`` op is the one exception — it has no CTX overload because
its wire format carries no payload to hold the wrapper.

## Write Flags

Modify operations accept a ``flags`` keyword argument carrying a
:class:`~aerospike_async.StringWriteFlags` bitmask. The only meaningful
flag today is ``NO_FAIL``, which suppresses **missing-bin** errors so an
op against an absent bin becomes a no-op success instead of a
``BIN_NOT_FOUND`` error.

```python
from aerospike_sdk import StringWriteFlags

# Bin "title" doesn't exist on this record; without NO_FAIL this would error.
await (session.upsert(key)
       .bin("title").str_upper(flags=StringWriteFlags.NO_FAIL)
       .execute())
```

``NO_FAIL`` does **not** suppress ``BIN_TYPE_ERROR`` (wrong-type bin) or
``KEY_NOT_FOUND`` (record absent entirely) — those still raise.

## Type Conversion: ``to_string``

Convert a non-string scalar bin into its string representation server-side.
Accepts integer, float, string, and blob source types. The op has no CTX
overload and no ``flags`` argument.

```python
await session.upsert(key).bin("count").set_to(42).execute()

await (session.upsert(key)
       .add_operation(StringOperation.to_string("count"))
       .execute())

stream = await session.query(key).bin("count").get().execute()
assert (await stream.first_or_raise()).record_or_raise().bins["count"] == "42"
```

## Positional Results

When a single ``execute()`` issues multiple ops, the response carries
results in op-arrival order, available on the record's ``results``
attribute (one slot per op). Modify ops produce ``Value::Nil`` on the
wire and surface as ``None`` in the positional list — the by-name
``bins`` dictionary reflects only the post-modify state.

```python
await session.upsert(key).bin("s").set_to("ab").execute()

stream = await (session.upsert(key)
                .bin("s").str_upper()
                .bin("s").get()
                .execute())
rec = (await stream.first_or_raise()).record_or_raise()

assert rec.bins["s"] == "AB"
assert rec.results == [None, "AB"]
```

Use ``results[i]`` (or ``record.operation_result(i)``) when you need to
distinguish *which* op produced *which* value — especially in pipelines
that interleave modifies and reads on the same or different bins.

## See Also

- {class}`~aerospike_async.StringOperation` — low-level operation factory
- {class}`~aerospike_async.StringWriteFlags` — write-side flag bitmask
- {class}`~aerospike_async.StringRegexFlags` — regex flag bitmask
- {class}`~aerospike_async.StringNumericType` — numeric-type filter for ``str_is_numeric``
- [CDT Operations](cdt-operations.md) — list/map structural ops
- [AEL Filter Expressions](expression-ael.md) — string predicates in AEL
