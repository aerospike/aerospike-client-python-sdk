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
await session.upsert(key).put({"name": "hello world"}).execute()

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
await session.upsert(key).put({"email": "alice@example.com"}).execute()

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
await session.upsert(key).put({"s": "hello"}).execute()

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
await session.upsert(key).put({"name": "alice"}).execute()

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

### Snip

``str_snip`` removes the half-open codepoint range ``[start, end)``. Omit
``end`` to remove everything from ``start`` through the end of the string:

```python
await session.upsert(key).put({"title": "hello world"}).execute()

# Remove a range:
await session.upsert(key).bin("title").str_snip(1, 4).execute()
# → "ho world"

# Truncate from a codepoint index through the end:
await session.upsert(key).put({"title": "hello world"}).execute()
await session.upsert(key).bin("title").str_snip(5).execute()
# → "hello"
```

Negative indexes count from the end of the string. Write ``flags`` require
an explicit ``end`` — the server reads the snip arguments by position, so
the truncate-to-end form is sent without a flags element.

### Replace, Trim, Pad

```python
await session.upsert(key).put({"greeting": "  hi there  "}).execute()

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

await session.upsert(key).put({"text": "a1 b2 c3"}).execute()

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
path. Use the low-level :class:`~aerospike_sdk.StringOperation` with
``ctx=[...]`` and ``add_operation`` for nested targets:

```python
from aerospike_sdk import StringOperation, CTX

await session.upsert(key).put({"tags": ["alpha", "beta", "gamma"]}).execute()

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
await session.upsert(key).put({"attrs": {"k1": "abcd", "k2": "xyz"}}).execute()

stream = await (session.upsert(key)
                .add_operation(StringOperation.strlen("attrs", ctx=[CTX.map_key("k1")]))
                .execute())
assert (await stream.first_or_raise()).record_or_raise().bins["attrs"] == 4
```

The ``to_string`` op is the one exception — it has no CTX overload because
its wire format carries no payload to hold the wrapper.

## Write Flags

Modify operations accept a ``flags`` keyword argument carrying a
:class:`~aerospike_sdk.StringWriteFlags` bitmask (OR-combine values):

- ``CREATE_ONLY`` — apply only if the bin does not already exist; a live
  bin raises ``BIN_EXISTS_ERROR``. Valid only on the additive ops
  (``str_insert``, ``str_overwrite``, ``str_concat``, ``str_append``,
  ``str_prepend``, ``str_pad_start``, ``str_pad_end``, ``str_repeat``)
  and never with a CTX path — either misuse is a ``PARAMETER_ERROR``.
- ``UPDATE_ONLY`` — apply only to an existing bin; on a missing bin the
  op is a silent no-op instead of creating it. Valid on all modify ops.
  Mutually exclusive with ``CREATE_ONLY`` (``PARAMETER_ERROR``).
- ``NO_FAIL`` — suppress in-op execution failures, such as the
  ``BIN_EXISTS_ERROR`` from ``CREATE_ONLY`` on a live bin: the op becomes
  a no-op and the bin keeps its current value.

```python
from aerospike_sdk import StringWriteFlags

# Seed "title" only if this record doesn't have one yet; a record that
# already carries a title is left alone rather than raising.
await (session.upsert(key)
       .bin("title").str_append("untitled",
                                flags=StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL)
       .execute())
```

A **missing bin is never an error** for string ops, with or without
flags: the additive ops create it from an empty string, and every other
modify is a silent no-op. ``NO_FAIL`` also does **not** suppress
``BIN_TYPE_ERROR`` (wrong-type bin), invalid-UTF-8 errors, or the
``PARAMETER_ERROR`` cases above — those still raise.

## Type Conversion: ``read_as_string``

Convert any scalar bin into its string representation server-side. Accepts
integer, float, string, and blob source types. Because the source bin need
not be a string, the builder method carries no ``str_`` prefix — the
``str_to_integer`` / ``str_to_double`` / ``str_to_blob`` conversions keep
theirs because they genuinely require a string source. The op has no CTX
overload and no ``flags`` argument.

```python
await session.upsert(key).put({"count": 42}).execute()

stream = await session.query(key).bin("count").read_as_string().execute()
assert (await stream.first_or_raise()).record_or_raise().bins["count"] == "42"
```

The low-level factory and expression forms are ``StringOperation.to_string``
and ``Exp.to_string``.

## Positional Results

When a single ``execute()`` issues multiple ops, the response carries
results in op-arrival order, available on the record's ``results``
attribute (one slot per op). Modify ops produce ``Value::Nil`` on the
wire and surface as ``None`` in the positional list — the by-name
``bins`` dictionary reflects only the post-modify state.

```python
await session.upsert(key).put({"s": "ab"}).execute()

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

The slot-per-op contract holds for scalar ops too, and the two views
diverge on purpose when one bin is read more than once: the positional
list keeps every slot, while ``bins`` merges the reads and skips the
write's empty slot.

```python
stream = await (session.upsert(key)
                .bin("n").get()
                .bin("n").add(10)
                .bin("n").get()
                .execute())
row = await stream.first_or_raise()
rec = row.record_or_raise()

assert rec.results == [1, None, 11]   # one slot per op
assert rec.bins["n"] == [1, 11]       # reads merged, write slot skipped
```

For per-op type enforcement, `RecordResult.typed_operation_result(i)`
wraps the same slot in an {class}`~aerospike_sdk.OperationResult`, whose
``get_*`` accessors raise ``TypeError`` on a mismatched read instead of
propagating a miscast value:

```python
count = row.typed_operation_result(2).get_long()   # 11
```

## See Also

- {class}`~aerospike_sdk.StringOperation` — low-level operation factory
- {class}`~aerospike_sdk.StringWriteFlags` — write-side flag bitmask
- {class}`~aerospike_sdk.StringRegexFlags` — regex flag bitmask
- {class}`~aerospike_sdk.StringNumericType` — numeric-type filter for ``str_is_numeric``
- [CDT Operations](cdt-operations.md) — list/map structural ops
- [AEL Filter Expressions](expression-ael.md) — string predicates in AEL
