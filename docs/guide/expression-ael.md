# Aerospike Expression Language (AEL)

AEL lets you write Aerospike filter expressions as strings.
Pass an AEL string to `.where()` on any query or write builder.

```python
stream = await session.query(users).where("$.age > 18").execute()
```

## Syntax Reference

### Bin Access

Prefix bin names with `$`:

```
$.age
$.name
$.settings
```

Bin names accept a wider character set than plain identifiers:

```
$.name@host         # @ is permitted in bin names
$.@attr             # leading or trailing @
$."my-bin"          # quoting allows otherwise-illegal characters (-, space, $, ...)
$.'my bin'          # single quotes work too
$.true              # reserved keywords are valid bin names
$.when              # so are keywords like 'when', 'and', 'or', 'let', etc.
```

The substring `null` (case-insensitive) is reserved and rejected: `$.null`,
`$.my_null_bin`, and `$."NULL"` all raise `AelParseException` at parse time.

### Comparison Operators

```
$.age == 30
$.age != 30
$.age > 18
$.age >= 18
$.age < 65
$.age <= 65
```

### Logical Operators

```
$.age > 18 and $.status == "active"
$.role == "admin" or $.role == "superadmin"
not $.deleted
```

### Arithmetic

```
$.price * $.quantity > 1000
$.score + $.bonus >= 100
$.total - $.discount > 0
$.value % 2 == 0
$.base ** 2 > 100
```

Arithmetic functions:

```
abs($.balance) > 100
ceil($.rating)
floor($.rating)
log($.value)
pow($.base, 2)
max($.a, $.b)
min($.a, $.b)
```

### Bitwise Operators

```
$.flags & 0xFF
$.mask | 0x01
$.value ^ 0xAA
~$.mask
$.bits << 4
$.bits >> 2
$.bits >>> 2
```

### Type Casting

```
$.count.asFloat() > 3.14
5.asFloat()
3.14.asInt()
```

### String Values

Use double or single quotes:

```
$.name == "Alice"
$.name == 'Alice'
```

### List Membership (IN)

```
$.status in ["active", "pending", "review"]
"gold" in $.tiers
```

### CDT Paths

Access nested data with bracket notation:

```
$.settings.["theme"] == "dark"
$.scores.[0] > 90
$.matrix.[0].[1] == 42
$.users.["alice"].age > 30
```

Map keys can be typed at parse time:

```
$.bin.42 == 100        # integer map key (decimal)
$.bin.0xff == 100      # integer map key (hex)
$.bin.0b101 == 100     # integer map key (binary)
$.bin.+5 == 100        # signed integer map key
$.bin.-3 == 100
$.bin."42" == "x"      # string map key (quoting forces string type)
$.bin.{1-5} == 100     # integer key range
$.bin.{1,2,3} == 100   # integer key list
```

A digit-only segment after the dot (`$.bin.42`) becomes an integer map key;
quote it (`$.bin."42"`) to force string interpretation. The two compile to
distinct expressions and match different keys at runtime.

### CDT Functions

```
$.scores.count() > 5
$.tags.count() == 0
```

### GeoJSON

Compare a GeoJSON bin to a literal value with `geoCompare(a, b)`. Either side
can be a bin path or a `geoJson('...')` literal — pick whichever reads more
naturally. The match semantics are server-side GEO2DSPHERE: a Point matches
any AeroCircle or Polygon containing it, and vice versa.

```
geoCompare($.loc, geoJson('{"type":"Point","coordinates":[-122.349,47.620]}'))
geoCompare(geoJson('{"type":"AeroCircle","coordinates":[[-122.0,37.4],3000.0]}'), $.loc)
```

Bins typed as `GEO` are recognized automatically when referenced inside
`geoCompare(...)`; an explicit cast like `$.loc.get(type: GEO)` is accepted
but not required.

### HyperLogLog

Seven read-side HLL path functions are available on HLL bins. Each operates on
`$.binName` as the receiver:

```
$.h.hllCount() > 1000000
$.h.hllDescribe() == [14, 0]
$.h.hllMayContain(['alice', 'bob']) == 1
$.h.hllUnionCount(?0) > 50000
$.h.hllIntersectCount(?0) > 100
$.h.hllSimilarity(?0) >= 0.8
$.h.hllUnion(?0) == ?1
```

`hllDescribe()` returns a two-element list ``[index_bit_count, min_hash_bit_count]``;
the server reports `0` for a sketch without minhash (the `-1` sentinel used
client-side to mean "inherit / no minhash" is normalized away on the wire).

The multi-sketch functions (`hllUnion`, `hllUnionCount`, `hllIntersectCount`,
`hllSimilarity`) take their multi-sketch argument in one of two shapes:

- **A single HLL bin reference** — `$.a`. The server treats a bare HLL value
  as an implicit single-element list, so `$.h.hllUnionCount($.a)` evaluates
  cleanly.
- **A list-typed expression of HLL byte blobs** — either an inline literal
  list `[?0, ?1]` or a placeholder bound to a Python `list[bytes]`.

`[$.a, $.b]` (a list literal containing bin references) is **not** supported
— the server's HLL ops can't recursively evaluate scalar bin sub-expressions
inside a composed list. If you need to combine multiple bins in one
expression without pre-fetching, drop down to the programmatic `Exp.*` API
or open multiple bin-pair queries.

Write-side AEL (`hllInit`, `hllAdd`) is **not** currently supported — the
existing grammar allows at most one path function per path, and chained
write-then-read forms require a grammar refactor that's better aligned with
the upcoming server-side AEL design. Use the builder API
(`session.upsert(key).bin("h").hll_init(HllConfig.of(14))`) for writes
today; AEL is read-only for HLL until then.

### Hex and Binary Literals

```
$.flags == 0xFF
$.mask == 0b10101010
```

### Variables (let/then)

Bind intermediate values:

```
let $total = $.price * $.qty then $total > 1000
```

### Placeholders

Use `?0`, `?1`, etc. for parameterized queries:

```python
from aerospike_sdk import parse_ael

expr = parse_ael("$.age > ?0 and $.status == ?1", 18, "active")
```

### Unknown and Error

The `unknown` and `error` keywords compile to a sentinel that the server
treats as an evaluator-unknown result — useful as a `when` action when no
sensible value can be returned:

```
when ($.role == "admin" => $.tier, default => unknown)
```

`error` is an alias for `unknown` and produces the same expression. Both
short-circuit any enclosing comparison or logical operator.

## Auto Index Discovery

When a secondary index exists on a bin referenced in the AEL expression,
the client automatically generates an optimal secondary index `Filter`
alongside the `FilterExpression`. This is transparent — no code changes needed.

To influence index selection, use [`QueryHint`](../api/query-hint.md):

```python
from aerospike_sdk import QueryHint

stream = await (
    session.query(users)
    .where("$.age > 25 and $.city == 'NYC'")
    .with_hint(QueryHint(index_name="age_idx"))
    .execute()
)
```

## Programmatic Expressions

For cases where a string AEL expression is insufficient, use the `Exp` builder
or raw `FilterExpression` from `aerospike_async`:

```python
from aerospike_sdk import Exp

expr = Exp.and_([
    Exp.gt(Exp.int_bin("age"), Exp.int_val(18)),
    Exp.eq(Exp.string_bin("status"), Exp.string_val("active")),
])

stream = await session.query(users).where(expr).execute()
```

## Path Expressions (Server 8.1.1+)

Path expressions — `select_by_path` / `modify_by_path`, the `SelectFlag` and
`ModifyFlag` return/modify flag enums, `CTX.all_children()` /
`CTX.all_children_with_filter()`, and the loop-variable family
(`FilterExpression.int_loop_var`, `.string_loop_var`, `.map_loop_var`, etc.)
— are not yet surfaced through the AEL string grammar. Use the low-level
`aerospike_async` types directly:

```python
from aerospike_async import (
    CTX,
    CdtOperation,
    FilterExpression,
    LoopVarPart,
    ModifyFlag,
    SelectFlag,
)

in_stock = FilterExpression.eq(
    FilterExpression.map_loop_var(LoopVarPart.VALUE),
    FilterExpression.bool_val(True),
)

op = CdtOperation.select_by_path(
    "store",
    SelectFlag.VALUE,
    [CTX.map_key("books"), CTX.all_children_with_filter(in_stock)],
)
```

These constructs require Aerospike Server 8.1.1 or newer. A dedicated AEL
surface is deferred until the DSL shape stabilizes across clients.
