# Aerospike Expression Language (AEL)

AEL lets you write Aerospike filter expressions as strings.
Pass an AEL string to `.where()` on any query or write builder.

```python
stream = await session.query(users).where("$.age > 18").execute()
```

## How string AEL is executed

The SDK does **not** parse AEL strings locally. When the connected cluster
supports it (Aerospike **8.1.3+** on every node), string AEL is sent to the
server for compilation (**field 43** via
`FilterExpression.from_server_compiled_ael`).

The check is re-derived from the cluster's node list once per tend interval, so a
node joining with an older build closes the capability within one tend rather
than at the next reconnect, and it reopens once that node leaves.

Dataset queries on clusters that also support **query selection** (**field 44**)
use server-led index selection: the server explains the AEL string, picks an
index/plan, and the SDK executes with that plan. Use
[`QueryHint`](../api/query-hint.md) to influence index choice.

On older clusters, use the programmatic [`Exp`](exp.md) builder instead of string
AEL. Check capability at runtime:

```python
async with await ClusterDefinition("localhost", 3000).connect() as cluster:
    if await cluster.supports_ael():
        stream = await session.query(users).where("$.age > 18").execute()
    else:
        from aerospike_sdk import Exp
        stream = await (
            session.query(users)
            .where(Exp.gt(Exp.int_bin("age"), Exp.int_val(18)))
            .execute()
        )
```

String AEL against an older cluster raises `AerospikeError` with
`ResultCode.OP_NOT_APPLICABLE`, the same result code the Java SDK reports for
this case:

```python
from aerospike_sdk import ResultCode
from aerospike_sdk.exceptions import AerospikeError

try:
    stream = await session.query(users).where("$.age > 18").execute()
except AerospikeError as exc:
    if exc.result_code == ResultCode.OP_NOT_APPLICABLE:
        ...  # fall back to Exp
```

## Syntax Reference

The grammar below describes valid AEL text accepted by the server compiler.
Invalid syntax or reserved names are rejected at **query time** on the server,
not by a local parser.

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

The substring `null` (case-insensitive) is reserved in bin names: `$.null`,
`$.my_null_bin`, and `$."NULL"` are invalid.

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

Embed dynamic values either with an f-string or by passing params to
`where()`, which interpolates them with printf syntax:

```python
min_age = 18
stream = await session.query(users).where(f"$.age > {min_age}").execute()
stream = await session.query(users).where("$.age > %d", min_age).execute()
```

The printf form uses the same template syntax as the Java SDK's
`where(String ael, Object... params)`, so one template works in both.

Both forms are plain interpolation — **neither quotes nor escapes the value,
so never pass untrusted input**. When the value is not trusted, use the `Exp`
builder, which never round-trips through text.

Two things to know about the printf form. Booleans are lowered to AEL's
`true` / `false` rather than Python's `True`. And AEL's `%` (modulo) operator
must be written `%%` whenever you pass params, since the template is a format
string only then:

```python
session.query(users).where("$.id % 100 == 0")             # no params, plain %
session.query(users).where("$.id %% 100 == 0 and $.age > %d", min_age)
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
$.h.hllUnionCount($.a) > 50000
$.h.hllIntersectCount($.a) > 100
$.h.hllSimilarity($.a) >= 0.8
$.h.hllUnion($.a) == x'00040c00...'
```

`hllDescribe()` returns a two-element list ``[index_bit_count, min_hash_bit_count]``;
the server reports `0` for a sketch without minhash (the `-1` sentinel used
internally to mean "inherit / no minhash" is normalized away on the wire).

The multi-sketch functions (`hllUnion`, `hllUnionCount`, `hllIntersectCount`,
`hllSimilarity`) take their multi-sketch argument in one of two shapes:

- **A single HLL bin reference** — `$.a`. The server treats a bare HLL value
  as an implicit single-element list, so `$.h.hllUnionCount($.a)` evaluates
  cleanly.
- **A list-typed expression of HLL byte blobs** — an inline literal list of
  AEL blob literals, `[x'00040c00...', x'00040c00...']`. Build these in
  Python with `sketch.hex()` and interpolate them into the template.

`[$.a, $.b]` (a list literal containing bin references) is **not** supported
— the server's HLL ops can't recursively evaluate scalar bin sub-expressions
inside a composed list. If you need to combine multiple bins in one
expression without pre-fetching, drop down to the programmatic `Exp.*` API
or open multiple bin-pair queries.

Write-side AEL (`hllInit`, `hllAdd`) is **not** currently supported — the
existing grammar allows at most one path function per path, and chained
write-then-read forms require a grammar refactor that's better aligned with
the server-side AEL design. Use the builder API
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

### Unknown and Error

The `unknown` and `error` keywords compile to a sentinel that the server
treats as an evaluator-unknown result — useful as a `when` action when no
sensible value can be returned:

```
when ($.role == "admin" => $.tier, default => unknown)
```

`error` is an alias for `unknown` and produces the same expression. Both
short-circuit any enclosing comparison or logical operator.

## Query hints and index selection

On clusters with query selection (field 44), the server picks the secondary
index and query plan from the AEL string. Influence that choice with
[`QueryHint`](../api/query-hint.md):

```python
from aerospike_sdk import QueryHint

stream = await (
    session.query(users)
    .where("$.age > 25 and $.city == 'NYC'")
    .with_hint(QueryHint(index_name="age_idx"))
    .execute()
)
```

See the [Secondary Indexes guide](indexes.md) for creating indexes and listing
them with `session.list_indexes()`.

## Programmatic Expressions

For cases where a string AEL expression is insufficient, use the `Exp` builder
(`Exp` is Aerospike's expression type, re-exported from `aerospike_sdk`):

```python
from aerospike_sdk import Exp

expr = Exp.and_([
    Exp.gt(Exp.int_bin("age"), Exp.int_val(18)),
    Exp.eq(Exp.string_bin("status"), Exp.string_val("active")),
])

stream = await session.query(users).where(expr).execute()
```

Use `Exp` on all clusters; use string AEL when `supports_ael()` is true.

## Path Expressions (Server 8.1.1+)

Path expressions — `select_by_path` / `modify_by_path`, the `SelectFlags` and
`ModifyFlags` return/modify flag enums, `CTX.all_children()` /
`CTX.all_children_with_filter()`, and the loop-variable family
(`Exp.int_loop_var`, `.string_loop_var`, `.map_loop_var`, etc.)
— are not yet surfaced through the AEL string grammar. Use the corresponding
`aerospike_sdk` types directly:

```python
from aerospike_sdk import (
    CTX,
    CdtOperation,
    Exp,
    LoopVarPart,
    ModifyFlags,
    SelectFlags,
)

in_stock = Exp.eq(
    Exp.map_loop_var(LoopVarPart.VALUE),
    Exp.bool_val(True),
)

op = CdtOperation.select_by_path(
    "store",
    SelectFlags.VALUE,
    [CTX.map_key("books"), CTX.all_children_with_filter(in_stock)],
)
```

These constructs require Aerospike Server 8.1.1 or newer. A dedicated AEL
surface is deferred until the DSL shape stabilizes across clients.
