# Aerospike Expression Language (AEL)

AEL lets you write Aerospike filter expressions as strings.
Pass an AEL string to `.where()` on any query or write builder.

```python
stream = await session.query(users).where("$.age > 18").execute()
```

The examples on this page assume a secondary index covers the bin being
filtered. On clusters with query selection, a `.where()` query that no index
can satisfy is rejected rather than run as a full-set scan; see
[Secondary Indexes](indexes.md) to create one or to opt a query into the
primary-index fallback.

## How string AEL is executed

The SDK does **not** parse AEL strings locally. String AEL is sent to the
server for compilation (**field 43** via
`FilterExpression.from_server_compiled_ael`) when every node can compile it.

The check is re-derived from the cluster's node list once per tend interval, so a
node joining with a build older than 8.2.0 (during a rolling upgrade, for
example) closes the capability within one tend rather than at the next
reconnect, and it reopens once that node leaves.

Dataset queries on clusters that also support **query selection** (**field 44**)
use server-led index selection: the server explains the AEL string, picks an
index/plan, and the SDK executes with that plan. Use
[`QueryHint`](../api/query-hint.md) to influence index choice.

If a cluster mid-upgrade may still include an older node, guard string AEL and
fall back to the programmatic [`Exp`](../api/exp.md) builder:

```python
async with ClusterDefinition("localhost", 3000).connect() as cluster:
    if cluster.supports_ael():
        stream = await session.query(users).where("$.age > 18").execute()
    else:
        from aerospike_sdk import Exp
        stream = await (
            session.query(users)
            .where(Exp.gt(Exp.int_bin("age"), Exp.int_val(18)))
            .execute()
        )
```

Unguarded, string AEL against a cluster with such a node raises `AerospikeError` with
`ResultCode.OP_NOT_APPLICABLE`:

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

#### Every bin reference needs a resolvable type

A bin carries no type the expression can see, so the server has to work one out
before it can read the bin. It resolves the type from **either an explicit pin
or a literal somewhere in the expression** — and rejects the expression with
`PARAMETER_ERROR` when it has neither. One literal is enough for every bin in
the expression:

```python
session.query(users).where("$.age == 25")            # the literal 25 resolves $.age
session.query(users).bin("n").select_from("$.age:INT")  # pinned
session.query(users).bin("n").select_from("$.age + 0")  # the literal 0 resolves it
session.query(users).bin("n").select_from("$.age + $.score + 0")  # resolves both

session.query(users).bin("n").select_from("$.age")      # PARAMETER_ERROR
session.query(users).where("$.age > $.score")           # PARAMETER_ERROR
```

This is one rule, not a difference between `where()` and `select_from()`: the
last two fail because nothing in them names a type, and pinning either operand
(`$.age:INT > $.score`) makes both legal. A method call resolves its receiver
the same way — `$.rate.toInt()` is rejected, `$.rate:FLOAT.toInt()` is not.

Failures here arrive as a bare `PARAMETER_ERROR`. Raise
`error_detail_verbosity` to `ErrorDetailVerbosity.MESSAGE` on the session's
`Behavior` to have the server's own explanation returned with the error.

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
not($.deleted:BOOL)
exclusive($.role == "admin", $.suspended:BOOL)
```

`not` is a call: `not($.age == 30)` parses, a bare `not $.age == 30` does not.
`exclusive` is true when exactly one of its two or more arguments is true;
`Exp.exclusive` is the builder form.

### Arithmetic

```
$.price * $.quantity > 1000
$.score + $.bonus >= 100
$.total - $.discount > 0
$.value % 2 == 0
$.rate:FLOAT ** 2.0 > 100.0
```

`**` takes `FLOAT` operands only; pin the bin and write the exponent as a float.

Arithmetic functions:

```
abs($.balance) > 100
ceil($.rating)
floor($.rating)
max($.a, $.b) > 10
min($.a, $.b) < 0
log(value: $.rating:FLOAT, base: 2.0)
pow(base: $.rating:FLOAT, exponent: 2.0)
```

`log` and `pow` take named arguments only.

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
$.count:INT.toFloat() > 3.14
$.rating:FLOAT.toInt()
$.numstr:STRING.toInt()
(5).toFloat()
```

A cast needs its receiver's type, so pin the bin; a numeric literal receiver goes in
parentheses.

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

The printf form uses standard printf template syntax. Both forms are plain
interpolation — **neither quotes nor escapes the value, so never pass untrusted
input**. When the value is not trusted, use the `Exp` builder, which never
round-trips through text.

Two things to know about the printf form. Booleans are lowered to AEL's
`true` / `false` rather than Python's `True`. And AEL's `%` (modulo) operator
must be written `%%` whenever you pass params, since the template is a format
string only then:

```python
session.query(users).where("$.id % 100 == 0")             # no params, plain %
session.query(users).where("$.id %% 100 == 0 and $.age > %d", min_age)
```

### String Methods

String methods chain onto a string-typed receiver, and work in `where()` and
`select_from()` alike:

```
$.name:STRING.upper() == 'ALICE'
$.name:STRING.startsWith('Al')
$.email:STRING.endsWith('@aerospike.com')
$.name:STRING.contains(needle: 'lic')
$.name:STRING.strlen() > 3
$.padded:STRING.trim()
$.name:STRING.replace(find: 'A', replace: 'a')
$.name:STRING =~ /^al/i
```

The `=~` operator matches a regular expression; flags such as `i` follow the
closing slash. Whether an argument is named is part of each method's signature,
not a matter of how many it takes: `contains(needle: 'lic')` must name its one
argument, while `startsWith('Al')` and `repeat(2)` must not, and the server
rejects the other form. For the builder-side equivalents, including string
writes, see [String Operations](string-ops.md).

### List Membership (IN)

```
$.status in ["active", "pending", "review"]
"gold" in $.tiers
```

### CDT Paths

Access nested data with bracket notation:

```
$.settings.theme == "dark"
$.scores.[0] > 90
$.matrix.[0].[1] == 42
$.users.alice.age > 30
```

Map keys can be typed at parse time:

```
$.bin.42 == 100        # integer map key (decimal)
$.bin.0xff == 100      # integer map key (hex)
$.bin.0b101 == 100     # integer map key (binary)
$.bin.-3 == 100        # negative integer map key
$.bin."42" == "x"      # string map key (quoting forces string type)
```

A digit-only segment after the dot (`$.bin.42`) becomes an integer map key;
quote it (`$.bin."42"`) to force string interpretation. The two compile to
distinct expressions and match different keys at runtime.

Selecting several elements at once returns a list, so these read naturally in
`select_from()` or under `.count()`:

```
$.m:MAP.{@alpha,gamma}          # values for a key list
$.m:MAP.{@alpha:gamma}          # values for a key range (end-exclusive)
$.l:LIST.[0:2]                  # values for an index range
$.l:LIST.[#-1]:INT              # element by rank (highest)
```

See {ref}`ael-path-expressions` for wildcards, filters and writes.

### CDT Functions

```
$.scores:LIST.count() > 5
$.tags:LIST.count() == 0
```

`count()` needs to know whether the bin is a list or a map, so pin it.

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

Create and update sketches with the builder API
(`session.upsert(key).bin("h").hll_init(HllConfig.of(14))`, `.hll_add(...)`);
the server compiler does not accept `hllInit`.

### Hex and Binary Literals

```
$.flags == 0xFF
$.mask == 0b10101010
```

### Variables (let/then)

Bind intermediate values:

```
let (total = $.price * $.qty) then (${total} > 1000)
```

Bind in `let (...)`, then refer to each variable as `${name}` inside `then (...)`.

### Unknown and Error

The `unknown` and `error` keywords compile to a sentinel that the server
treats as an evaluator-unknown result — useful as a `when` action when no
sensible value can be returned:

```
when ($.role == "admin" => $.tier:STRING, default => unknown)
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

(ael-path-expressions)=
## Path Expressions

A path walks into a collection bin, fans out over its elements with `*`, and
narrows them with a filter `[?(...)]`. Inside the filter, `@` is the current
element's value, `@key` its map key and `@index` its list index. Paths work in
all three AEL entry points:

```python
# select_from: project the matching elements, or a count of them
stream = await (
    session.query(key).bin("big").select_from("$.l:LIST.*[?(@:INT > 200)]").execute()
)

# where: filter records on a path
stream = await (
    session.query(users)
    .where("$.tags:LIST.*[?(@.upper() == 'VIP')].count() > 0")
    .execute()
)

# upsert_from: write a modified copy of the collection into another bin
await (
    session.upsert(key)
    .bin("bumped").upsert_from("$.l:LIST.*[?(@:INT > 200)].modify(@:INT + 1)")
    .execute()
)
```

A path write never changes the bin it reads: `upsert_from` stores the modified
collection in the target bin (`bumped` above) and leaves `$.l` as it was. The
write methods are `append(...)`, `appendItems([...])`, `setTo(...)` on a
selected element, and `modify(...)` on filtered elements; flags follow a colon,
as in `appendItems([600, 700]):ADD_UNIQUE`.

A loop variable carries no type of its own, so pin it before a cast or
arithmetic: `(@:INT).toString()` and `@:INT + 1` compile and evaluate. Before a
method call the pinned variable must be parenthesized; unlike the bin form
`$.rate:FLOAT.toInt()`, `@:INT.toString()` is a syntax error. An
unpinned `@.toString()` parses but fails at evaluation with
`ResultCode.OP_NOT_APPLICABLE`. Methods that only make sense on one type, such
as `@key.upper()`, need no pin.

### Building paths programmatically

The same walks are available as values, for use with `Exp` or when the path is
assembled at runtime: `select_by_path` / `modify_by_path`, the `SelectFlags`
and `ModifyFlags` enums, `CTX.all_children()` /
`CTX.all_children_with_filter()`, and the loop-variable family
(`Exp.int_loop_var`, `.string_loop_var`, `.map_loop_var`, etc.):

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

Path expressions can also **remove** the elements they match. The dedicated
factory is `CdtOperation.remove(bin, ctx)` — equivalent to
`CdtOperation.modify_by_path` with an `Exp.remove_result()` modify expression —
and `Exp.exp_remove()` is the expression-level counterpart:

```python
over_5 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))

op = CdtOperation.remove("nums", [CTX.all_children_with_filter(over_5)])
```

`examples/cdt_path_expression_example.py` in the repository runs this removal
end to end. Mind the name collision: the chainable
[`.on_map_key(...).remove()`](cdt-operations.md) is the older, unrelated CDT
removal — only the path-based `CdtOperation.remove` takes a `CTX` path with
filters.
