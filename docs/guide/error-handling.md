# Error Handling

## Exception Hierarchy

All Aerospike errors extend `AerospikeError`:

```
AerospikeError
├── TimeoutError
├── ConnectionError
├── SerializationError
├── SecurityError
│   ├── AuthenticationError
│   └── AuthorizationError
├── GenerationError
├── InvalidNamespaceError
├── InvalidNodeError
├── BackoffError
│   └── MaxErrorRate
├── QuotaError
├── QueryError
│   └── QueryTerminatedError
├── UdfError
├── BatchError
├── RecordNotFoundError
├── RecordExistsError
├── RecordTooBigError
├── FilteredOutError
├── BinError
│   ├── BinExistsError
│   ├── BinNotFoundError
│   ├── BinTypeError
│   └── BinOpInvalidError
├── ElementError
│   ├── ElementNotFoundError
│   └── ElementExistsError
├── CapacityError
│   └── KeyBusyError
├── SecondaryIndexError
│   ├── IndexNotFoundError
│   └── IndexAlreadyExistsError
└── TransactionError
    └── CommitError
```

Catch a base class to handle a whole family at once (for example `except BinError`
covers `BinExistsError`, `BinNotFoundError`, `BinTypeError`, and
`BinOpInvalidError`), or a leaf class for a specific outcome. The
secondary-index base is named `SecondaryIndexError` rather than `IndexError` so
it does not shadow Python's built-in `IndexError`.

Import from the top-level package:

```python
from aerospike_sdk import AerospikeError, RecordExistsError, RecordNotFoundError
```

## Default Behavior

The default error handling depends on the operation type:

| Operation | Default | Behavior |
|-----------|---------|----------|
| Single-key (1 key) | **Raise** | Exception raised immediately |
| Multi-key (batch) | **In-stream** | Errors embedded in `RecordResult` |
| Set query / scan | **In-stream** | Errors embedded in `RecordResult` |

## Checking Results

```python
stream = await session.query(*users.ids(1, 2, 3)).execute()

async for result in stream:
    if result.is_ok:
        print(result.record.bins)
    else:
        print(f"Key failed: {result.exception or result.result_code}")
```

Always branch on `is_ok`, not on `result_code`. A row that failed client-side —
before the request reached the server — has no server result code, so its
`result_code` reads `OK` and the failure is carried by `exception` instead.
`is_ok` accounts for both, which is why the snippet above reports `exception`
first.

Or raise on any failure:

```python
async for result in stream:
    record = result.record_or_raise()  # raises AerospikeError on failure
```

## Extended Server Error Detail

For failures the result code alone does not fully explain, the server can attach
a **subcode** and a **message** to the response. This is opt-in per operation via
`error_detail_verbosity` on a `Behavior`, so the default path pays no extra cost.

| Verbosity | Attaches |
|-----------|----------|
| `ErrorDetailVerbosity.NONE` (default) | nothing |
| `ErrorDetailVerbosity.SUBCODE` | numeric `sub_code` |
| `ErrorDetailVerbosity.MESSAGE` | `sub_code` + human-readable `server_message` |
| `ErrorDetailVerbosity.EXPRESSION_TRACE` | the above + an `exp_trace` on expression-build failures |

When requested, the detail is surfaced on the raised `AerospikeError` (and every
subclass) as `sub_code`, `server_message`, and `exp_trace`; each is `None` when
the server did not supply it.

```python
from aerospike_sdk import Behavior, ErrorDetailVerbosity, SubCode
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from aerospike_sdk.exceptions import AerospikeError

behavior = Behavior(
    "verbose-errors",
    {Scope.ALL: Settings(error_detail_verbosity=ErrorDetailVerbosity.MESSAGE)},
)
session = client.create_session(behavior=behavior)

try:
    stream = await session.query(users.id(1)).bin("scores").on_list_index(99).get_values().execute()
    await stream.first_or_raise()
except AerospikeError as err:
    if err.sub_code == SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS:
        ...  # refresh the cached list size and retry
    print(err.result_code, err.sub_code, err.server_message)
```

A subcode value is scoped to its parent result code — it is **not** globally
unique, so always interpret the `(result_code, sub_code)` pair together. The
`SubCode` catalog enumerates the known subcodes. Requires Aerospike server 8.1.3
or later; older servers ignore the request and leave the attributes `None`.

Batches report failures per record rather than raising, so the same detail
travels as data instead: a failed batch row carries the full surface on
`RecordResult` — `sub_code`, `server_message`, and `exp_trace` (each `None`
for successful rows or when detail was not requested) — and
`RecordResult.or_raise()` attaches all three to the raised error, matching
the single-key exception shape.

## Client-Side Guidance (`hint`)

Some result codes name a condition without saying what caused it. The server
reports `BinNameTooLong` for a bin name over the limit, for instance, but does not
say *which* bin. For codes whose cause is a common, recognizable
misconfiguration, the SDK adds local guidance: it is appended to the exception
message and also kept on its own attribute.

```python
try:
    await session.upsert(customers.id(1)).put({"a_very_long_bin_name": 1}).execute()
except AerospikeError as err:
    print(err)         # server message, then the guidance on the next line
    print(err.hint)    # just the guidance, or None when there is nothing to add
```

When the SDK still holds the operation that failed, the guidance names the
specific cause rather than the general one:

```
Code: BinNameTooLong, In Doubt: false, Node: 10.0.0.4:3000
Bin name 'a_very_long_bin_name' exceeds the server's 15-character limit.
```

`hint` is generated locally and never travels the wire — that is the difference
from `server_message`, which is the server's own text. Most codes carry no hint:
guidance that only restates the code name would be noise.

### Precedence

Guidance resolves narrowest-first:

1. **What the SDK knows about the operation** — the bin-naming case above.
2. **The `(result_code, sub_code)` pair**, when extended error detail is enabled.
3. **The result code alone**, as a fallback.

Step 2 matters because several codes cover more than one condition, and the
server already distinguishes them. `FailForbidden` alone could be a durability
violation, a set-level stop-writes limit, cluster clock skew, an in-progress
truncate, XDR filtering, or conflict resolution; `BinNameTooLong` is returned
both for an over-long name *and* for a record with too many bins. Without a
subcode the SDK says so rather than picking one — naming a single cause would
misdirect whenever it is not that one. With a subcode it names the condition
exactly:

```
Code: FailForbidden, In Doubt: false, Node: 10.0.0.4:3000
A non-durable delete was refused because it would violate durability.
Strong-consistency namespaces require durable deletes: enable durable delete on
the operation, or on the Behavior the session carries.
```

Subcode integers are scoped to their parent code, not globally unique, so the
lookup keys on the pair. An unrecognized subcode — a newer server than this
SDK — falls back to the per-code text rather than losing the guidance.

Keeping the guidance on its own attribute means it can be logged or suppressed
independently of the message — useful when error text is parsed or forwarded.

## In-Doubt Writes

Every `AerospikeError` carries an `in_doubt` flag. It is `True` when a **write**
reached the server but its outcome is unknown — the record may or may not have
been applied. Typical producers are a client-side timeout after the request was
sent, a connection that dropped mid-flight, and retry exhaustion on a write.
Failures where the request never left the client (validation errors, timeouts
before send) and read operations are never in doubt.

```python
from aerospike_sdk import TimeoutError

try:
    await session.insert(orders.id(order_id)).put(payload).execute()
except TimeoutError as err:
    if err.in_doubt:
        ...  # verify with a read before retrying a non-idempotent write
    else:
        ...  # the write did not happen; safe to retry as-is
```

For batch operations with in-stream errors, the per-key flag is
`RecordResult.in_doubt`. This holds even when the batch fails as a whole:
each row reports its own outcome — rows the server answered keep their
result, and unanswered rows carry the failure's stamped code, so a batch
write that client-times-out after reaching the wire yields one row per key
with `result_code=TIMEOUT` and `in_doubt=True`. For transactions,
`CommitError.in_doubt` reports whether the commit itself may have landed
(see [Transactions](transactions.md)).

The direct point-operation shortcuts — `session.get`, `session.put`,
`session.get_many`, and `session.put_many` — raise the same SDK exception
types as every other path: a failure that propagates out of them is
converted at the boundary, so `except AerospikeError` (or a typed subclass
like `TimeoutError`) works uniformly, `in_doubt` included. The one remaining
distinction is the `_many` variants' **per-key result slots**: an exception
instance delivered *in the result list* (not raised) is the underlying
client's type from `aerospike_async.exceptions`, left unconverted so
successful windows never pay a conversion scan. Those instances carry the
same `in_doubt` attribute; check slots with `isinstance(slot, Exception)`
rather than an SDK-typed `except`.

## Client vs Server Timeouts

`TimeoutError.client` tells you which side gave up. `True` means the
client's own deadline fired (socket or total timeout expired locally) —
the server may still be working, so pair it with `in_doubt` before
retrying a write. `False` means the server itself reported the timeout
result code, so the operation was accounted for on the server side.

```python
try:
    await session.put(orders.id(order_id), payload)
except TimeoutError as err:
    if err.client and err.in_doubt:
        ...  # local deadline on a sent write; read-verify before retrying
    elif not err.client:
        ...  # the server timed it out; look at server load, not the network
```

## Retry and Diagnostic Context

Every `AerospikeError` also answers *where* and *how many times* the
command failed:

- `node` — the cluster node the failing attempt targeted, when the retry
  loop recorded one (`None` for failures that never reached node
  selection).
- `iteration` — the number of attempts made before the command failed.
- `sub_exceptions` — the errors of prior retry attempts, oldest first,
  each converted into this hierarchy (empty when the command was not
  retried).
- `base_message` — the failure message without the retry-context
  decoration the full message carries.

```python
try:
    await session.put(orders.id(order_id), payload)
except AerospikeError as err:
    log.error(
        "write failed on %s after %s attempts: %s",
        err.node, err.iteration, err.base_message,
    )
    for attempt in err.sub_exceptions:
        log.debug("prior attempt: %s", attempt)
```

### Design notes

A few adjacent capabilities are intentionally *not* part of the exception
surface; they are recorded here so the choices are visible:

- **Resolved policy values** (`connect_timeout`, `socket_timeout`,
  `total_timeout`, `max_retries`) are not echoed onto exceptions. Since
  namespace-mode-aware policy selection happens inside the underlying
  client, no SDK-level layer reliably knows which resolved policy the
  failing command actually used — an echoed value that can be wrong is
  worse than none. Reconstruct the resolved values from your `Behavior`
  (`behavior.get_settings(...)`) when needed.
- **Connection-recycling hints**: the underlying client decides internally
  whether a failed command's connection is reusable; the signal is not
  actionable from Python and is not exposed.
- **Message format**: the human-readable message stays prose (with
  `base_message` as the undecorated form) rather than encoding fields in a
  positional prefix — the structured attributes carry every field the
  message would otherwise need to encode.

## ErrorStrategy

Override the default with `on_error` at execute time:

```python
from aerospike_sdk import ErrorStrategy

# Force in-stream errors for single-key operations
stream = await (
    session.query(users.id(1))
    .execute(on_error=ErrorStrategy.IN_STREAM)
)
# result.is_ok will be False instead of raising
```

## ErrorHandler (Callback)

Route errors to a custom callback:

```python
from aerospike_sdk import ErrorHandler

def my_handler(result):
    log.warning(f"Record error: {result.result_code} for {result.record.key}")

stream = await (
    session.query(*users.ids(1, 2, 3))
    .execute(on_error=ErrorHandler(my_handler))
)
# Errors go to my_handler; only successes appear in the stream
```

## Common Patterns

### Optimistic Locking

```python
try:
    await (
        session.update(users.id(1))
        .ensure_generation_is(expected_gen)
        .bin("balance").set_to(new_balance)
        .execute()
    )
except GenerationError:
    # Record was modified since we read it — retry
    pass
```

### Create-Only Insert

```python
from aerospike_sdk import RecordExistsError

try:
    await session.insert(users.id(1)).put({"name": "Ada"}).execute()
except RecordExistsError:
    # Key already taken — fall back to an update or report a conflict
    pass
```

### Conditional Write with Filter

```python
from aerospike_sdk import FilteredOutError

stream = await (
    session.update(users.id(1))
    .where("$.balance >= 100")
    .fail_on_filtered_out()
    .bin("balance").increment_by(-100)
    .execute()
)
try:
    result = await stream.first_or_raise()
except FilteredOutError:
    print("Insufficient balance — filter matched no record")
```
