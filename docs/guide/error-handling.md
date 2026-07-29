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
├── QueryTerminatedError
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
        print(f"Key failed: {result.result_code}")
```

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
`RecordResult.in_doubt`. For transactions, `CommitError.in_doubt` reports
whether the commit itself may have landed (see
[Transactions](transactions.md)).

One caveat: the direct point-operation shortcuts — `session.get`,
`session.put`, `session.get_many`, and `session.put_many` — trade the SDK
error boundary for minimum per-op overhead. They surface the underlying
client's exception types (from `aerospike_async.exceptions`), which do
**not** inherit from this module's `AerospikeError`: `get`/`put` raise them,
and the `_many` variants also deliver them as the per-key exception
*instances* in their result lists. Those exceptions carry the same
`in_doubt` attribute, but an `except TimeoutError` written against the
SDK's class will not catch them. Use the builder path when typed handling
matters, or handle the underlying client's types explicitly on these calls.

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
