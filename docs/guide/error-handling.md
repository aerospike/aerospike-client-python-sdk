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
