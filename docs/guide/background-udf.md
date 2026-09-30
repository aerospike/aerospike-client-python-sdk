# Background Tasks & UDF

## Background Tasks

Background tasks execute server-side operations across a dataset without
streaming results back. Use them for bulk updates, deletes, or touches.

### Bulk Delete

```python
users = DataSet.of("test", "users")

task = await (
    session.background_task()
    .delete(users)
    .where("$.status == 'inactive'")
    .execute()
)
await task.wait_till_complete(sleep_time=0.5, max_attempts=60)
```

### Bulk Update

```python
task = await (
    session.background_task()
    .update(users)
    .where("$.tier == 'free'")
    .bin("trial_expired").set_to(True)
    .execute()
)
await task.wait_till_complete()
```

`bin()` offers the same steps as a key-scoped write: scalar and expression
writes, and list, map, bit, HyperLogLog and string operations, including nested
navigation. This job removes every segment whose value list sorts before a
cutoff, and appends a tag to the list stored under a map key:

```python
profiles = DataSet.of("test", "profiles")

task = await (
    session.background_task()
    .update(profiles)
    .bin("segments").on_map_value_range(None, [1704067200]).remove()
    .bin("prefs").on_map_key("tags").list_append_items(["sports"])
    .execute()
)
await task.wait_till_complete()
```

To send an operation you already built, pass it to `add_operation()`:

```python
from aerospike_async import MapOperation, MapReturnType

task = await (
    session.background_task()
    .update(profiles)
    .add_operation(MapOperation.remove_by_value_range(
        "segments", None, [1704067200], MapReturnType.NONE,
    ))
    .execute()
)
```

A background job can only write. The server rejects the job if it contains a
read operation, such as `get()` or `select_from()`.

### Narrowing with an Index and a Predicate

`where()` filters every record the job reaches. `index_filters()` changes *which*
records it reaches at all, by sending the job through a secondary index instead of
a scan of the set. They combine: the index selects the candidates, and the
predicate decides which of those the job writes.

```python
from aerospike_async import Filter

task = await (
    session.background_task()
    .update(donors)
    .index_filters(Filter.range("age", 30, 65))     # reached through the index
    .where("not($.update_pass.exists()) or $.update_pass < 5")   # and filtered
    .bin("campaign1").add(50)
    .bin("update_pass").set_to(5)
    .execute()
)
await task.wait_till_complete()
```

The predicate makes the job re-runnable: it excludes the records an earlier run
already processed, so running the job a second time changes nothing. The index keeps
the server off a full scan.

Either narrowing works alone. With only `where()`, the server scans the set; with
only `index_filters()`, every record in the index range is written.

### Bulk Touch (Reset TTL)

```python
from datetime import timedelta

task = await (
    session.background_task()
    .touch(users)
    .where("$.active == true")
    .expire_record_after(timedelta(days=30))
    .execute()
)
await task.wait_till_complete()
```

The TTL verbs are the same as on single-record writes: `expire_record_after`,
`expire_record_after_seconds`, `expire_record_at`, `never_expire`,
`with_no_change_in_expiration`, and `expiry_from_server_default`.

Background tasks cannot run inside a transaction; see
{ref}`Transactions <txn-background-tasks>`.

### Background UDF

Run a Lua UDF across matching records:

```python
task = await (
    session.background_task()
    .execute_udf(users)
    .function("my_module", "transform_record")
    .passing("arg1", "arg2")
    .execute()
)
await task.wait_till_complete()
```

## Foreground UDF

Execute a Lua UDF on specific keys and get results back:

### Single Key

```python
stream = await (
    session.execute_udf(users.id(1))
    .function("my_module", "get_computed_value")
    .passing(42)
    .execute()
)
result = await stream.first_or_raise()
print(result.udf_result)  # return value from Lua
```

### Multiple Keys (Batch UDF)

```python
stream = await (
    session.execute_udf(*users.ids(1, 2, 3))
    .function("my_module", "process_record")
    .execute()
)
async for result in stream:
    print(result.record.key, result.udf_result)
```

A UDF segment takes the same chain-wide verbs as a write segment:
`default_where`, the `default_expire*` family, `fail_on_filtered_out`, and
`with_txn`. `with_txn(None)` also opts a multi-key UDF out of the
{ref}`implicit batch-write transaction <txn-implicit-batch-write>`
on a strong-consistency namespace:

```python
stream = await (
    session.execute_udf(*users.ids(1, 2, 3))
    .function("my_module", "process_record")
    .with_txn(None)
    .execute()
)
```

### Chaining with Reads and Writes

UDF segments compose with read and write segments in both directions —
the whole chain executes as one batch, one result row per segment. Use
`execute_udf(*keys)` mid-chain to close the current segment and open a
UDF segment on new keys; from a UDF segment, `query(...)` or a write
verb switches back:

```python
stream = await (
    session.upsert(orders.id(17)).put({"status": "paid"})
    .execute_udf(stats.id("daily"))
    .function("order_stats", "record_payment")
    .passing(17)
    .query(orders.id(17)).bin("status").get()
    .execute()
)
rows = await stream.collect()  # write result, UDF result, read result
```

## UDF Registration

Register and remove Lua modules:

```python
info = session.info()

# Register a UDF module
await info.register_udf("/path/to/my_module.lua")

# Remove a UDF module
await info.remove_udf("my_module.lua")
```

## Monitoring Tasks

`ExecuteTask` provides polling-based completion monitoring:

```python
task = await (
    session.background_task()
    .delete(users)
    .where("$.expired == true")
    .execute()
)

# Poll with custom intervals
await task.wait_till_complete(
    sleep_time=0.2,       # seconds between polls
    max_attempts=100,     # max poll attempts
)
```
