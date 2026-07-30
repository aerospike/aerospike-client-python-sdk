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

### Bulk Touch (Reset TTL)

```python
task = await (
    session.background_task()
    .touch(users)
    .where("$.active == true")
    .execute()
)
await task.wait_till_complete()
```

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
