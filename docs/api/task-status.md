# TaskStatus

The state a server-side task reports through `query_status` on
{class}`~aerospike_sdk.RegisterTask` and {class}`~aerospike_sdk.UdfRemoveTask`.

| Member | Meaning |
|--------|---------|
| `IN_PROGRESS` | At least one node has not finished the task |
| `COMPLETE` | Every node reports the task done |
| `NOT_FOUND` | No node knows the task, which the wait helpers also treat as done |

`wait_till_complete` polls until the status is `COMPLETE` or `NOT_FOUND`, so
most code never inspects this enum directly. Query it when you want to
report progress without blocking.

```python
status = await task.query_status()
if status is TaskStatus.IN_PROGRESS:
    print("UDF still loading on some nodes")
```

```{eval-rst}
.. autoclass:: aerospike_sdk.TaskStatus
   :members:
   :undoc-members:
   :show-inheritance:
```
