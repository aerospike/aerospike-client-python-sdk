# UdfRemoveTask

The handle returned by
{meth}`Cluster.remove_udf() <aerospike_sdk.aio.cluster.Cluster.remove_udf>` and
its {class}`~aerospike_sdk.aio.session.Session` and sync equivalents. It has
the same four methods as {class}`~aerospike_sdk.RegisterTask`: wait on it
before registering a replacement module under the same name, or before
asserting that calls to the removed functions now fail.

```python
task = await cluster.remove_udf("record_ops.lua")
await task.wait_till_complete(timeout=30)

# Sync surface
task = sync_cluster.remove_udf("record_ops.lua")
task.wait_till_complete_blocking(timeout=30)
```

`wait_till_complete` returns `True` once the module is gone from every node
and raises `TimeoutError` if the budget elapses first. `query_status`
returns the current state as a {class}`~aerospike_sdk.TaskStatus`.

```{eval-rst}
.. autoclass:: aerospike_sdk.UdfRemoveTask
   :members:
   :undoc-members:
   :show-inheritance:
```
