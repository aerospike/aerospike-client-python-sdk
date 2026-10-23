# RegisterTask

The handle returned by every UDF registration call:
{meth}`Cluster.register_udf() <aerospike_sdk.aio.cluster.Cluster.register_udf>`,
{meth}`Cluster.register_udf_from_file() <aerospike_sdk.aio.cluster.Cluster.register_udf_from_file>`,
{meth}`Cluster.register_udf_from_resource() <aerospike_sdk.aio.cluster.Cluster.register_udf_from_resource>`,
and their {class}`~aerospike_sdk.aio.session.Session` and sync equivalents.

Registration returns as soon as the server has accepted the module, but the
module is not callable until every node has loaded it. Wait on the task
before the first `execute_udf` or background UDF call, or a fast follower
can fail with a "function not found" error on a node that is still
loading.

The async methods poll with the running event loop. The `_blocking`
siblings do the same from a plain thread and refuse to run inside an
active loop, so use them from
{class}`~aerospike_sdk.sync.session.SyncSession` code.

```python
task = await cluster.register_udf_from_file(
    "udf/record_ops.lua", "record_ops.lua", UDFLang.LUA
)
await task.wait_till_complete(sleep_time=0.25, timeout=30)

# Sync surface
task = sync_cluster.register_udf_from_file(
    "udf/record_ops.lua", "record_ops.lua", UDFLang.LUA
)
task.wait_till_complete_blocking(timeout=30)
```

`wait_till_complete` returns `True` once the module is loaded on every node
and raises `TimeoutError` if the budget elapses first. Pass `timeout=None`
to wait without limit. `query_status` returns the current registration
status as a {class}`~aerospike_sdk.TaskStatus` without waiting.
{class}`~aerospike_sdk.UdfRemoveTask`, returned by `remove_udf`, has the same
four methods.

```{eval-rst}
.. autoclass:: aerospike_sdk.RegisterTask
   :members:
   :undoc-members:
   :show-inheritance:
```
