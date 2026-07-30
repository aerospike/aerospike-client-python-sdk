# ExpressionTrace

Structured server-supplied expression build trace, surfaced on
[`AerospikeError.exp_trace`](exceptions.md) when `error_detail_verbosity` is set
to `ErrorDetailVerbosity.EXPRESSION_TRACE` and an expression fails to build on a
server that emits the trace. See the
[Error Handling guide](../guide/error-handling.md) for usage.

```{eval-rst}
.. autoclass:: aerospike_sdk.ExpressionTrace
   :members:
```
