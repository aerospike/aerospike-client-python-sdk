# String Operations

Low-level string-operation factory and supporting types. The chainable
``str_*`` methods on :class:`~aerospike_sdk.aio.operations.query.WriteBinBuilder`
and :class:`~aerospike_sdk.aio.operations.query.QueryBinBuilder` (documented
under [QueryBuilder](query.md)) cover the common cases; reach for the
factory directly when you need to apply a string op to a value nested
inside a CDT via ``ctx=[...]``.

See the [String Operations guide](../guide/string-ops.md) for narrative
examples.

```{eval-rst}
.. autoclass:: aerospike_sdk.StringOperation
   :members:
   :show-inheritance:

.. autoclass:: aerospike_sdk.StringWriteFlags
   :members:
   :show-inheritance:

.. autoclass:: aerospike_sdk.StringRegexFlags
   :members:
   :show-inheritance:

.. autoclass:: aerospike_sdk.StringNumericType
   :members:
   :show-inheritance:
```
