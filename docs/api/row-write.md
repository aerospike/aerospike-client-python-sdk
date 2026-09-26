# Tabular writes

`session.upsert(dataset)` (and `insert`, `update`, `replace`) returns a
`DataSetWriteBuilder`; its `bins(...)` opens the `RowWriteBuilder`.

```{eval-rst}
.. autoclass:: aerospike_sdk.aio.operations.query.DataSetWriteBuilder
   :members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: aerospike_sdk.aio.operations.query.RowWriteBuilder
   :members:
   :inherited-members:
   :show-inheritance:
```
