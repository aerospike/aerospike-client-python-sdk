# Operation Builders

Factories for prebuilt operations, passed to `add_operation()` on a write
segment or a background task. The chainable bin methods on the builders
(documented under [QueryBuilder](query.md),
[CdtReadBuilder](cdt-read.md) and [CdtWriteBuilder](cdt-write.md)) cover the
common cases; reach for a factory when you hold an operation built elsewhere,
or need one the chain does not spell.

```python
from aerospike_sdk import MapOperation, MapOrder, MapPolicy

await (
    session.upsert(profiles.id("u1"))
    .add_operation(MapOperation.put("scores", "math", 97, MapPolicy(MapOrder.KEY_ORDERED)))
    .execute()
)
```

| Factory | Operations on | Policy and flag types it takes |
|---------|---------------|--------------------------------|
| `Operation` | whole bins and the record | |
| `ListOperation` | list bins | `ListPolicy`, `ListOrderType`, `ListWriteFlags`, `ListSortFlags`, `ListReturnType` |
| `MapOperation` | map bins | `MapPolicy`, `MapOrder`, `MapWriteMode`, `MapWriteFlags`, `MapReturnType` |
| `BitOperation` | bytes bins, bitwise | `BitPolicy`, `BitWriteFlags`, `BitwiseResizeFlags`, `BitwiseOverflowActions` |
| `HllOperation` | HyperLogLog bins | `HLLWriteFlags` |
| `ExpOperation` | bins, through an expression | `Exp`, `ExpReadFlags`, `ExpWriteFlags` |

[`StringOperation`](string-builder.md) and `CdtOperation` are the factories
for string and path operations. `HLLPolicy` is the policy the `Exp.hll_*`
expressions take. All of these import from `aerospike_sdk`.

```{eval-rst}
.. autoclass:: aerospike_sdk.Operation
   :members:

.. autoclass:: aerospike_sdk.ListOperation
   :members:

.. py:class:: aerospike_sdk.ListPolicy(order=None, write_flags=None)

   How a list write behaves. A required argument of the ``ListOperation``
   methods that add or change elements.

   :param order: A :class:`~aerospike_sdk.ListOrderType`: whether the list is
      kept sorted. ``None`` leaves it unordered.
   :param write_flags: A :class:`~aerospike_sdk.ListWriteFlags` member, or
      several combined with ``|``. ``None`` applies no flags.

   .. py:attribute:: order
      :type: ListOrderType

   .. py:attribute:: write_flags
      :type: int

.. autoclass:: aerospike_sdk.MapOperation
   :members:

.. py:class:: aerospike_sdk.MapPolicy(order=None, write_mode=None, flags=None, persist_index=None)

   How a map write behaves. A required argument of the ``MapOperation``
   methods that add or change entries.

   :param order: A :class:`~aerospike_sdk.MapOrder`: how the map is ordered.
      ``None`` leaves it unordered.
   :param write_mode: A :class:`~aerospike_sdk.MapWriteMode`. ``None`` is
      ``UPDATE``.
   :param flags: A :class:`~aerospike_sdk.MapWriteFlags` member, or several
      combined with ``|``. The flag form of *write_mode*; it wins when both
      are given.
   :param persist_index: Whether the server stores the map's index with the
      map, which speeds lookups at the cost of storage. Applies to a
      top-level ordered map. ``None`` is ``False``.

   .. py:attribute:: order
      :type: MapOrder

   .. py:attribute:: write_mode
      :type: MapWriteMode

   .. py:attribute:: flags
      :type: MapWriteFlags

   .. py:attribute:: persist_index
      :type: bool

.. py:class:: aerospike_sdk.MapWriteMode

   What a map write does when the key is, or is not, already there.

   .. py:attribute:: UPDATE

      Overwrite the entry if the key exists; create it if not. The default.

   .. py:attribute:: UPDATE_ONLY

      Overwrite the entry if the key exists; fail if it does not.

   .. py:attribute:: CREATE_ONLY

      Create the entry if the key does not exist; fail if it does.

.. autoclass:: aerospike_sdk.BitOperation
   :members:

.. autoclass:: aerospike_sdk.HllOperation
   :members:

.. autoclass:: aerospike_sdk.HLLPolicy
   :members:

.. autoclass:: aerospike_sdk.ExpOperation
   :members:

.. autoclass:: aerospike_sdk.ExpReadFlags
   :members:
   :undoc-members:

.. autoclass:: aerospike_sdk.ExpWriteFlags
   :members:
   :undoc-members:
```
