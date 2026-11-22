# Selector

Names the operations an `on` entry of
{meth}`~aerospike_sdk.policy.behavior.Behavior.derive_with_changes` applies
to: a kind of operation, optionally narrowed by shape, retryability and
namespace mode, or one of the two transaction phases.

```python
from aerospike_sdk import Behavior
from aerospike_sdk.policy import ReadModeAP, Selector, Settings

thorough_batch_reads = Behavior.DEFAULT.derive_with_changes(
    "thorough-batch-reads",
    on=[(Selector.reads().batch().ap(), Settings(read_mode_ap=ReadModeAP.ALL))],
)
```

| Start from | Narrow with |
|------------|-------------|
| `Selector.reads()` | `point()`, `batch()`, `query()`; `ap()`, `sc()` |
| `Selector.writes()` | `retryable()`, `non_retryable()`; `point()`, `batch()`, `query()`; `ap()`, `sc()` |
| `Selector.all()` | nothing: every operation and both transaction phases |
| `Selector.txn_verify()`, `Selector.txn_roll()` | nothing: one transaction phase |

See [Targeting a combination](../guide/connecting.md) in the connecting guide
for the order entries apply in.

```{eval-rst}
.. autoclass:: aerospike_sdk.policy.Selector
   :members:

.. autoclass:: aerospike_sdk.policy.ReadSelector
   :members:
   :show-inheritance:

.. autoclass:: aerospike_sdk.policy.WriteSelector
   :members:
   :show-inheritance:
```
