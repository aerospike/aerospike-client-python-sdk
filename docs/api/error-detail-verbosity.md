# ErrorDetailVerbosity

How much extended detail the server attaches to a failed operation. Set it
through the `error_detail_verbosity` field of
{class}`~aerospike_sdk.policy.behavior_settings.Settings` on a
{class}`~aerospike_sdk.policy.behavior.Behavior`. The default, `NONE`,
adds nothing to the wire, so sessions that never raise it pay no cost.

| Member | Value | Attaches to the error |
|--------|-------|-----------------------|
| `NONE` | 0 | Nothing (default) |
| `SUBCODE` | 1 | The numeric `sub_code` |
| `MESSAGE` | 2 | `sub_code` plus the human-readable `server_message` |
| `EXPRESSION_TRACE` | 3 | The above plus an {class}`~aerospike_sdk.ExpressionTrace` when an expression fails to build |

Each level is a superset of the one before it. The detail surfaces on the
raised {class}`~aerospike_sdk.exceptions.AerospikeError` as `sub_code`,
`server_message`, and `exp_trace`, each `None` when the server supplied
nothing. Servers before 8.2.0 ignore the request.

```python
from aerospike_sdk import Behavior, ErrorDetailVerbosity, SubCode
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from aerospike_sdk.exceptions import AerospikeError

behavior = Behavior(
    "verbose-errors",
    {Scope.ALL: Settings(error_detail_verbosity=ErrorDetailVerbosity.MESSAGE)},
)
session = cluster.create_session(behavior)

try:
    stream = await session.query(users.id(1)).bin("scores").on_list_index(99).get_values().execute()
    await stream.first_or_raise()
except AerospikeError as err:
    if err.sub_code == SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS:
        print(err.server_message)      # e.g. "list index 99 out of bounds"
```

See the [Error Handling guide](../guide/error-handling.md) for the full
subcode catalog and the
[Dynamic SDK Config guide](../guide/dynamic-sdk-config.md) for setting it
from YAML.

```{eval-rst}
.. autoclass:: aerospike_sdk.ErrorDetailVerbosity
   :show-inheritance:
```
