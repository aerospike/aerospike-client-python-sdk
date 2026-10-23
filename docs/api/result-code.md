# ResultCode

Every {class}`~aerospike_sdk.exceptions.AerospikeError` carries a
`ResultCode` in its `result_code` attribute, and
{class}`~aerospike_sdk.record_result.RecordResult` exposes the same code
per record in a batch or query stream. Non-negative codes come from the
server. Negative codes are assigned by the client when a request never
received a server answer, such as `MAX_RETRIES_EXCEEDED` or
`SERVER_NOT_AVAILABLE`.

Branch on the code, not on the exception type, when the distinction
matters: the same exception class can carry different codes depending on
which part of the request failed.

```python
from aerospike_sdk import ResultCode
from aerospike_sdk.exceptions import AerospikeError

try:
    await session.insert(users.id("user-123")).put({"name": "Alice"}).execute()
except AerospikeError as exc:
    if exc.result_code == ResultCode.KEY_EXISTS_ERROR:
        ...                          # record already present
    else:
        raise

ResultCode.KEY_NOT_FOUND_ERROR.name          # "KEY_NOT_FOUND_ERROR"
ResultCode.KEY_NOT_FOUND_ERROR.value         # 2
ResultCode.KEY_NOT_FOUND_ERROR.description   # "Key not found"
ResultCode.KEY_NOT_FOUND_ERROR == 2          # True
```

```{eval-rst}
.. autoclass:: aerospike_sdk.ResultCode
   :members:
   :undoc-members:
   :show-inheritance:
```
