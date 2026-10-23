# Key

A `Key` identifies one record: a namespace, a set, and either the user key
you supplied or the 20-byte digest the server derived from it. Build keys
through {meth}`DataSet.id() <aerospike_sdk.dataset.DataSet.id>` rather than
the constructor, so the namespace and set are spelled once.

Keys compare and hash by digest, so two keys built from the same namespace,
set, and user key are interchangeable as dict keys and set members, and a
key rebuilt from a digest alone with {meth}`~aerospike_sdk.Key.key_with_digest`
is equal to the original.

The SDK hands keys back on every per-record result as
{attr}`RecordResult.key <aerospike_sdk.record_result.RecordResult.key>`.
On a point read that is the key you asked for. On a query or scan it is
the key the server returned, whose `value` is populated only when the
record was written with `send_key` enabled; otherwise only the digest is
known and `value` is `None`.

```python
from aerospike_sdk import DataSet, Key

users = DataSet.of("test", "users")
key = users.id("user-123")           # same as Key("test", "users", "user-123")

key.namespace                        # "test"
key.set_name                         # "users"
key.value                            # "user-123"
key.digest                           # 40-char hex string of the 20-byte digest
key.partition_id                     # 0..4095, derived from the digest

same = Key.key_with_digest("test", "users", key.digest)
same == key                          # True; same.value is None
```

## Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `namespace` | `str` | Namespace the record lives in |
| `set_name` | `str` | Set within the namespace |
| `value` | `str \| int \| bytes \| None` | The user key, or `None` when only the digest is known |
| `digest` | `str` | The 20-byte RIPEMD-160 digest as a 40-character hex string |
| `partition_id` | `int` | Partition (0 to 4095) that the digest maps to |

```{eval-rst}
.. autoclass:: aerospike_sdk.Key
   :members: namespace, set_name, value, digest, partition_id, key_with_digest
   :undoc-members:
   :exclude-members: from_int_user_key
   :show-inheritance:
```
