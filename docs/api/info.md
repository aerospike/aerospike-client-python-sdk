# InfoCommands

```{eval-rst}
.. autoclass:: aerospike_sdk.aio.info.InfoCommands
   :members:
   :show-inheritance:
```

## NamespaceDetail

Returned by {meth}`~aerospike_sdk.aio.info.InfoCommands.namespace_details` and
{meth}`~aerospike_sdk.sync.info.InfoCommands.namespace_details`. A mapping of every key
the server reported for the namespace, with typed properties for the fields the SDK
consults — so raw-key access and typed access mix freely:

```python
detail = await session.info().namespace_details("customers")
if detail is None:
    raise RuntimeError("namespace 'customers' is not configured")

# Typed, coerced on access.
if detail.nsup_period == 0 and not detail.allow_ttl_without_nsup:
    print("record expiration is disabled; a TTL will be rejected")

# Any other reported key stays addressable by name.
replication = detail["replication-factor"]
```

Values are converted when a property is read rather than up front, so reading two fields
does not cost anything for the hundreds of others the response carries.

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.NamespaceDetail
   :members:
   :show-inheritance:
```
