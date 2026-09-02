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

## SetDetail

Returned by {meth}`~aerospike_sdk.aio.info.InfoCommands.sets` and
{meth}`~aerospike_sdk.sync.info.InfoCommands.sets`, one per set in a namespace, or singly
by {meth}`~aerospike_sdk.aio.info.InfoCommands.set`.

```python
for detail in await session.info().sets("customers"):
    if detail.objects > 1_000_000:
        print(f"{detail.name}: {detail.objects} records, {detail.data_used_bytes} bytes")
```

The wire spells the identity fields `ns` and `set`; both are reachable under the SDK's
names (`namespace`, `name`) as well as their raw keys. Counters come from the first node
reporting each set, so they describe that node's share rather than a cluster-wide total —
the same contract the underlying info command has.

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.SetDetail
   :members:
   :show-inheritance:
```

## Sindex

Returned by {meth}`~aerospike_sdk.aio.info.InfoCommands.secondary_indexes` and
{meth}`~aerospike_sdk.sync.info.InfoCommands.secondary_indexes`, one per index.

```python
for index in await session.info().secondary_indexes("customers"):
    if not index.is_ready:
        print(f"{index.name} is still building")
```

Keys are the SDK's normalized names — `namespace`, `set`, `bin`, `name` — rather than the
wire's `ns` / `indexname` / `indextype`. `index_type` is the value type indexed
(`numeric`, `string`, `geo2dsphere`, `blob`); `collection_type` is the collection walked
(`default`, `list`, `mapkeys`, `mapvalues`).

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.Sindex
   :members:
   :show-inheritance:
```

## SindexDetail

Returned by {meth}`~aerospike_sdk.aio.info.InfoCommands.secondary_index_details` and
{meth}`~aerospike_sdk.sync.info.InfoCommands.secondary_index_details`, or `None` when the
index does not exist.

```python
detail = await session.info().secondary_index_details("customers", "by_age")
if detail is not None and not detail.is_ready:
    print(f"{detail.load_pct}% built")
```

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.SindexDetail
   :members:
   :show-inheritance:
```

## StorageEngine

The `storage-engine` section of a namespace response, reached from
{attr}`~aerospike_sdk.info_types.NamespaceDetail.storage_engine`.

```python
engine = (await session.info().namespace_details("customers")).storage_engine
if engine.is_memory:
    print("memory-backed; data does not survive a restart")
else:
    for storage_file in engine.files:
        print(storage_file.path, storage_file.used_bytes, storage_file.free_wblocks)
```

The wire reports this section flat and prefixed (`storage-engine=device`,
`storage-engine.defrag-lwm-pct=50`); the view holds those keys with the prefix removed, so
the section reads as a unit.

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.StorageEngine
   :members:
   :show-inheritance:
```

## Per-node variants

`namespace_details`, `set_details`, `secondary_indexes`, and `secondary_index_details` each
give one answer for the cluster. The `*_per_node` variants keep each node's own answer,
keyed by node name, which is what shows disagreement between nodes:

```python
per_node = await session.info().sets_per_node("customers")
for node, details in per_node.items():
    print(node, sum(d.objects for d in details))
# BB95E3E 254
# BB99519 221
# BB902E9 253
```

Object counts legitimately differ per node, so the merged {meth}`set_details` view reports
the first node's numbers rather than a cluster-wide total. Use the per-node variant when
the distribution matters, or to check whether a config change or index rebuild has reached
every node. Nodes that do not host the namespace or index are omitted rather than mapped to
an empty view.

## StorageFileDetail

One backing file or device, returned from {attr}`~aerospike_sdk.info_types.StorageEngine.files`.

The wire flattens these into the namespace response — `storage-engine.file[0]` names the
path and `storage-engine.file[0].free_wblocks` counts against it — with the index as the
only thing tying a counter to its file. This view is that group reassembled.

```python
for storage_file in (await session.info().namespace_details("customers")).storage_engine.files:
    if storage_file.read_errors:
        print(f"{storage_file.path}: {storage_file.read_errors} read errors")
```

```{eval-rst}
.. autoclass:: aerospike_sdk.info_types.StorageFileDetail
   :members:
   :show-inheritance:
```
