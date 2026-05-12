# API Reference

The Aerospike Python SDK is organized into three layers:

## Async API

The primary API. All operations are `async`/`await`.

| Class | Description |
|-------|-------------|
| [`Client`](client.md) | Entry point — connect, create sessions, manage lifecycle |
| [`Cluster`](cluster.md) | Cluster handle returned by `Client` |
| [`Session`](session.md) | Scoped reads and writes with a fixed `Behavior` |
| [`QueryBuilder`](query.md) | Build and execute read queries (point, set, batch) |
| [`WriteSegmentBuilder`](write-segment.md) | Build and execute writes (upsert, insert, update, replace, delete) |
| [`CdtReadBuilder`](cdt-read.md) | Read operations on list and map CDTs |
| [`CdtWriteBuilder`](cdt-write.md) | Write operations on list and map CDTs |
| [`BatchOperationBuilder`](batch.md) | Low-level batch operation builder |
| [`IndexBuilder`](index-builder.md) | Create and drop secondary indexes |
| [`BackgroundTaskSession`](background.md) | Server-side background jobs (update, delete, touch, UDF) |
| [`UdfFunctionBuilder`](udf.md) | Foreground UDF execution |
| [`InfoCommands`](info.md) | Aerospike info protocol commands |
| [`TransactionalSession`](transactional-session.md) | Multi-record transactions |

## Sync API

Synchronous wrappers for the async API. Same functionality, no `async`/`await`.

| Class | Description |
|-------|-------------|
| [`SyncClient`](sync/client.md) | Sync entry point |
| [`Cluster`](sync/cluster.md) | Sync cluster handle |
| [`SyncSession`](sync/session.md) | Sync session |
| [`SyncQueryBuilder`](sync/query.md) | Sync query builder |
| [`SyncWriteSegmentBuilder`](sync/write-segment.md) | Sync write builder (upsert, insert, update, replace, delete) |
| [`SyncBatchOperationBuilder`](sync/batch.md) | Sync batch operation builder |
| [`SyncIndexBuilder`](sync/index-builder.md) | Sync secondary index builder |
| [`SyncBackgroundTaskSession`](sync/background.md) | Sync server-side background jobs |
| [`SyncUdfFunctionBuilder`](sync/udf.md) | Sync foreground UDF execution |
| [`SyncInfoCommands`](sync/info.md) | Sync info protocol commands |
| [`SyncTransactionalSession`](sync/transactional-session.md) | Sync multi-record transactions |
| [`SyncRecordStream`](sync/record-stream.md) | Sync iterator over query results |
| [`ClusterDefinition`](sync/cluster-definition.md) | Sync cluster connection configuration |
| [`TlsBuilder`](sync/tls-builder.md) | Sync TLS configuration builder |

## Core

Shared types used by both async and sync APIs.

| Class | Description |
|-------|-------------|
| [`DataSet`](dataset.md) | Namespace + set pair, key factory |
| [`HllConfig`](hll-config.md) | HyperLogLog bin precision (index + minhash bit counts) |
| [`RecordResult`](record-result.md) | Single result from a query or batch |
| [`OperationResult`](operation-result.md) | Typed-accessor wrapper around a single operation's value |
| [`RecordStream`](record-stream.md) | Async iterator over query results |
| [`Behavior`](behavior.md) | Policy presets (timeouts, consistency) |
| [`ClusterDefinition`](cluster-definition.md) | Cluster connection configuration |
| [`TlsBuilder`](tls-builder.md) | TLS configuration builder |
| [`SystemSettings`](system-settings.md) | Global system-level tunables |
| [`ErrorStrategy`](error-strategy.md) | Error handling strategies |
| [`Exceptions`](exceptions.md) | Exception hierarchy |
| [`QueryHint`](query-hint.md) | Query optimization hints |
| [`IndexesMonitor`](indexes-monitor.md) | Background secondary index discovery |

## AEL

Aerospike Expression Language parsing and filter generation.

| Class / Function | Description |
|-----------------|-------------|
| [`parse_ael`](ael-parser.md) | Parse AEL strings into filter expressions |
| [`FilterGenerator`](ael-filter-gen.md) | Secondary index filter generation |
| [`Exp`](exp.md) | Programmatic expression builder |

```{toctree}
:hidden:
:maxdepth: 1

client
cluster
session
query
write-segment
cdt-read
cdt-write
batch
index-builder
background
udf
info
transactional-session
sync/client
sync/cluster
sync/session
sync/query
sync/write-segment
sync/batch
sync/transactional-session
sync/record-stream
sync/cluster-definition
sync/index-builder
sync/background
sync/info
sync/udf
sync/tls-builder
dataset
hll-config
record-result
operation-result
record-stream
behavior
cluster-definition
tls-builder
system-settings
error-strategy
exceptions
query-hint
indexes-monitor
ael-parser
ael-filter-gen
exp
```
