# API Reference

The Aerospike Python SDK is organized into three layers:

## Async API

The primary API. All operations are `async`/`await`.

| Class | Description |
|-------|-------------|
| [`ClusterDefinition`](cluster-definition.md) | Entry point — configure seeds/auth/TLS, then `connect()` |
| [`AsyncPool`](async-pool.md) | Multi-loop async pool — N event loops × N cluster members for parallel async work (free-threaded Python) |
| [`Cluster`](cluster.md) | Live cluster connection returned by `ClusterDefinition.connect()` |
| [`Session`](session.md) | Scoped reads and writes with a fixed `Behavior` |
| [`QueryBuilder`](query.md) | Build and execute read queries (point, set, batch) |
| [`WriteSegmentBuilder`](write-segment.md) | Build and execute writes (upsert, insert, update, replace, delete) |
| [`CdtReadBuilder`](cdt-read.md) | Read operations on list and map CDTs |
| [`CdtWriteBuilder`](cdt-write.md) | Write operations on list and map CDTs |
| [`StringOperation`](string-builder.md) | Server-side string operation factory + flag types (8.1.3+) |
| [`IndexBuilder`](index-builder.md) | Create and drop secondary indexes |
| [`BackgroundTaskSession`](background.md) | Server-side background jobs (update, delete, touch, UDF) |
| [`UdfFunctionBuilder`](udf.md) | Foreground UDF execution |
| [`InfoCommands`](info.md) | Aerospike info protocol commands |
| [`TransactionalSession`](transactional-session.md) | Multi-record transactions |
| [`Client`](client.md) | Low-level connection primitive (deprecated — use `ClusterDefinition`) |

## Sync API

Synchronous wrappers for the async API. Same functionality, no `async`/`await`.

| Class | Description |
|-------|-------------|
| [`ClusterDefinition`](sync/cluster-definition.md) | Sync entry point — configure, then `connect()` |
| [`Cluster`](sync/cluster.md) | Sync cluster handle |
| [`Session`](sync/session.md) | Sync session |
| [`QueryBuilder`](sync/query.md) | Sync query builder |
| [`WriteSegmentBuilder`](sync/write-segment.md) | Sync write builder (upsert, insert, update, replace, delete) |
| [`IndexBuilder`](sync/index-builder.md) | Sync secondary index builder |
| [`SyncBackgroundTaskSession`](sync/background.md) | Sync server-side background jobs |
| [`UdfFunctionBuilder`](sync/udf.md) | Sync foreground UDF execution |
| [`InfoCommands`](sync/info.md) | Sync info protocol commands |
| [`TransactionalSession`](sync/transactional-session.md) | Sync multi-record transactions |
| [`RecordStream`](sync/record-stream.md) | Sync iterator over query results |
| [`SyncClient`](sync/client.md) | Low-level connection primitive (deprecated — use `ClusterDefinition`) |
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
| [`ExpressionTrace`](expression-trace.md) | Structured expression build trace on `AerospikeError` (verbosity 3) |
| [`QueryHint`](query-hint.md) | Query optimization hints |
| [`SdkLoggers`](loggers.md) | Stable logger names for operator tuning |

## Expressions

| Class / Function | Description |
|-----------------|-------------|
| [`Exp`](exp.md) | Programmatic expression builder (all server versions) |

String AEL for `.where()` is compiled on the server (field 43) when the cluster
supports it — see the [AEL guide](../guide/expression-ael.md).

```{toctree}
:hidden:
:maxdepth: 1

client
async-pool
cluster
session
query
write-segment
cdt-read
cdt-write
string-builder
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
expression-trace
exceptions
query-hint
loggers
exp
```
