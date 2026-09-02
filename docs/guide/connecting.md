# Connecting to a Cluster

## Basic Connection

`ClusterDefinition` is the entry point for every connection. Connect it to
obtain a `Cluster`, then create sessions from that cluster:

::::{tab-set}

:::{tab-item} Async
```python
from aerospike_sdk import ClusterDefinition

async with await ClusterDefinition("localhost", 3000).connect() as cluster:
    session = cluster.create_session()
    # ... use session ...
```
:::

:::{tab-item} Sync
```python
from aerospike_sdk.sync import ClusterDefinition

with ClusterDefinition("localhost", 3000).connect() as cluster:
    session = cluster.create_session()
    # ... use session ...
```
:::

::::

The `Cluster` returned by `connect()` supports the context-manager protocol,
which automatically closes the connection on exit.

## Advanced Configuration

`ClusterDefinition` exposes a fluent builder for credentials, alternate-access,
IP mapping, TLS, rack awareness, and more:

```python
from aerospike_sdk import ClusterDefinition

cluster_def = (
    ClusterDefinition("localhost", 3000)
    .with_native_credentials("username", "password")
    .using_services_alternate()
    .with_ip_map({"10.0.0.1": "3.72.54.187"})
)

async with await cluster_def.connect() as cluster:
    session = cluster.create_session()
    # ...
```

### Alternate access addresses

`using_services_alternate()` makes peer discovery follow each node's
`alternate-access-address` rather than its standard service address. Enable it
only against a cluster that publishes those addresses: against one that does
not, the peer list comes back empty, the client falls back to a single node,
and every key outside that node's partitions fails to route.

The setting **defaults to on** whenever `AEROSPIKE_USE_SERVICES_ALTERNATE` is
truthy in the environment, so pass `False` explicitly to force it off
regardless of the environment:

```python
cluster_def = ClusterDefinition("localhost", 3000).using_services_alternate(False)
```

### Restricting the cluster to its seeds

`restricting_cluster_to_seeds()` disables peer discovery entirely: the seed
addresses become the whole cluster, and nodes advertised by `peers` are
ignored. Seeds are also kept across connection failures rather than being
dropped by the tend loop, and a seed address is treated as the canonical
service endpoint rather than resolved to a backend node.

```python
# Behind a load balancer or VIP that fronts the cluster.
cluster_def = ClusterDefinition("aerospike-vip", 3000).restricting_cluster_to_seeds()
```

Reach for it when the addresses the cluster advertises are not routable from
the client at all — a fixed VIP or proxy in front of the cluster — or when a
test or benchmark needs a node set that cannot shift as tending discovers and
drops peers.

This is a different remedy from `using_services_alternate()`, and they solve
different problems: alternate addresses still discover the peers and then
address each one by its alternate address, while this stops the client
contacting peers at all. Neither implies the other, and on a multi-node
cluster the difference is visible — a client restricted to one seed reports
one node where discovery would report all of them.

### TLS

Server-side TLS with CA certificate verification:

```python
cluster_def = (
    ClusterDefinition("localhost", 4333)
    .with_tls_config_of()
        .tls_name("myTlsName")
        .ca_file("/path/to/ca.pem")
    .done()
    .with_native_credentials("username", "password")
    .using_services_alternate()
)

async with await cluster_def.connect() as cluster:
    session = cluster.create_session()
    # ... use session ...
```

Mutual TLS (mTLS) with client certificate authentication:

```python
cluster_def = (
    ClusterDefinition("localhost", 4333)
    .with_tls_config_of()
        .tls_name("myTlsName")
        .ca_file("/path/to/ca.pem")
        .client_cert_file("/path/to/client-cert.pem")
        .client_key_file("/path/to/client-key.pem")
    .done()
    .with_native_credentials("username", "password")
    .using_services_alternate()
)

async with await cluster_def.connect() as cluster:
    session = cluster.create_session()
    # ... use session ...
```

:::{note}
The `tls_name` must match the server's configured TLS name for certificate
validation. Setting `tls_name()` on the `ClusterDefinition` builder
automatically applies it to all hosts.
:::

### Rack Awareness

```python
cluster = await (
    ClusterDefinition("localhost", 3000)
    .preferring_racks(1, 2)
    .connect()
)
```

### Application Identifier

Tag this client's traffic with an application identifier via `app_id`. The
server records it (as part of the client's user-agent), letting operators
attribute load per calling application. It is separate from the client-library
identifier the SDK reports automatically.

```python
cluster = await (
    ClusterDefinition("localhost", 3000)
    .app_id("billing-service")
    .connect()
)
```

### SDK Configuration File

Connection pool sizing, circuit-breaker thresholds, tend interval, and named
operation-policy behaviors can also come from a YAML file (the
`AEROSPIKE_SDK_CONFIG_URL` environment variable), read at `connect()` and
hot-reloaded on change. See [Dynamic SDK Configuration](dynamic-sdk-config.md).

## Checking Server Capabilities

Some features require a minimum server version. On a mixed-version or
mid-upgrade cluster you can guard a feature before using it, rather than
letting a call fail at runtime. The `Cluster` reports a feature as supported
only when *every* connected node supports it, so you always guard against the
cluster's least-capable node.

```python
async with await ClusterDefinition("localhost", 3000).connect() as cluster:
    if await cluster.supports_string_operations():
        await session.upsert(key).bin("s").str_append("!").execute()
    else:
        ...  # fall back

    version = await cluster.server_version()   # minimum Version across the cluster
    if version is not None and (version.major, version.minor, version.patch) >= (8, 1, 3):
        ...
```

The probes are `supports_ael()`, `supports_query_operations()`,
`supports_string_operations()`, `supports_query_selection()`, and
`server_version()` (the minimum version across all nodes, or `None` if the
cluster reports no nodes). They are `async` on the async `Cluster` and plain
methods on the sync
`Cluster`; each reads live, so the answer reflects current cluster membership.

## Sessions

A **Session** binds a connected cluster to a set of policy defaults via a
[`Behavior`](../api/behavior.md). All reads and writes go through a session.

```python
from aerospike_sdk import Behavior

session = cluster.create_session(Behavior.DEFAULT)
fast_session = cluster.create_session(Behavior.READ_FAST)
consistent_session = cluster.create_session(Behavior.STRICTLY_CONSISTENT)
```

:::{tip}
Create different sessions for different workloads. A "fast read" session with
short timeouts and a "batch import" session with longer timeouts can coexist
on the same cluster.
:::

## Behaviors

Predefined behaviors:

| Behavior | Description |
|----------|-------------|
| `Behavior.DEFAULT` | Balanced defaults |
| `Behavior.READ_FAST` | Low-latency reads |
| `Behavior.STRICTLY_CONSISTENT` | Strong consistency mode |
| `Behavior.FAST_RACK_AWARE` | Prefer local rack for reads |

Custom behaviors via derivation:

```python
my_behavior = Behavior.DEFAULT.derive_with_changes(
    total_timeout_ms=5000,
    max_retries=3,
)
```

## DataSets

A `DataSet` represents a namespace + set pair and is a convenient key factory:

```python
from aerospike_sdk import DataSet

users = DataSet.of("test", "users")

key = users.id(42)                       # single key
keys = users.ids(1, 2, 3)               # list of keys
digest_key = users.id_from_digest(b"...")  # key from raw digest
```

Pass datasets to session methods to avoid repeating namespace/set strings:

```python
stream = await session.query(users).execute()
await session.upsert(users.id(1)).put({"name": "Alice"}).execute()
```
