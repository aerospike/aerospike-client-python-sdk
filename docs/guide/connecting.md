# Connecting to a Cluster

## Basic Connection

The simplest way to connect is with a host and port:

::::{tab-set}

:::{tab-item} Async
```python
from aerospike_sdk import Client

async with Client("localhost:3000") as client:
    session = client.create_session()
    # ... use session ...
```
:::

:::{tab-item} Sync
```python
from aerospike_sdk import SyncClient

with SyncClient("localhost:3000") as client:
    session = client.create_session()
    # ... use session ...
```
:::

::::

Both clients support context managers that automatically close the connection on exit.

## ClusterDefinition

For advanced configuration, use `ClusterDefinition`:

```python
from aerospike_sdk import ClusterDefinition

cluster_def = (
    ClusterDefinition("localhost", 3000)
    .with_native_credentials("username", "password")
    .using_services_alternate()
    .with_ip_map({"10.0.0.1": "3.72.54.187"})
)

async with cluster_def.connect() as client:
    # ...
```

### TLS

Server-side TLS with CA certificate verification:

```python
cluster = (
    ClusterDefinition("localhost", 4333)
    .with_tls_config_of()
        .tls_name("myTlsName")
        .ca_file("/path/to/ca.pem")
    .done()
    .with_native_credentials("username", "password")
    .using_services_alternate()
)

async with await cluster.connect() as c:
    session = c.create_session()
    # ... use session ...
```

Mutual TLS (mTLS) with client certificate authentication:

```python
cluster = (
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

async with await cluster.connect() as c:
    session = c.create_session()
    # ... use session ...
```

You can also configure TLS directly on a `Client` by passing a `ClientPolicy`
with `tls_config` set:

```python
from aerospike_async import ClientPolicy, TlsConfig, AuthMode
from aerospike_sdk import Client

policy = ClientPolicy()
policy.tls_config = TlsConfig("/path/to/ca.pem")
policy.set_auth_mode(AuthMode.INTERNAL, user="admin", password="admin")
policy.use_services_alternate = True

async with Client("localhost:myTlsName:4333", policy) as client:
    session = client.create_session()
    # ... use session ...
```

:::{note}
When using TLS, the seed string format is `host:tls_name:port` (three parts).
The `tls_name` must match the server's configured TLS name for certificate
validation. With `ClusterDefinition`, setting `tls_name()` on the builder
automatically applies it to all hosts.
:::

### Rack Awareness

```python
cluster = (
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
cluster = (
    ClusterDefinition("localhost", 3000)
    .app_id("billing-service")
    .connect()
)
```

## Sessions

A **Session** binds a connected client to a set of policy defaults via a
[`Behavior`](../api/behavior.md). All reads and writes go through a session.

```python
from aerospike_sdk import Behavior

session = client.create_session(Behavior.DEFAULT)
fast_session = client.create_session(Behavior.READ_FAST)
consistent_session = client.create_session(Behavior.STRICTLY_CONSISTENT)
```

:::{tip}
Create different sessions for different workloads. A "fast read" session with
short timeouts and a "batch import" session with longer timeouts can coexist
on the same client.
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
