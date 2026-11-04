# Seed Discovery and Address Translation

Two connect-time settings for clusters whose addresses are not fixed or not
directly reachable:

- **Seed discovery** finds the seed hosts at connect, and keeps finding them
  while the client runs, from DNS, AWS EC2 tags, or a provider you write.
- **Address translation** rewrites the addresses nodes advertise to each
  other before the client connects to them, for clients outside the
  cluster's network (Kubernetes, NAT, a load balancer port per node).

Both are set on the `ClusterDefinition` or in the
[SDK config file](dynamic-sdk-config.md), and both are read at `connect()`.

## Seed discovery

```python
from aerospike_sdk import ClusterDefinition, DnsRecordType, DnsSeedCandidateProvider

cluster_def = ClusterDefinition(hosts=[]).with_seed_discovery(
    DnsSeedCandidateProvider(
        ["_aerospike._tcp.prod.example.com"], record_type=DnsRecordType.SRV,
    ),
)

async with cluster_def.connect() as cluster:
    ...
```

The provider's hosts combine with the hosts the definition was created with,
per `merge_policy`:

| `merge_policy` | Seeds tried |
|---|---|
| `MERGE` (default) | the definition's, then the provider's |
| `REPLACE` | both at connect; afterwards each refresh's hosts replace the list |
| `DISCOVERY_ONLY` | the provider's, falling back to the definition's when it returns none |

Create the definition with `hosts=[]` to rely on the provider alone. A refresh
that fails or returns nothing keeps the previous seeds, so a discovery outage
never empties a running client's seed list. When the provider fails at
connect and no seed is left to try, `connect()` raises `SeedDiscoveryError`.

### Providers

| Provider | Finds seeds from | Refreshes |
|---|---|---|
| `StaticSeedCandidateProvider` | a fixed list | never |
| `DnsSeedCandidateProvider` | A or SRV records | on the cluster tend task |
| `AwsEc2TagSeedCandidateProvider` | running EC2 instances matching tag filters | on the cluster tend task |
| a `SeedCandidateProvider` subclass | your code | on a background thread |

The AWS provider signs its own `DescribeInstances` request. It resolves
credentials from `credentials=`, or else the environment, the shared profile,
EKS web identity, ECS, and the instance metadata service, in that order. A
`credentials_hook` replaces that chain. The hook is called on the refreshing
task, so it must return credentials obtained elsewhere rather than call AWS
itself:

```python
from aerospike_sdk import AwsCredentials, AwsEc2TagSeedCandidateProvider

provider = AwsEc2TagSeedCandidateProvider(
    "us-west-2",
    {"tag:aerospike-cluster": ["prod"]},
    credentials_hook=lambda: AwsCredentials(
        role.access_key_id, role.secret_access_key, role.session_token, role.expiration,
    ),
)
```

A provider of your own subclasses `SeedCandidateProvider`. It runs on a thread
the client starts for it, so `refresh_seed_candidates` may block:

```python
from datetime import timedelta

from aerospike_native import Host
from aerospike_sdk import ClusterDefinition, SeedCandidateProvider


class RegistryProvider(SeedCandidateProvider):
    def __init__(self, registry):
        self.registry = registry

    def refresh_seed_candidates(self, ctx):
        return [Host(node.address, node.port) for node in self.registry.lookup("aerospike")]

    def needs_periodic_refresh(self):
        return True


cluster_def = ClusterDefinition("10.0.0.1", 3000).with_seed_discovery(
    RegistryProvider(registry), refresh_timeout=timedelta(seconds=5),
)
```

### Execution mode

Each provider runs in one place in this client: DNS and AWS refresh on the
cluster tend task, within `tend_refresh_deadline`, and a `SeedCandidateProvider`
subclass refreshes on its own thread, within `refresh_timeout`. The
`execution_mode` argument therefore only checks: `DiscoveryExecutionMode.TEND`
with a Python provider, or `THREAD` with DNS or AWS, raises `AerospikeError`
with `ResultCode.PARAMETER_ERROR`. Leave it unset to take the provider's own.

| Setting | Default | Applies to |
|---|---|---|
| `refresh_interval` | 30 s | every periodic provider; at least the tend interval |
| `refresh_timeout` | 10 s | `SeedCandidateProvider` subclasses |
| `tend_refresh_deadline` | 100 ms | providers on the tend task; at most half the tend interval |

## Address translation

```python
from aerospike_sdk import ClusterDefinition, PortMapAddressTranslator, TranslationMissMode

cluster_def = ClusterDefinition("lb.example.com", 30001).with_address_translation(
    PortMapAddressTranslator({
        "10.0.0.1:3000": "lb.example.com:30001",
        "10.0.0.2:3000": "lb.example.com:30002",
    }),
    on_miss=TranslationMissMode.STRICT,
)
```

| Translator | Rewrites |
|---|---|
| `StaticMapAddressTranslator` | advertised host to host, keeping the port; what `ip_map()` builds |
| `PortMapAddressTranslator` | advertised `host:port` to `host:port` |
| `PassThroughAddressTranslator` | nothing |

A translator replaces `ip_map()`. `on_miss` decides what happens to an
address the translator has no mapping for: `PASS_THROUGH` (the default)
connects to it as advertised, `STRICT` skips that node. Calling
`with_address_translation(on_miss=...)` without a translator keeps `ip_map()`.

## In the SDK config file

Both settings have a block in each `system:` profile. The keys use
milliseconds, as the spelling shared across Aerospike SDKs does:

```yaml
system:
  production:
    discovery:
      provider: dns              # static (default), dns or aws-tags
      merge_policy: discovery_only
      execution_mode: tend       # checked like the argument in code
      refresh_interval_ms: 30000
      refresh_timeout_ms: 10000
      tend_refresh_deadline_ms: 100
      dns:
        names: [_aerospike._tcp.prod.example.com]
        record_type: srv         # a (default) or srv
        port: 3000
        tls_name: prod
        timeout_ms: 80
        servers: ["10.0.0.53:53"]
    translation:
      provider: port-map         # static-map (default), port-map or pass-through
      on_miss: strict            # pass_through (default) or strict
      port_map:
        "10.0.0.1:3000": "lb.example.com:30001"
```

The AWS provider takes an `aws` block instead of `dns`:

```yaml
    discovery:
      provider: aws-tags
      aws:
        region: us-west-2
        filters:
          - {name: "tag:aerospike-cluster", values: [prod]}
        address_field: private_ip  # private_ip, public_ip, private_dns or public_dns
        port: 3000
        timeout_ms: 80
        init_timeout_ms: 10000
```

The file layers over the code per field, like every other `system:` setting:
a block that sets only `merge_policy` keeps the provider passed in code.
`provider: static` clears a provider set in code. In `translation`, a
`static_map` without `provider` means `static-map`; `provider: static-map`
without a map restores `ip_map()`; and a block with neither keeps the
translator from code and sets only `on_miss`.

An invalid value is handled like any other in the file: it is skipped with a
warning and the rest of the file applies. A provider or translator is one
setting with its option block, so an invalid option skips the whole provider
rather than build half of one. An `execution_mode` its provider does not run
in is skipped the same way. Unknown keys warn, and fail the connect under
`with_strict_config()`.

Both blocks are read at connect. A reloaded file that changes them logs a
warning that the change takes effect when the client reconnects.

## Errors

| Exception | Result code | Raised when |
|---|---|---|
| `SeedDiscoveryError` | `SEED_DISCOVERY_FAILED` | the provider fails at connect and no seed is left |
| `AddressTranslationError` | `TRANSLATION_MISS`, `TRANSLATION_COLLISION` | a translation failure reaches the caller |

Both are `ConnectionError` subclasses. A translation miss under `STRICT`, or
two nodes translated to one address, skips the node and logs why, so these
failures usually show in the log rather than as an exception.
