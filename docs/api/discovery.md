# Seed Discovery and Address Translation

Providers, translators and settings for
{meth}`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.with_seed_discovery` and
{meth}`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.with_address_translation`.
See the [Seed Discovery and Address Translation guide](../guide/discovery.md).

## Seed candidate providers

```{eval-rst}
.. autoclass:: aerospike_sdk.DnsSeedCandidateProvider
   :members:

.. autoclass:: aerospike_sdk.DnsRecordType
   :members:
   :undoc-members:

.. autoclass:: aerospike_sdk.AwsEc2TagSeedCandidateProvider
   :members:

.. autoclass:: aerospike_sdk.AwsCredentials
   :members:

.. autoclass:: aerospike_sdk.AwsAddressField
   :members:
   :undoc-members:

.. autoclass:: aerospike_sdk.StaticSeedCandidateProvider
   :members:

.. autoclass:: aerospike_sdk.SeedCandidateProvider
   :members:

.. autoclass:: aerospike_sdk.SeedCandidateContext
   :members:

.. autoclass:: aerospike_sdk.SeedMergePolicy
   :members:
   :undoc-members:

.. autoclass:: aerospike_sdk.DiscoveryExecutionMode
   :members:
   :undoc-members:
```

## Address translators

```{eval-rst}
.. autoclass:: aerospike_sdk.StaticMapAddressTranslator
   :members:

.. autoclass:: aerospike_sdk.PortMapAddressTranslator
   :members:

.. autoclass:: aerospike_sdk.PassThroughAddressTranslator
   :members:

.. autoclass:: aerospike_sdk.TranslationMissMode
   :members:
   :undoc-members:
```

## Settings

```{eval-rst}
.. autoclass:: aerospike_sdk.policy.discovery_settings.DiscoverySettings
   :members:

.. autoclass:: aerospike_sdk.policy.discovery_settings.TranslationSettings
   :members:

.. autoclass:: aerospike_sdk.policy.discovery_settings.FileSeedProvider
   :members:

.. autoclass:: aerospike_sdk.policy.discovery_settings.FileAddressTranslator
   :members:
```
