# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""ClusterDefinition - Builder for configuring Aerospike cluster connections."""

from __future__ import annotations

from typing import List

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.aio.tls_builder import TlsBuilder
from aerospike_sdk.cluster_shared import ClusterDefinitionBase, Host
from aerospike_sdk.policy.sdk_config_loader import load_at_connect
from aerospike_sdk.sdk_config_monitor import SdkConfigSource

__all__ = ["ClusterDefinition", "Host"]


class ClusterDefinition(ClusterDefinitionBase[TlsBuilder]):
    """Configure seeds, auth, TLS, rack awareness, and validation before :meth:`connect`.

    Call :meth:`connect` to obtain a live :class:`~aerospike_sdk.aio.cluster.Cluster`.
    The sync counterpart lives under ``aerospike_sdk.sync.cluster_definition``.
    Every builder method (auth, racks, TLS, system settings, ...) is inherited
    from :class:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase`; only the
    runtime-bound pieces below live here.

    Example::

            cluster = await (
                ClusterDefinition("localhost", 3100)
                .with_native_credentials("user", "secret")
                .using_services_alternate()
                .preferring_racks(1, 2)
                .validate_cluster_name_is("my-cluster")
                .connect()
            )

    See Also:
        :class:`~aerospike_sdk.aio.cluster.Cluster`
    """

    def _new_tls_builder(self) -> TlsBuilder:
        """Return an async ``TlsBuilder`` bound to this definition."""
        return TlsBuilder(self)

    def _build_pool_members(self, count: int) -> List[Client]:
        """Construct *count* unconnected pool-member clients (AsyncPool hook).

        All members share a single ``ClientPolicy`` built from this
        definition — which is exactly the shared-policy invariant AsyncPool's
        one-shot ``per_client_runtime_workers`` mutation relies on. Resolved
        SDK settings are applied to every member; config-file hot-reload is
        not armed for pool members (N file watchers for one process would be
        waste — pools should reload by restart).

        Each member is connected later, on its own pool loop, via
        :meth:`Cluster._connect_and_wrap` — connecting here would bind every
        ``CompletionBridge`` to the caller's loop.
        """
        self._validate()
        settings, _config_path, _raw = load_at_connect(self._cluster_name, self._system_settings)
        policy = self._get_policy(settings)
        seeds = self._build_seeds_string()
        members: List[Client] = []
        for _ in range(count):
            client = Client(
                seeds=seeds,
                policy=policy,
            )
            if settings is not None:
                client._sdk_settings = settings
            members.append(client)
        return members

    async def connect(self) -> Cluster:
        """
        Establishes a connection to the Aerospike cluster.

        This method creates and returns a Cluster instance using the configured
        parameters. The returned Cluster should be closed when no longer needed
        to properly release resources.

        Example with async context manager::

                async with await ClusterDefinition("localhost", 3100).connect() as cluster:
                    session = cluster.create_session(Behavior.DEFAULT)
                    # Use the session...

        Returns:
            A connected Cluster instance

        Raises:
            ValueError: If PKI auth is configured but hosts are missing TLS names
            ConnectionError: If ``fail_if_not_connected`` is True (default) and
                the cluster is unreachable
        """
        self._validate()
        # SDK config file (AEROSPIKE_SDK_CONFIG_URL): applies the behaviors
        # section, layers system settings over programmatic ones per-field,
        # and arms hot-reload on the client when a path is configured.
        settings, config_path, raw = load_at_connect(self._cluster_name, self._system_settings)
        config_source = (
            SdkConfigSource(config_path, self._cluster_name, self._system_settings, raw)
            if config_path is not None
            else None
        )
        policy = self._get_policy(settings)
        seeds = self._build_seeds_string()
        return await Cluster._create(
            policy,
            seeds,
            sdk_settings=settings,
            sdk_config_source=config_source,
        )
