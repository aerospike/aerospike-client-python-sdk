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

"""ClusterDefinition - Builder for configuring Aerospike cluster connections (sync version)."""

from __future__ import annotations

from aerospike_sdk.cluster_shared import ClusterDefinitionBase, Host
from aerospike_sdk.sync.cluster import Cluster
from aerospike_sdk.sync.tls_builder import TlsBuilder
from aerospike_sdk.policy.sdk_config_loader import load_at_connect
from aerospike_sdk.sdk_config_monitor import SdkConfigSource

__all__ = ["ClusterDefinition", "Host"]


class ClusterDefinition(ClusterDefinitionBase[TlsBuilder]):
    """Sync builder for seeds, auth, TLS, and validation; :meth:`connect` returns :class:`Cluster`.

    Mirrors :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`
    with a blocking :meth:`connect` and context-manager support on
    :class:`~aerospike_sdk.sync.cluster.Cluster`. Every builder method is
    inherited from :class:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase`;
    only the blocking :meth:`connect` and TLS-builder factory live here.

    Example::

            with (
                ClusterDefinition("localhost", 3100)
                .with_native_credentials("username", "password")
                .connect()
            ) as cluster:
                session = cluster.create_session(Behavior.DEFAULT)

    See Also:
        :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`
    """

    def _new_tls_builder(self) -> TlsBuilder:
        """Return a sync ``TlsBuilder`` bound to this definition."""
        return TlsBuilder(self)

    def connect(self) -> Cluster:
        """
        Establishes a connection to the Aerospike cluster (synchronously).

        This method creates and returns a Cluster instance using the configured
        parameters. The returned Cluster should be closed when no longer needed
        to properly release resources.

        Example with context manager::

                with ClusterDefinition("localhost", 3100).connect() as cluster:
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
        return Cluster._create(
            policy,
            seeds,
            sdk_settings=settings,
            sdk_config_source=config_source,
        )
