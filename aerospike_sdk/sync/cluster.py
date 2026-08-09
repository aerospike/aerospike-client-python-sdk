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

"""Cluster - Represents a connection to an Aerospike cluster (sync version)."""

from __future__ import annotations

import types
import typing
from typing import Optional

from aerospike_async import ClientPolicy, UDFLang, Version

from aerospike_sdk import capabilities
from aerospike_sdk.cluster_shared import ClusterBase
from aerospike_sdk.exceptions import ConnectionError
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource
from aerospike_sdk.sync.client import SyncClient

if typing.TYPE_CHECKING:
    from aerospike_async import AdminPolicy, RegisterTask, UdfRemoveTask
    # These resolve the ClusterBase[_S, _TS] string forward-refs; ruff reads them as unused
    # (F401) because it doesn't count string-subscript usage.
    from aerospike_sdk.sync.session import Session  # noqa: F401
    from aerospike_sdk.sync.transactional_session import TransactionalSession  # noqa: F401


class Cluster(ClusterBase["Session", "TransactionalSession"]):
    """Synchronous cluster handle from ``sync.cluster_definition.ClusterDefinition.connect``.

    Mirrors :class:`~aerospike_sdk.aio.cluster.Cluster` but uses
    :class:`~aerospike_sdk.sync.client.SyncClient` and
    :class:`~aerospike_sdk.sync.session.Session`. The
    :meth:`create_session` / :meth:`transaction` / :meth:`is_connected`
    factories are inherited from
    :class:`~aerospike_sdk.cluster_shared.ClusterBase`.

    Example::

            with ClusterDefinition("localhost", 3100).connect() as cluster:
                session = cluster.create_session(Behavior.DEFAULT)

    See Also:
        :class:`~aerospike_sdk.aio.cluster.Cluster`
    """
    
    def __init__(self, sdk_client: SyncClient) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            sdk_client: The underlying SyncClient instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._sdk_client = sdk_client
    
    @classmethod
    def _create(
        cls,
        policy: ClientPolicy,
        seeds: str,
        index_refresh_interval: float = 5.0,
        sdk_settings: Optional[SystemSettings] = None,
        sdk_config_source: Optional[SdkConfigSource] = None,
    ) -> Cluster:
        """
        Internal method to create a new Cluster instance.

        Args:
            policy: The ClientPolicy configuration
            seeds: The seeds string (e.g., "localhost:3000")
            index_refresh_interval: Seconds between secondary-index cache refreshes
            sdk_settings: Resolved SDK settings to store for runtime reads
            sdk_config_source: When set, arms config hot-reload on the client

        Returns:
            A new Cluster instance

        Raises:
            ConnectionError: If post-connect validation fails
        """
        sdk_client = SyncClient(
            seeds=seeds,
            policy=policy,
            index_refresh_interval=index_refresh_interval,
        )
        if sdk_settings is not None:
            sdk_client._sdk_settings = sdk_settings
        sdk_client.connect()

        # Bypass asyncio for the post-connect sanity check — `is_connected`
        # on PAC is a non-blocking synchronous probe (no I/O).
        if not sdk_client._pac_client().is_connected_blocking():
            sdk_client.close()
            raise ConnectionError(f"Connected to seeds '{seeds}' but cluster reports not connected")

        if sdk_config_source is not None:
            sdk_client._start_sdk_config_monitor(sdk_config_source)
        return cls(sdk_client)
    
    def __enter__(self) -> Cluster:
        """Context manager entry."""
        return self
    
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Context manager exit."""
        self.close()
    
    @property
    def _client(self) -> SyncClient:
        """Get the underlying SyncClient."""
        return self._sdk_client

    def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF module from bytes (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf`
        """
        return self._sdk_client._register_udf(body, server_path, language, policy=policy)

    def register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF module from a local file (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_file`
        """
        return self._sdk_client._register_udf_from_file(
            client_path, server_path, language, policy=policy)

    def register_udf_from_resource(
        self,
        package: str,
        resource: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF from a Python package resource (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_resource`
        """
        return self._sdk_client._register_udf_from_resource(
            package, resource, server_path, language, policy=policy)

    def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "UdfRemoveTask":
        """Remove a UDF module from the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.remove_udf`
        """
        return self._sdk_client._remove_udf(server_path, policy=policy)

    def list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_udf`
        """
        return self._sdk_client._list_udf()

    def list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_indexes`
        """
        return self._sdk_client._list_indexes()

    # -- Server-capability probes ---------------------------------------------
    # Guard feature use against the cluster's least-capable node. Sync
    # counterparts of the async Cluster probes; see there for detail.

    def server_version(self) -> Optional[Version]:
        """The minimum server version across connected nodes.

        Returns:
            The least-capable node's :class:`~aerospike_async.Version`, or
            ``None`` when the cluster reports no nodes.

        Example::

            v = cluster.server_version()
            if v is not None and (v.major, v.minor, v.patch) >= (8, 1, 3):
                ...
        """
        return capabilities.min_version(self._sdk_client._cluster_versions_blocking())

    def supports_ael(self) -> bool:
        """Whether every node parses server-compiled AEL (filters, exp reads/writes)."""
        return capabilities.supports_ael(self._sdk_client._cluster_versions_blocking())

    def supports_query_operations(self) -> bool:
        """Whether every node supports read operations inside an index query."""
        return capabilities.supports_query_operations(
            self._sdk_client._cluster_versions_blocking())

    def supports_string_operations(self) -> bool:
        """Whether every node supports the server-side string operations."""
        return capabilities.supports_string_operations(
            self._sdk_client._cluster_versions_blocking())

    def supports_query_selection(self) -> bool:
        """Whether every node supports server-led index selection (>= 8.1.3)."""
        return capabilities.supports_query_selection(
            self._sdk_client._cluster_versions_blocking())

    def close(self) -> None:
        """
        Closes the cluster connection and releases all associated resources.
        
        This method closes the underlying client connection. It should be called
        when the cluster is no longer needed to ensure proper resource cleanup.
        
        This method is automatically called when using context manager::

                with ClusterDefinition("localhost", 3100).connect() as cluster:
                    # Use the cluster...
                # cluster.close() is automatically called here
        """
        self._sdk_client.close()

