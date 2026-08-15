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

"""Cluster - Represents a connection to an Aerospike cluster."""

from __future__ import annotations

import asyncio
import types
import typing
from typing import Optional

from aerospike_async import ClientPolicy, UDFLang, Version

from aerospike_sdk import capabilities
from aerospike_sdk.aio.client import Client
from aerospike_sdk.cluster_shared import ClusterBase
from aerospike_sdk.exceptions import ConnectionError
from aerospike_sdk.metrics import MetricsPolicy, MetricsSnapshot
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource

if typing.TYPE_CHECKING:
    from aerospike_async import AdminPolicy, RegisterTask, UdfRemoveTask
    # These resolve the ClusterBase[_S, _TS] string forward-refs; ruff reads them as unused
    # (F401) because it doesn't count string-subscript usage.
    from aerospike_sdk.aio.session import Session  # noqa: F401
    from aerospike_sdk.aio.transactional_session import TransactionalSession  # noqa: F401


class Cluster(ClusterBase["Session", "TransactionalSession"]):
    """Live connection to a cluster, obtained from :meth:`ClusterDefinition.connect`.

    Owns a connected :class:`~aerospike_sdk.aio.client.Client` and exposes
    :meth:`create_session` / :meth:`transaction` (both inherited from
    :class:`~aerospike_sdk.cluster_shared.ClusterBase`). Prefer
    ``async with await ClusterDefinition(...).connect() as cluster`` so
    :meth:`close` runs on exit.

    Example::

            async with await ClusterDefinition("localhost", 3100).connect() as cluster:
                session = cluster.create_session(Behavior.DEFAULT)

    See Also:
        :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`
    """
    
    def __init__(self, sdk_client: Client) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            sdk_client: The underlying Client instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._sdk_client = sdk_client
    
    @classmethod
    async def _create(
        cls,
        policy: ClientPolicy,
        seeds: str,
        sdk_settings: Optional[SystemSettings] = None,
        sdk_config_source: Optional[SdkConfigSource] = None,
    ) -> Cluster:
        """
        Internal method to create a new Cluster instance.

        Args:
            policy: The ClientPolicy configuration
            seeds: The seeds string (e.g., "localhost:3000")
            sdk_settings: Resolved SDK settings to store for runtime reads
            sdk_config_source: When set, arms config hot-reload on the client

        Returns:
            A new Cluster instance

        Raises:
            ConnectionError: If post-connect validation fails
        """
        sdk_client = Client(
            seeds=seeds,
            policy=policy,
        )
        if sdk_settings is not None:
            sdk_client._sdk_settings = sdk_settings
        cluster = await cls._connect_and_wrap(sdk_client)

        if sdk_config_source is not None:
            sdk_client._start_sdk_config_monitor(sdk_config_source)
        return cluster

    @classmethod
    async def _connect_and_wrap(cls, sdk_client: Client) -> Cluster:
        """Connect *sdk_client* on the current loop, validate, and wrap it.

        Shared by :meth:`_create` and :class:`~aerospike_sdk.aio.pool.AsyncPool`
        (which connects one pre-built member per pool loop), so
        connect-then-validate is defined once.

        Raises:
            ConnectionError: If post-connect validation fails.
        """
        await sdk_client.connect()

        if not await sdk_client.underlying_client.is_connected():
            await sdk_client.close()
            raise ConnectionError(
                f"Connected to seeds '{sdk_client._seeds}' but cluster reports not connected"
            )
        return cls(sdk_client)
    
    async def __aenter__(self) -> Cluster:
        """Async context manager entry."""
        return self
    
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()
    
    @property
    def _client(self) -> Client:
        """Get the underlying Client."""
        return self._sdk_client

    async def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF package from in-memory bytes on the cluster.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf`
        """
        return await self._sdk_client._register_udf(body, server_path, language, policy=policy)

    async def register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF by reading module bytes from a local file.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_file`
        """
        return await self._sdk_client._register_udf_from_file(
            client_path, server_path, language, policy=policy)

    async def register_udf_from_resource(
        self,
        package: str,
        resource: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF from a Python package resource.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_resource`
        """
        return await self._sdk_client._register_udf_from_resource(
            package, resource, server_path, language, policy=policy)

    async def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "UdfRemoveTask":
        """Remove a registered UDF package from the cluster.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.remove_udf`
        """
        return await self._sdk_client._remove_udf(server_path, policy=policy)

    async def list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_udf`
        """
        return await self._sdk_client._list_udf()

    async def list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_indexes`
        """
        return await self._sdk_client._list_indexes()

    # -- Server-capability probes ---------------------------------------------
    # Guard feature use against the cluster's least-capable node before
    # calling a feature that a mixed-version cluster may not fully support.
    # Each folds the per-node version, so a single lagging node reports the
    # feature unsupported. Read live, so the answer tracks current membership.

    async def server_version(self) -> Optional[Version]:
        """The minimum server version across connected nodes.

        Returns:
            The least-capable node's :class:`~aerospike_async.Version`, or
            ``None`` when the cluster reports no nodes. Guarding against the
            *minimum* is what makes a feature check safe on a mixed-version
            or mid-upgrade cluster.

        Example::

            v = await cluster.server_version()
            if v is not None and (v.major, v.minor, v.patch) >= (8, 1, 3):
                ...
        """
        return capabilities.min_version(await self._sdk_client._cluster_versions())

    async def supports_ael(self) -> bool:
        """Whether every node parses server-compiled AEL (filters, exp reads/writes)."""
        return capabilities.supports_ael(await self._sdk_client._cluster_versions())

    async def supports_query_operations(self) -> bool:
        """Whether every node supports read operations inside an index query."""
        return capabilities.supports_query_operations(
            await self._sdk_client._cluster_versions())

    async def supports_string_operations(self) -> bool:
        """Whether every node supports the server-side string operations.

        Example::

            if await cluster.supports_string_operations():
                await session.upsert(key).bin("s").str_append("!").execute()
        """
        return capabilities.supports_string_operations(
            await self._sdk_client._cluster_versions())

    async def supports_query_selection(self) -> bool:
        """Whether every node supports server-led index selection (>= 8.1.3)."""
        return capabilities.supports_query_selection(
            await self._sdk_client._cluster_versions())

    # -- Metrics ---------------------------------------------------------------
    # Collection lives in the client core and is cluster-scoped; these
    # configure it and pull snapshots. Enable/disable/enabled are instant
    # (no IO) and therefore plain methods even on the async surface.

    def enable_metrics(self, policy: Optional[MetricsPolicy] = None) -> None:
        """Enable metrics collection for this cluster.

        Collection is off until enabled. Re-enabling with a changed latency
        unit or histogram shape discards the accumulated latency samples;
        counters are retained.

        Args:
            policy: Collection configuration. Defaults to
                :class:`~aerospike_sdk.MetricsPolicy`'s milliseconds/7-column
                scheme with every command recorded.

        Example::

            cluster.enable_metrics(MetricsPolicy(sampler=Sampler.probability(0.1)))

        See Also:
            :meth:`metrics`, :meth:`disable_metrics`
        """
        pac_policy = (policy if policy is not None else MetricsPolicy())._to_pac()
        self._sdk_client.underlying_client.enable_metrics(pac_policy)

    def disable_metrics(self) -> None:
        """Disable metrics collection. Accumulated data is retained."""
        self._sdk_client.underlying_client.disable_metrics()

    def metrics_enabled(self) -> bool:
        """Whether metrics collection is currently enabled."""
        return self._sdk_client.underlying_client.metrics_enabled()

    async def metrics(self) -> MetricsSnapshot:
        """Snapshot the accumulated cluster metrics.

        Values are cumulative since metrics were enabled (connection gauges
        are point-in-time). Snapshotting drains and aggregates per-node
        state, so poll at an export interval rather than per operation.

        Returns:
            A :class:`~aerospike_sdk.MetricsSnapshot`; empty (zeroed) if
            metrics were never enabled.

        Example::

            snapshot = await cluster.metrics()
            reads = snapshot.latency(LatencyType.READ)
            print(f"{reads.count} reads, avg {reads.average:.1f}")
        """
        pac = self._sdk_client.underlying_client
        return MetricsSnapshot(await asyncio.to_thread(pac.metrics))

    async def close(self) -> None:
        """Close the SDK client and release cluster resources.

        Invoked automatically when used as an async context manager.
        """
        await self._sdk_client.close()

