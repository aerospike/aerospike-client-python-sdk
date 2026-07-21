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

import types
import typing
from typing import Optional

from aerospike_async import ClientPolicy, UDFLang

from aerospike_sdk.aio.client import Client
from aerospike_sdk.exceptions import ConnectionError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource

if typing.TYPE_CHECKING:
    from aerospike_async import AdminPolicy, RegisterTask, UdfRemoveTask
    from aerospike_sdk.aio.session import Session
    from aerospike_sdk.aio.transactional_session import TransactionalSession


class Cluster:
    """Live connection to a cluster, obtained from :meth:`ClusterDefinition.connect`.

    Owns a connected :class:`~aerospike_sdk.aio.client.Client` and exposes
    :meth:`create_session` / :meth:`transaction`. Prefer
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
        sdk_client = Client(
            seeds=seeds,
            policy=policy,
            index_refresh_interval=index_refresh_interval,
        )
        if sdk_settings is not None:
            sdk_client._sdk_settings = sdk_settings
        await sdk_client.connect()

        if not await sdk_client.underlying_client.is_connected():
            await sdk_client.close()
            raise ConnectionError(
                f"Connected to seeds '{seeds}' but cluster reports not connected"
            )

        if sdk_config_source is not None:
            sdk_client._start_sdk_config_monitor(sdk_config_source)
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
    
    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """Open a :class:`~aerospike_sdk.aio.session.Session` with optional behavior.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: Defaults to :attr:`~aerospike_sdk.policy.behavior.Behavior.DEFAULT`.

        Returns:
            Session bound to this cluster's SDK client.

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.create_session`
        """
        if behavior is None:
            behavior = Behavior.DEFAULT
        return self._sdk_client.create_session(behavior)
    
    def transaction(
        self,
        behavior: Optional[Behavior] = None,
    ) -> TransactionalSession:
        """Return a :class:`TransactionalSession` for a multi-record transaction.

        Operations run inside the returned context manager use *behavior* (or
        :attr:`Behavior.DEFAULT` when omitted) and auto-participate in a fresh
        :class:`~aerospike_async.Txn`, committed on clean exit and aborted if an
        exception propagates. Requires a strong-consistency (SC) namespace.

        Args:
            behavior: :class:`~aerospike_sdk.policy.behavior.Behavior` for
                operations inside the transaction; defaults to
                :attr:`Behavior.DEFAULT`.

        Returns:
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`.

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.transaction`
        """
        return self._sdk_client.transaction(behavior)

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
        return await self._sdk_client._register_udf(
            body, server_path, language, policy=policy)

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

    def is_connected(self) -> bool:
        """Mirror :attr:`~aerospike_sdk.aio.client.Client.is_connected`."""
        return self._sdk_client.is_connected
    
    async def close(self) -> None:
        """Close the SDK client and release cluster resources.

        Invoked automatically when used as an async context manager.
        """
        await self._sdk_client.close()

