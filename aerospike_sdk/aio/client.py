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

"""Client - Main entry point for the Aerospike SDK API."""

from __future__ import annotations

import logging
import types
from importlib import resources
from typing import Any, Awaitable, Callable, Dict, List, Optional

from aerospike_async import (
    AdminPolicy,
    Client as AsyncClient,
    ClientPolicy,
    Key,
    RegisterTask,
    UDFLang,
    UdfRemoveTask,
    new_client,
)

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import PacAerospikeError, _convert_pac_exception
from aerospike_sdk.routing_capabilities_shared import RoutingCapabilitiesMixin
from aerospike_sdk.udf_shared import parse_udf_list
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.index_list import parse_index_list
from aerospike_sdk.metrics import apply_metrics_settings
from aerospike_sdk.metrics.usage import UsageCounters
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import AsyncSdkConfigMonitor, SdkConfigSource
from aerospike_sdk.session_shared import _dataset_not_strings
from aerospike_sdk.aio.session import Session
from aerospike_sdk.aio.transactional_session import TransactionalSession

from aerospike_sdk.loggers import SdkLoggers, refresh_log_levels

log = logging.getLogger(SdkLoggers.LIFECYCLE)


class Client(RoutingCapabilitiesMixin):
    """Internal async connection primitive — not part of the public API.

    Held by :class:`~aerospike_sdk.aio.cluster.Cluster` and
    :class:`~aerospike_sdk.aio.pool.AsyncPool` as the object that actually owns
    the connection. It is not exported, not documented, and nothing public hands
    one out; construct a cluster with
    :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition` and read or
    write through a :class:`~aerospike_sdk.aio.session.Session` instead.

    See Also:
        :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`:
            The entry point to use.
    """

    def __init__(
        self,
        seeds: str,
        policy: Optional[ClientPolicy] = None,
        *,
        max_error_rate: Optional[int] = None,
        error_rate_window: Optional[int] = None,
    ) -> None:
        """Store cluster seeds and policy; connection starts in :meth:`connect` or ``async with``.

        Args:
            seeds: Seed address string understood by the async client (for example
                ``"127.0.0.1:3000"`` or a comma-separated host list if supported).
            policy: Optional :class:`~aerospike_async.ClientPolicy`; defaults to a
                new client policy when omitted.
            max_error_rate: Per-node circuit-breaker threshold. When a node's
                error count crosses this value within ``error_rate_window``
                tend iterations, subsequent commands routed to that node fail
                fast with :class:`~aerospike_sdk.MaxErrorRateError` until the
                window resets. ``0`` disables the breaker. Defaults to the
                underlying :class:`ClientPolicy` default (``100``).
            error_rate_window: Number of cluster tend iterations after which
                each node's error counter is reset. Defaults to the underlying
                :class:`ClientPolicy` default (``1``).
        """
        self._seeds = seeds
        if policy is None:
            policy = ClientPolicy()
        if max_error_rate is not None:
            policy.max_error_rate = max_error_rate
        if error_rate_window is not None:
            policy.error_rate_window = error_rate_window
        self._policy = policy
        self._client: Optional[AsyncClient] = None
        self._connected = False
        # Shared by all Session instances from this client; avoids repeated
        # namespace/<ns> info probes when callers use multiple sessions.
        self._namespace_mode_cache: Dict[str, Mode] = {}
        self._init_routing_capability_cache()
        # Resolved SDK-level settings (file over programmatic over defaults).
        # A frozen snapshot swapped wholesale by the config monitor, so the
        # operation path reads it lock-free.
        self._sdk_settings: SystemSettings = fill_hard_defaults(None)
        # Feature-usage counters. The flag is read on every gated call
        # site, so it is a plain attribute rather than a policy lookup.
        self._usage_on: bool = False
        # Per-call recording gates: `_cmd_count_on` mirrors metrics-enabled and
        # drives the cluster command count; `_record_on` is the one flag the
        # hot paths test (true when either recording kind is on).
        self._cmd_count_on: bool = False
        self._record_on: bool = False
        self._command_counts = UsageCounters()
        self._usage_counters = UsageCounters()
        # Set by the owning Cluster. Weak so the pair does not form a
        # cycle; a reload needs the Cluster because collection, the
        # export timer and the usage gate are all owned up there.
        self._owner_cluster: Optional[Callable[[], Any]] = None
        self._sdk_config_monitor: Optional[AsyncSdkConfigMonitor] = None
        # Cluster-wide MRT capability (all nodes >= the MRT server version),
        # resolved lazily on the first implicit-transaction gate check and
        # cached for the client's lifetime (cleared on close, like the
        # namespace-mode cache).
        self._supports_mrt_cache: Optional[bool] = None

    def _apply_sdk_settings(self, settings: SystemSettings) -> None:
        """Adopt reloaded settings, applying the ones that act on a live client.

        Most system settings are read at connect and cannot change on a running
        client; metrics is the exception — the whole point of putting it in the
        file is turning collection on without a restart.
        """
        self._sdk_settings = settings
        cluster = self._owner_cluster() if self._owner_cluster is not None else None
        if cluster is not None:
            # Full re-apply: collection, the export timer and the usage gate.
            cluster._apply_metrics_settings(settings.metrics)
        else:
            apply_metrics_settings(self.underlying_client, settings.metrics)

    def _start_sdk_config_monitor(self, source: SdkConfigSource) -> None:
        """Arm config-file hot-reload; swaps ``_sdk_settings`` on change."""
        monitor = AsyncSdkConfigMonitor(
            source,
            self._sdk_settings,
            self._apply_sdk_settings,
        )
        monitor.start()
        self._sdk_config_monitor = monitor

    async def connect(self) -> None:
        """Open a connection to the cluster using the configured seeds and policy.

        Idempotent: if already connected, returns immediately.

        Raises:
            ConnectionError: If the async client cannot reach the cluster (from PAC).

        See Also:
            :meth:`close`: Release the connection.

        Example::

            client = Client(ClusterDefinition("localhost", 3000))
            await client.connect()
        """
        if self._connected and self._client is not None:
            return

        # Rust-emitted log levels are cached at first emission; re-sync so
        # logging configured between import and connect is honored.
        refresh_log_levels()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Connecting to cluster seeds=%r", self._seeds)
        try:
            self._client = await new_client(self._policy, self._seeds)
        except PacAerospikeError as exc:
            raise _convert_pac_exception(exc) from exc
        self._connected = True
        self._warm_routing_capabilities()
        log.info(
            "Connected seeds=%r", self._seeds,
            extra={"aerospike.cluster": self._policy.cluster_name},
        )
        if log.isEnabledFor(logging.DEBUG):
            try:
                build_by_node = await self._client.info("build")
                log.debug(
                    "Connected seeds=%r; build info by node=%s",
                    self._seeds,
                    build_by_node,
                )
            except Exception as exc:
                log.debug(
                    "Connected seeds=%r but build probe failed: %s",
                    self._seeds,
                    exc,
                    exc_info=True,
                )

    async def close(self) -> None:
        """Close the underlying async client and clear connection state.

        Safe to call when already closed.

        See Also:
            :meth:`connect`.
        """
        if self._sdk_config_monitor is not None:
            await self._sdk_config_monitor.stop()
            self._sdk_config_monitor = None
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._connected = False
            log.info("Client closed")
        self._clear_routing_capability_cache()
        self._namespace_mode_cache.clear()
        self._supports_mrt_cache = None

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        await self.connect()
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
    def is_connected(self) -> bool:
        """Check if the client is connected.

        Returns:
            ``True`` when :meth:`connect` has succeeded and :meth:`close` has not been called.
        """
        return self._connected

    @property
    def _async_client(self) -> AsyncClient:
        """
        Get the underlying async client.

        Raises:
            RuntimeError: If the client is not connected.
        """
        if not self._connected or self._client is None:
            raise RuntimeError("Client is not connected. Call connect() first or use async with.")
        return self._client

    @property
    def underlying_client(self) -> AsyncClient:
        """
        The underlying aerospike_async (PAC) Client for direct API access.

        Use this when you need PAC calls that are not wrapped by the SDK API,
        e.g. info(), nodes(), get_node(). The returned client is the same
        instance used internally by the SDK API.

        Example::

            async with Client("localhost:3000") as client:
                pac = client.underlying_client
                response = await pac.info("sindex-list")
                nodes = pac.nodes()
                node = pac.get_node(nodes[0].name)
                response = await node.info("build")

        Returns:
            The aerospike_async Client instance.

        Raises:
            RuntimeError: If the client is not connected.
        """
        return self._async_client

    def _query(
        self,
        *,
        dataset: Optional[DataSet],
        keys: Optional[List[Key]],
        behavior: Behavior,
        namespace_mode_resolver: Callable[[str], Awaitable[Mode]],
        namespace_mode_resolver_blocking: Callable[[str], Mode],
    ) -> QueryBuilder:
        """Create a dataset or multi-key query builder for a session.

        Single-key queries never come here: the session builds those directly.
        Exactly one of ``dataset`` and ``keys`` is set.
        """
        if keys is not None:
            builder = QueryBuilder(
                client=self._async_client,
                namespace=keys[0].namespace,
                set_name=keys[0].set_name,
                behavior=behavior,
                namespace_mode_resolver=namespace_mode_resolver,
                namespace_mode_resolver_blocking=namespace_mode_resolver_blocking,
                sdk_client=self,
            )
            builder._keys = keys
            return builder
        assert dataset is not None
        return QueryBuilder(
            client=self._async_client,
            namespace=dataset.namespace,
            set_name=dataset.set_name,
            behavior=behavior,
            namespace_mode_resolver=namespace_mode_resolver,
            namespace_mode_resolver_blocking=namespace_mode_resolver_blocking,
            sdk_client=self,
        )

    def index(self, dataset: DataSet, /) -> IndexBuilder:
        """Create an index builder for a dataset.

        Args:
            dataset: Namespace and set to index.

        Returns:
            An IndexBuilder for chaining index operations.

        Raises:
            TypeError: If ``dataset`` is not a :class:`~aerospike_sdk.dataset.DataSet`.
        """
        if not isinstance(dataset, DataSet):
            if isinstance(dataset, str):
                raise TypeError(_dataset_not_strings("index"))
            raise TypeError(f"Expected a DataSet, got {type(dataset)}")
        return IndexBuilder(
            client=self,
            namespace=dataset.namespace,
            set_name=dataset.set_name,
        )

    def transaction(
        self, behavior: Optional[Behavior] = None,
    ) -> "TransactionalSession":
        """Create a multi-record transaction (MRT) session.

        Allocates a fresh :class:`~aerospike_async.Txn` on entry. Operations
        chained off the returned session (``tx.upsert(...)``, ``tx.query(...)``,
        ...) auto-participate in the transaction — every
        builder stamps ``policy.txn = tx.txn`` under the hood. On clean exit
        the transaction is committed; if an exception propagates out of the
        block it is aborted.

        Multi-record transactions require an Aerospike server running in
        strong-consistency (SC) mode on the target namespace.

        Args:
            behavior: Optional :class:`~aerospike_sdk.policy.behavior.Behavior`
                for operations inside the transaction. Defaults to
                :attr:`Behavior.DEFAULT` when omitted.

        Returns:
            A :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
            bound to this client and behavior.

        Example::

            async with client.transaction() as tx:
                await tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
                await tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()
        """
        return TransactionalSession(client=self, behavior=behavior)

    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """
        Create a session with the specified behavior.

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: The behavior configuration for the session.
                     If None, uses Behavior.DEFAULT.

        Returns:
            A new :class:`~aerospike_sdk.aio.session.Session` bound to this
            client.

        Example::

            session = client.create_session()
            users = DataSet.of("test", "users")
            await session.upsert(users.id(1)).put({"k": 1}).execute()

        Example::

            from datetime import timedelta

            from aerospike_sdk.policy import Settings

            fast = Behavior.DEFAULT.derive_with_changes(
                name="fast",
                all=Settings(total_timeout=timedelta(seconds=5)),
            )
            session = client.create_session(fast)

        See Also:
            :class:`~aerospike_sdk.policy.behavior.Behavior`: Available presets.
        """
        if behavior is None:
            behavior = Behavior.DEFAULT

        return Session(client=self, behavior=behavior)

    async def _register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF package from in-memory bytes on the cluster.

        Args:
            body: Raw module source (for example UTF-8 encoded Lua).
            server_path: Path name stored on the server (often ends with ``.lua``).
            language: :class:`~aerospike_async.UDFLang`; default is Lua.
            policy: Optional :class:`~aerospike_async.AdminPolicy` (PAC leading
                argument); use keyword ``policy=``.

        Returns:
            A :class:`~aerospike_async.RegisterTask`; await
            ``wait_till_complete(...)`` until propagation finishes.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster or admin errors (via PAC).

        See Also:
            :meth:`register_udf_from_file`: Load source from disk.

        Example::

            task = await session.register_udf("my_module", udf_source_code)
            await task.wait_till_complete()
        """
        return await self._async_client.register_udf(body, server_path, language, policy=policy)

    async def _register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF by reading module bytes from a local path.

        Args:
            client_path: Filesystem path to the module file on the client machine.
            server_path: Path name stored on the server.
            language: :class:`~aerospike_async.UDFLang`; default is Lua.
            policy: Optional admin policy; use keyword ``policy=``.

        Returns:
            A :class:`~aerospike_async.RegisterTask` for completion polling.

        Raises:
            RuntimeError: If not connected.
            OSError: If ``client_path`` cannot be read.
            AerospikeError: On cluster or admin errors (via PAC).

        See Also:
            :meth:`register_udf`: Register from bytes.

        Example::

            task = await session.register_udf_from_file("scripts/my_module.lua", "my_module.lua")
            await task.wait_till_complete()
        """
        return await self._async_client.register_udf_from_file(
            client_path, server_path, language, policy=policy)

    async def _register_udf_from_resource(
        self,
        package: str,
        resource: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF from a Python package resource (``importlib.resources``).

        For a module shipped as package data — e.g. a ``.lua`` bundled inside a
        library — rather than read from the filesystem. Reads the resource bytes
        and delegates to :meth:`register_udf`.

        Args:
            package: Importable package holding the resource (e.g. ``"myapp.udfs"``).
            resource: Resource name within the package (e.g. ``"record_example.lua"``).
            server_path: Path name stored on the server.
            language: :class:`~aerospike_async.UDFLang`; default is Lua.
            policy: Optional admin policy; use keyword ``policy=``.

        Returns:
            A :class:`~aerospike_async.RegisterTask` for completion polling.

        Raises:
            RuntimeError: If not connected.
            ModuleNotFoundError: If ``package`` cannot be imported.
            FileNotFoundError: If ``resource`` is not found in the package.
            AerospikeError: On cluster or admin errors (via PAC).

        Example::

            task = await session.register_udf_from_resource(
                "myapp.udfs", "record_example.lua", "record_example.lua")
            await task.wait_till_complete()

        See Also:
            :meth:`register_udf_from_file`, :meth:`register_udf`.
        """
        body = resources.files(package).joinpath(resource).read_bytes()
        return await self._register_udf(body, server_path, language, policy=policy)

    async def _remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> UdfRemoveTask:
        """Remove a registered UDF package from the cluster.

        Args:
            server_path: Same server path used when registering the module.
            policy: Optional admin policy; use keyword ``policy=``.

        Returns:
            A :class:`~aerospike_async.UdfRemoveTask`; await completion like register.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster or admin errors (via PAC).

        Example::

            task = await session.remove_udf("my_module")
            await task.wait_till_complete()
        """
        return await self._async_client.remove_udf(server_path, policy=policy)

    async def _list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster.

        Returns:
            One dict per registered module with ``name``, ``hash`` and
            ``type`` keys (e.g. ``[{"name": "my_module.lua", "hash": "…",
            "type": "LUA"}]``); an empty list when nothing is registered.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster or admin errors (via PAC).

        Example::

            for module in await session.list_udf():
                print(module["name"], module["type"])

        See Also:
            :meth:`register_udf`, :meth:`remove_udf`.
        """
        resp = await self._async_client.info("udf-list")
        return parse_udf_list(resp.get("udf-list", ""))

    async def _list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster.

        Returns:
            One dict per index with ``namespace``, ``set``, ``bin`` and
            ``name`` keys, plus ``type`` / ``index_type`` / ``context`` when
            the server reports them (``context`` is present for CDT indexes).
            Empty when no secondary indexes are defined.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster or info errors (via PAC).

        Example::

            for idx in await session.list_indexes():
                print(idx["name"], idx["namespace"], idx["bin"])

        See Also:
            :meth:`index`: Create or drop a secondary index.
        """
        raw = await self._async_client.info_on_all_nodes("sindex-list")
        return parse_index_list(raw)

