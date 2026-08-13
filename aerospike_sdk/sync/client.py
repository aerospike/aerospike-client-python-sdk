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

"""Synchronous SDK client.

Owns a PAC ``aerospike_async.Client``. Every lifecycle and IO entry calls
PAC's ``_blocking`` methods; no asyncio event loop is constructed.
"""

from __future__ import annotations

import logging
import types
from importlib import resources
from typing import TYPE_CHECKING, Dict, Optional, overload

from aerospike_async import (
    AdminPolicy,
    Client as AsyncClient,
    ClientPolicy,
    RegisterTask,
    UDFLang,
    UdfRemoveTask,
    new_client_blocking,
)

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.udf_shared import parse_udf_list
from aerospike_sdk.index_list import parse_index_list
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.feature_gates import (
    PSDK_ENABLE_QUERY_SELECTION,
    PSDK_ENABLE_SERVER_COMPILED_AEL,
)
from aerospike_sdk.query_selection import compute_query_selection_support_blocking
from aerospike_sdk.sdk_config_monitor import SdkConfigSource, SyncSdkConfigMonitor
from aerospike_sdk.server_compiled_ael import compute_server_compiled_ael_support_blocking

if TYPE_CHECKING:  # avoid circular imports — type-only annotations
    from aerospike_sdk.sync.operations.index import IndexBuilder
    from aerospike_sdk.sync.session import Session
    from aerospike_sdk.sync.transactional_session import TransactionalSession

from aerospike_sdk.loggers import SdkLoggers, refresh_log_levels

log = logging.getLogger(SdkLoggers.LIFECYCLE)


class SyncClient:
    """Low-level synchronous connection primitive (no ``async``/``await``).

    Most applications should connect via
    :class:`~aerospike_sdk.sync.cluster_definition.ClusterDefinition`
    (``ClusterDefinition(...).connect()``) rather than instantiating
    ``SyncClient`` directly. Reads and writes go through a
    :class:`~aerospike_sdk.sync.session.Session` from :meth:`create_session`.

    Example::

            with SyncClient("localhost:3000") as client:
                session = client.create_session()
                for row in session.query(
                    namespace="test",
                    set_name="users"
                ).execute():
                    if row.record:
                        print(row.record.bins)

    See Also:
        :class:`~aerospike_sdk.aio.client.Client`: Async equivalent.
        :meth:`create_session`: Session-scoped :class:`~aerospike_sdk.policy.behavior.Behavior`.
    """

    def __init__(
        self,
        seeds: str,
        policy: Optional[ClientPolicy] = None,
        *,
        max_error_rate: Optional[int] = None,
        error_rate_window: Optional[int] = None,
        current_thread_runtime: bool = False,
    ) -> None:
        """Initialize a SyncClient (no IO).

        Args:
            seeds: Aerospike cluster seed addresses (e.g., "localhost:3000").
            policy: Optional client policy. When not supplied, defaults to a
                fresh ``ClientPolicy`` left at PAC's own defaults. Pass an
                explicit ``ClientPolicy`` to override any client-level
                setting.
            max_error_rate: Per-node circuit-breaker threshold (see
                :class:`aerospike_sdk.aio.client.Client`).
            error_rate_window: Tend iterations until each node's error
                counter resets.
            current_thread_runtime: **Experimental — opt-in, subject to
                removal.** When ``True``, each calling OS thread gets its
                own PAC ``_LocalClient`` (sync-only, backed by a per-thread
                ``current_thread`` Tokio runtime). Eliminates the
                cross-thread worker hop on every op for ~+30-40% TPS lift
                on 32-thread sync workloads. Open caveat before production
                use: per-thread cluster tend multiplies info load on the
                cluster at high thread counts. Recommended pairing when
                opted in: set
                ``policy.conn_pools_per_node = 1`` so total connections
                per node stay modest (N threads × 1 ≈ a shared client's 4).
        """
        self._seeds = seeds
        if policy is None:
            policy = ClientPolicy()
            if current_thread_runtime:
                # Per-thread Client = per-thread pool; one connection per
                # thread is enough for the per-thread blocking pattern.
                policy.conn_pools_per_node = 1
        if max_error_rate is not None:
            policy.max_error_rate = max_error_rate
        if error_rate_window is not None:
            policy.error_rate_window = error_rate_window
        self._policy = policy
        self._current_thread_runtime = current_thread_runtime
        self._client: Optional[AsyncClient] = None
        self._connected = False
        # Shared by all sessions from this client; avoids repeated
        # namespace/<ns> info probes when callers use multiple sessions.
        self._namespace_mode_cache: Dict[str, Mode] = {}
        self._cached_supports_query_selection: Optional[bool] = None
        self._cached_supports_server_compiled_ael: Optional[bool] = None
        # Resolved SDK-level settings (file over programmatic over defaults).
        # A frozen snapshot swapped wholesale by the config monitor, so the
        # operation path reads it lock-free.
        self._sdk_settings: SystemSettings = fill_hard_defaults(None)
        self._sdk_config_monitor: Optional[SyncSdkConfigMonitor] = None
        # Cluster-wide MRT capability (all nodes >= the MRT server version),
        # resolved lazily on the first implicit-transaction gate check and
        # cached for the client's lifetime (cleared on close, like the
        # namespace-mode cache).
        self._supports_mrt_cache: Optional[bool] = None

    def _supports_mrt_blocking(self) -> bool:
        """Whether every cluster node supports multi-record transactions.

        An MRT spans the cluster, so the aggregate is all-nodes: a single
        node below the MRT server version makes the answer ``False``. The
        ``current_thread_runtime`` proxy has no node-listing surface, so
        MRT support cannot be verified there and this reports ``False``
        (implicit batch-write transactions stay off on that path).
        """
        if self._supports_mrt_cache is None:
            nodes_fn = getattr(self._client, "nodes_blocking", None)
            if nodes_fn is None:
                self._supports_mrt_cache = False
                return False
            nodes = nodes_fn()
            self._supports_mrt_cache = bool(nodes) and all(
                node.version.supports_mrt() for node in nodes
            )
        return self._supports_mrt_cache

    def _cluster_versions_blocking(self) -> list:
        """Per-node ``Version`` list for capability probes (fresh, uncached).

        Sync sibling of the async ``_cluster_versions``: read live so a probe
        reflects current cluster membership. The ``current_thread_runtime``
        proxy has no node-listing surface, so it yields an empty list (every
        probe then reports unsupported), matching the MRT-probe behavior.
        """
        nodes_fn = getattr(self._client, "nodes_blocking", None)
        if nodes_fn is None:
            return []
        return [node.version for node in nodes_fn()]

    def _start_sdk_config_monitor(self, source: SdkConfigSource) -> None:
        """Arm config-file hot-reload; swaps ``_sdk_settings`` on change."""
        monitor = SyncSdkConfigMonitor(
            source,
            self._sdk_settings,
            lambda settings: setattr(self, "_sdk_settings", settings),
        )
        monitor.start()
        self._sdk_config_monitor = monitor

    # -- Lifecycle ------------------------------------------------------------

    def connect(self) -> None:
        """Open a connection to the cluster synchronously.

        Calls :func:`aerospike_async.new_client_blocking` directly — no
        asyncio loop is constructed.

        When ``current_thread_runtime=True``, no PAC Client is constructed
        here. Instead a thread-local proxy is installed; each calling OS
        thread lazy-constructs its own
        :class:`aerospike_async.LocalClient` on first op.

        Idempotent: returns early if already connected.
        """
        if self._connected and self._client is not None:
            return
        # Rust-emitted log levels are cached at first emission; re-sync so
        # logging configured between import and connect is honored.
        refresh_log_levels()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Connecting (blocking) to cluster seeds=%r", self._seeds)
        if self._current_thread_runtime:
            from aerospike_sdk.sync._threadlocal_client import _ThreadLocalLocalClient
            # type: ignore[assignment] — proxy duck-types as PAC Client.
            self._client = _ThreadLocalLocalClient(self._policy, self._seeds)  # type: ignore[assignment]
        else:
            self._client = new_client_blocking(self._policy, self._seeds)
        self._connected = True
        if PSDK_ENABLE_QUERY_SELECTION:
            self._cached_supports_query_selection = compute_query_selection_support_blocking(
                self._client,
            )
        else:
            self._cached_supports_query_selection = False
        if PSDK_ENABLE_SERVER_COMPILED_AEL:
            self._cached_supports_server_compiled_ael = (
                compute_server_compiled_ael_support_blocking(self._client)
            )
        else:
            self._cached_supports_server_compiled_ael = False
        log.info(
            "Connected seeds=%r", self._seeds,
            extra={"aerospike.cluster": self._policy.cluster_name},
        )

    def close(self) -> None:
        """Close the connection synchronously.

        Calls PAC's ``close_blocking``. Safe to call when already closed.
        """
        if self._sdk_config_monitor is not None:
            self._sdk_config_monitor.stop()
            self._sdk_config_monitor = None
        if self._client is not None:
            self._client.close_blocking()
            self._client = None
            self._connected = False
            log.info("Client closed")
        self._cached_supports_query_selection = None
        self._cached_supports_server_compiled_ael = None
        self._namespace_mode_cache.clear()
        self._supports_mrt_cache = None

    def __enter__(self) -> SyncClient:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        self.close()

    # -- State accessors ------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """``True`` once :meth:`connect` has succeeded and :meth:`close` hasn't run."""
        return self._connected

    @property
    def underlying_client(self) -> AsyncClient:
        """The underlying PAC ``aerospike_async.Client``.

        Use for PAC calls the SDK doesn't wrap (info, nodes, etc.).
        """
        if not self._connected or self._client is None:
            raise RuntimeError("SyncClient is not connected. Call connect() first or use `with`.")
        return self._client

    @property
    def _async_client(self) -> AsyncClient:
        """Alias of :attr:`underlying_client` for parity with
        :class:`~aerospike_sdk.aio.client.Client`."""
        return self.underlying_client

    @property
    def supports_query_selection(self) -> bool:
        """``True`` when all cluster nodes support field ``44`` query selection (>= 8.1.3).

        Computed at :meth:`connect` from PAC ``Version.supports_query_selection()``
        on every node.
        """
        if not self._connected or self._client is None:
            return False
        return bool(self._cached_supports_query_selection)

    @property
    def supports_server_compiled_ael(self) -> bool:
        """``True`` when server-compiled AEL filters are usable on this connection.

        Requires all nodes >= 8.1.3 (PAC ``Version.supports_server_compiled_ael``)
        and PAC ``FilterExpression.from_server_compiled_ael``. Cached at connect.
        """
        if not self._connected or self._client is None:
            return False
        return bool(self._cached_supports_server_compiled_ael)

    def _ensure_connected(self) -> SyncClient:
        """Connect if not already connected; return ``self`` for chaining."""
        if not self._connected:
            self.connect()
        return self

    def _pac_client(self) -> AsyncClient:
        """Return the underlying PAC ``aerospike_async.Client`` (post-connect)."""
        self._ensure_connected()
        return self.underlying_client

    def _resolve_namespace_mode_blocking(self, namespace: str) -> Mode:
        """Resolve AP vs SC for ``namespace`` synchronously; caches per-client."""
        cached = self._namespace_mode_cache.get(namespace)
        if cached is not None:
            return cached
        try:
            from aerospike_sdk.aio.session import _parse_namespace_info_body
            result = self.underlying_client.info_blocking(f"namespace/{namespace}")
        except Exception:
            mode = Mode.AP
            self._namespace_mode_cache[namespace] = mode
            return mode
        is_sc = False
        for node_result in result.values():
            if not node_result:
                continue
            exists, sc_opt = _parse_namespace_info_body(node_result)
            if exists and sc_opt is True:
                is_sc = True
                break
        mode = Mode.SC if is_sc else Mode.AP
        self._namespace_mode_cache[namespace] = mode
        return mode

    # -- Factories: query / index / session ------------------------------------

    @overload
    def index(
        self, *, dataset: DataSet, behavior: Optional[Behavior] = None,
    ) -> IndexBuilder: ...
    @overload
    def index(
        self, namespace: str, set_name: str, *, behavior: Optional[Behavior] = None,
    ) -> IndexBuilder: ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create a secondary-index builder (synchronous)."""
        from aerospike_sdk.sync.operations.index import IndexBuilder

        self._ensure_connected()
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")
        return IndexBuilder(
            async_client=self,
            namespace=namespace,
            set_name=set_name,
        )

    def truncate(
        self, dataset: DataSet, before_nanos: Optional[int] = None,
    ) -> None:
        """Truncate a set, synchronously (PAC ``truncate_blocking``)."""
        self.underlying_client.truncate_blocking(
            dataset.namespace, dataset.set_name, before_nanos,
        )

    def _register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF module from bytes (synchronous)."""
        return self.underlying_client.register_udf_blocking(
            body, server_path, language, policy=policy,
        )

    def _register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF module from a local file (synchronous)."""
        return self.underlying_client.register_udf_from_file_blocking(
            client_path, server_path, language, policy=policy,
        )

    def _register_udf_from_resource(
        self,
        package: str,
        resource: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF from a Python package resource (synchronous).

        The Pythonic analog of the Java client's classpath/resource registration;
        reads the resource bytes via ``importlib.resources`` and delegates to
        :meth:`register_udf`. See
        :meth:`aerospike_sdk.aio.session.Session.register_udf_from_resource`.
        """
        body = resources.files(package).joinpath(resource).read_bytes()
        return self._register_udf(body, server_path, language, policy=policy)

    def _remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> UdfRemoveTask:
        """Remove a UDF module from the cluster (synchronous)."""
        return self.underlying_client.remove_udf_blocking(server_path, policy=policy)

    def _list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster (synchronous).

        Returns one dict per module with ``name`` / ``hash`` / ``type`` keys;
        empty when nothing is registered. See
        :meth:`aerospike_sdk.aio.session.Session.list_udf`.
        """
        resp = self.underlying_client.info_blocking("udf-list")
        return parse_udf_list(resp.get("udf-list", ""))

    def _list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster (synchronous).

        Returns one dict per index with ``namespace`` / ``set`` / ``bin`` /
        ``name`` keys, plus ``type`` / ``index_type`` / ``context`` when the
        server reports them. See
        :meth:`aerospike_sdk.aio.session.Session.list_indexes`.
        """
        raw = self.underlying_client.info_on_all_nodes_blocking("sindex-list")
        return parse_index_list(raw)

    def create_session(self, behavior: Optional[Behavior] = None) -> Session:
        """Create a synchronous session with the specified behavior."""
        from aerospike_sdk.sync.session import Session

        self._ensure_connected()
        return Session(client=self, behavior=behavior or Behavior.DEFAULT)

    def transaction(
        self, behavior: Optional[Behavior] = None,
    ) -> TransactionalSession:
        """Create a synchronous multi-record transaction session."""
        from aerospike_sdk.sync.transactional_session import TransactionalSession

        self._ensure_connected()
        return TransactionalSession(client=self, behavior=behavior or Behavior.DEFAULT)
