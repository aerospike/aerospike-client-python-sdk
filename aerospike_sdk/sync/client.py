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

Owns a PAC ``aerospike_async.Client`` and a daemon-thread
:class:`~aerospike_sdk.index_monitor.IndexesMonitor`. Every lifecycle and
IO entry calls PAC's ``_blocking`` methods; no asyncio event loop is
constructed. Builder and session factories return synchronous wrappers
(:class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder`,
:class:`~aerospike_sdk.sync.session.SyncSession`).
"""

from __future__ import annotations

import logging
import types
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, overload

from aerospike_async import (
    AdminPolicy,
    Client as AsyncClient,
    ClientPolicy,
    Key,
    RegisterTask,
    UDFLang,
    UdfRemoveTask,
    new_client_blocking,
)

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.index_monitor import IndexesMonitor
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode

if TYPE_CHECKING:  # avoid circular imports — type-only annotations
    from aerospike_sdk.aio.operations.query import QueryBuilder
    from aerospike_sdk.sync.operations.index import SyncIndexBuilder
    from aerospike_sdk.sync.operations.query import SyncQueryBuilder
    from aerospike_sdk.sync.session import SyncSession
    from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

log = logging.getLogger(__name__)


class SyncClient:
    """Connect to Aerospike and run the SDK API without ``async``/``await``.

    Example::

            with SyncClient("localhost:3000") as client:
                for row in client.query(
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
        index_refresh_interval: float = 5.0,
        *,
        max_error_rate: Optional[int] = None,
        error_rate_window: Optional[int] = None,
        indexes_monitor: Optional[IndexesMonitor] = None,
    ) -> None:
        """Initialize a SyncClient (no IO).

        Args:
            seeds: Aerospike cluster seed addresses (e.g., "localhost:3000").
            policy: Optional client policy. Defaults to a fresh ``ClientPolicy``.
            index_refresh_interval: Seconds between secondary index cache
                refreshes (default 5.0). The monitor is a daemon thread that
                starts lazily on the first AEL ``where()`` query — clients
                that never use AEL filters never spin up the thread.
            max_error_rate: Per-node circuit-breaker threshold (see
                :class:`aerospike_sdk.aio.client.Client`).
            error_rate_window: Tend iterations until each node's error
                counter resets.
            indexes_monitor: Optional pre-constructed
                :class:`IndexesMonitor` to share across clients (for example
                an :class:`AsyncPool`). When supplied, this client uses it
                but does not own its lifecycle.
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
        if indexes_monitor is not None:
            self._indexes_monitor = indexes_monitor
            self._owns_monitor = False
        else:
            self._indexes_monitor = IndexesMonitor(refresh_interval=index_refresh_interval)
            self._owns_monitor = True
        # Shared by all sessions from this client; avoids repeated
        # namespace/<ns> info probes when callers use multiple sessions.
        self._namespace_mode_cache: Dict[str, Mode] = {}

    # -- Lifecycle ------------------------------------------------------------

    def connect(self) -> None:
        """Open a connection to the cluster synchronously.

        Calls :func:`aerospike_async.new_client_blocking` directly — no
        asyncio loop is constructed. The :class:`IndexesMonitor` daemon
        thread is not started here; it lazy-starts on the first AEL
        ``where()`` query.

        Idempotent: returns early if already connected.
        """
        if self._connected and self._client is not None:
            return
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Connecting (blocking) to cluster seeds=%r", self._seeds)
        self._client = new_client_blocking(self._policy, self._seeds)
        self._connected = True

    def close(self) -> None:
        """Close the connection synchronously.

        Stops the :class:`IndexesMonitor` daemon thread (if owned) and
        calls PAC's ``close_blocking``. Safe to call when already closed.
        """
        if self._owns_monitor:
            self._indexes_monitor.stop()
        if self._client is not None:
            self._client.close_blocking()
            self._client = None
            self._connected = False
        self._namespace_mode_cache.clear()

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
    def query(self, arg1: DataSet, *, behavior: Optional[Behavior] = None) -> SyncQueryBuilder: ...
    @overload
    def query(self, arg1: Key, *, behavior: Optional[Behavior] = None) -> SyncQueryBuilder: ...
    @overload
    def query(self, arg1: List[Key], *, behavior: Optional[Behavior] = None) -> SyncQueryBuilder: ...
    @overload
    def query(
        self, arg1: str, set_name: str, *, behavior: Optional[Behavior] = None,
    ) -> SyncQueryBuilder: ...

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        set_name: Optional[str] = None,
        namespace: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys: Optional[List[Key]] = None,
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Any] = None,
        namespace_mode_resolver_blocking: Optional[Any] = None,
    ) -> SyncQueryBuilder:
        """Create a synchronous query builder.

        Same calling shapes as
        :meth:`Client.query <aerospike_sdk.aio.client.Client.query>`. Returns
        :class:`SyncQueryBuilder` whose ``.execute()`` runs synchronously.
        """
        from aerospike_sdk.sync.operations.query import SyncQueryBuilder

        self._ensure_connected()
        # Normalize args: extract the right (namespace, set_name, key, keys, dataset).
        if arg1 is not None:
            if isinstance(arg1, DataSet):
                dataset = arg1
            elif isinstance(arg1, Key):
                key = arg1
            elif isinstance(arg1, list):
                if not arg1:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(
                        f"Expected List[Key], got first element {type(arg1[0])}",
                    )
                keys = arg1
            elif isinstance(arg1, str):
                namespace = arg1
                # set_name is the positional second arg in this calling style
            else:
                raise TypeError(f"Expected DataSet, Key, List[Key], or str; got {type(arg1)}")

        return self._build_sync_query_builder(
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            key=key,
            keys=keys,
            behavior=behavior,
            namespace_mode_resolver=namespace_mode_resolver,
            namespace_mode_resolver_blocking=namespace_mode_resolver_blocking,
        )

    def _build_sync_query_builder(
        self,
        *,
        namespace: Optional[str],
        set_name: Optional[str],
        dataset: Optional[DataSet],
        key: Optional[Key],
        keys: Optional[List[Key]],
        behavior: Optional[Behavior],
        namespace_mode_resolver: Optional[Any] = None,
        namespace_mode_resolver_blocking: Optional[Any] = None,
    ) -> SyncQueryBuilder:
        """Construct a :class:`SyncQueryBuilder` with full context."""
        from aerospike_sdk.sync.operations.query import SyncQueryBuilder as _SQB

        nmrb = namespace_mode_resolver_blocking or self._resolve_namespace_mode_blocking

        if key is not None:
            builder = _SQB(
                client=self.underlying_client,
                namespace=key.namespace,
                set_name=key.set_name,
                behavior=behavior,
                indexes_monitor=self._indexes_monitor,
                namespace_mode_resolver=namespace_mode_resolver,
                namespace_mode_resolver_blocking=nmrb,
            )
            builder._single_key = key
            return builder

        if keys is not None:
            ns = keys[0].namespace
            sn = keys[0].set_name
            builder = _SQB(
                client=self.underlying_client,
                namespace=ns,
                set_name=sn,
                behavior=behavior,
                indexes_monitor=self._indexes_monitor,
                namespace_mode_resolver=namespace_mode_resolver,
                namespace_mode_resolver_blocking=nmrb,
            )
            builder._keys = keys
            return builder

        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        elif namespace is None or set_name is None:
            raise ValueError(
                "Invalid arguments. Use one of: query(dataset=...), query(key=...), "
                "query(keys=[...]), or query(namespace=..., set_name=...).",
            )

        return _SQB(
            client=self.underlying_client,
            namespace=namespace,
            set_name=set_name,
            behavior=behavior,
            indexes_monitor=self._indexes_monitor,
            namespace_mode_resolver=namespace_mode_resolver,
            namespace_mode_resolver_blocking=nmrb,
        )

    @overload
    def index(
        self, *, dataset: DataSet, behavior: Optional[Behavior] = None,
    ) -> SyncIndexBuilder: ...
    @overload
    def index(
        self, namespace: str, set_name: str, *, behavior: Optional[Behavior] = None,
    ) -> SyncIndexBuilder: ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> SyncIndexBuilder:
        """Create a secondary-index builder (synchronous)."""
        from aerospike_sdk.sync.operations.index import SyncIndexBuilder

        self._ensure_connected()
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")
        return SyncIndexBuilder(
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

    def register_udf(
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

    def register_udf_from_file(
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

    def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> UdfRemoveTask:
        """Remove a UDF module from the cluster (synchronous)."""
        return self.underlying_client.remove_udf_blocking(server_path, policy=policy)

    def create_session(self, behavior: Optional[Behavior] = None) -> SyncSession:
        """Create a synchronous session with the specified behavior."""
        from aerospike_sdk.sync.session import SyncSession

        self._ensure_connected()
        return SyncSession(client=self, behavior=behavior or Behavior.DEFAULT)

    def create_transactional_session(
        self, behavior: Optional[Behavior] = None,
    ) -> SyncTransactionalSession:
        """Create a synchronous multi-record transaction session."""
        from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

        self._ensure_connected()
        return SyncTransactionalSession(client=self, behavior=behavior or Behavior.DEFAULT)
