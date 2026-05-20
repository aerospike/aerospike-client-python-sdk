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
import typing
from typing import Awaitable, Callable, Dict, List, Optional, Union, overload

from aerospike_async import (
    AdminPolicy,
    Client as AsyncClient,
    ClientPolicy,
    Key,
    RegisterTask,
    UDFLang,
    UdfRemoveTask,
    new_client,
    new_client_blocking,
)

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.index_monitor import IndexesMonitor
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode

if typing.TYPE_CHECKING:
    from aerospike_sdk.aio.session import Session
    from aerospike_sdk.aio.transactional_session import TransactionalSession

log = logging.getLogger(__name__)


class Client:
    """Async entry point for the SDK API over the Aerospike Python Async Client.

    Use ``async with Client(...) as client`` (or ``await connect()``) to
    open a connection, then :meth:`create_session` for reads and writes with a
    chosen :class:`~aerospike_sdk.policy.behavior.Behavior`.

    Example::

        async with Client("127.0.0.1:3000") as client:
            session = client.create_session()
            stream = await client.query(
                namespace="test",
                set_name="users",
            ).execute()
            async for row in stream:
                if row.record is not None:
                    print(row.record.bins)

    See Also:
        :meth:`create_session`: Primary API for application code.
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
        """Store cluster seeds and policy; connection starts in :meth:`connect` or ``async with``.

        Args:
            seeds: Seed address string understood by the async client (for example
                ``"127.0.0.1:3000"`` or a comma-separated host list if supported).
            policy: Optional :class:`~aerospike_async.ClientPolicy`; defaults to a
                new client policy when omitted.
            index_refresh_interval: Seconds between secondary index cache refreshes
                (default 5.0). The monitor is a daemon thread that starts lazily
                on the first AEL ``where()`` query and periodically refreshes
                cached index metadata so subsequent queries can transparently
                generate secondary index filters. Clients that never use
                ``where()`` never start the monitor thread.
            max_error_rate: Per-node circuit-breaker threshold. When a node's
                error count crosses this value within ``error_rate_window``
                tend iterations, subsequent commands routed to that node fail
                fast with :class:`~aerospike_sdk.MaxErrorRate` until the
                window resets. ``0`` disables the breaker. Defaults to the
                underlying :class:`ClientPolicy` default (``100``).
            error_rate_window: Number of cluster tend iterations after which
                each node's error counter is reset. Defaults to the underlying
                :class:`ClientPolicy` default (``1``).
            indexes_monitor: Optional pre-constructed :class:`IndexesMonitor`
                to share across Clients (for example, all clients in an
                :class:`~aerospike_sdk.AsyncPool`). When provided, this
                Client uses it for AEL filter generation but does not
                start or stop it — the caller that constructed the monitor
                owns its lifecycle. When ``None`` (default), the Client
                owns and manages a private monitor.

        Note:
            No network I/O occurs here. The client connects when you ``await
            connect()`` or enter ``async with``.
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
        # Shared by all Session instances from this client; avoids repeated
        # namespace/<ns> info probes when callers use multiple sessions.
        self._namespace_mode_cache: Dict[str, Mode] = {}

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

        if log.isEnabledFor(logging.DEBUG):
            log.debug("Connecting to cluster seeds=%r", self._seeds)
        self._client = await new_client(self._policy, self._seeds)
        self._connected = True
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
        # IndexesMonitor starts lazily on the first AEL ``where()`` query.
        # Sync benches and callers that never touch the AEL filter-generation
        # path pay zero daemon-thread cost.

    async def close(self) -> None:
        """Close the underlying async client and clear connection state.

        Safe to call when already closed.

        See Also:
            :meth:`connect`.
        """
        if self._owns_monitor:
            self._indexes_monitor.stop()
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._connected = False
        self._namespace_mode_cache.clear()

    def connect_blocking(self) -> None:
        """Synchronously open a connection without requiring an asyncio loop.

        Uses :func:`aerospike_async.new_client_blocking` to construct the
        underlying PAC client and sets ``_connected = True``. The
        :class:`IndexesMonitor` daemon thread is not started here; it
        lazy-starts on the first AEL ``where()`` query that needs cached
        secondary-index metadata.

        Idempotent: returns early if already connected.

        Raises:
            ConnectionError: When the PAC blocking connect cannot reach the
                cluster.

        Example::

            client = Client("localhost:3000")
            client.connect_blocking()
            try:
                ...
            finally:
                client.close_blocking()
        """
        if self._connected and self._client is not None:
            return
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Connecting (blocking) to cluster seeds=%r", self._seeds)
        self._client = new_client_blocking(self._policy, self._seeds)
        self._connected = True
        # IndexesMonitor starts lazily on the first AEL ``where()`` query.

    def close_blocking(self) -> None:
        """Synchronously close the underlying client. Pair with :meth:`connect_blocking`.

        Safe to call when already closed.
        """
        if self._owns_monitor:
            self._indexes_monitor.stop()
        if self._client is not None:
            self._client.close_blocking()
            self._client = None
            self._connected = False
        self._namespace_mode_cache.clear()

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
                nodes = await pac.nodes()
                node = await pac.get_node(nodes[0].name)
                response = await node.info("build")

        Returns:
            The aerospike_async Client instance.

        Raises:
            RuntimeError: If the client is not connected.
        """
        return self._async_client

    @overload
    def query(
        self,
        *,
        dataset: DataSet,
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """Create a query builder from a DataSet."""
        ...

    @overload
    def query(
        self,
        *,
        key: Key,
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """Create a query builder for a single Key (point read)."""
        ...

    @overload
    def query(
        self,
        *,
        keys: List[Key],
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @overload
    def query(
        self,
        *keys: Key,
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @overload
    def query(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """Create a query builder with explicit namespace/set."""
        ...

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
        namespace_mode_resolver: Optional[Callable[[str], Awaitable[Mode]]] = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> QueryBuilder:
        """
        Create a query builder.

        Supports multiple calling styles:

        1. Using a DataSet (positional or keyword)::

              users = DataSet.of("test", "users")
              async for record in client.query(users).execute():
              # or
              async for record in client.query(dataset=users).execute():
                  print(record.bins)

        2. Using a single Key (positional or keyword)::

              users = DataSet.of("test", "users")
              key = users.id("user123")
              recordset = await client.query(key).execute()
              # or
              recordset = await client.query(key=key).execute()

        3. Using multiple Keys (positional or keyword)::

              users = DataSet.of("test", "users")
              keys = users.ids("user1", "user2", "user3")
              recordset = await client.query(keys).execute()
              # or
              recordset = await client.query(keys=keys).execute()

        4. Explicit namespace/set (original style)::

              async for record in client.query(
                  namespace="test",
                  set_name="users"
              ).execute():
                  print(record.bins)

        Args:
            arg1: Optional first positional: :class:`~aerospike_sdk.dataset.DataSet`,
                :class:`~aerospike_async.Key`, list of keys, or namespace string for
                the ``("namespace", "set")`` pair form.
            set_name: When ``arg1`` is a namespace string, the set name as the second
                positional (``client.query("test", "users")``).
            namespace: Optional third positional; not used for the usual two-string
                namespace/set pair (that form uses ``arg1`` and ``set_name``).
            dataset: Keyword-only :class:`~aerospike_sdk.dataset.DataSet`.
            key: Keyword-only single key for a point read.
            keys: Keyword-only list of keys for a batch read.
            behavior: Optional :class:`~aerospike_sdk.policy.behavior.Behavior`
                for timeouts, retries, and replica settings on this builder. If
                ``None``, the client uses generic defaults (unlike
                :meth:`~aerospike_sdk.aio.session.Session.query`, which applies
                the session's behavior automatically).

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.QueryBuilder` for
            chaining filters, bin selection, and execution.

        Raises:
            TypeError: If a positional argument is not a dataset, key, or key list.
            ValueError: If required namespace/set information is missing or key
                lists are empty.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.query`: Same builder with
                session-scoped behavior.
        """
        # Handle positional arguments
        # Check if arg1 and arg2 are both strings (namespace, set_name pattern)
        if isinstance(arg1, str) and set_name is not None:
            # This is the namespace, set_name pattern - use them directly
            namespace = arg1
            # set_name is already set from the parameter
        elif arg1 is not None:
            # Handle single positional argument (DataSet, Key, or List[Key])
            if isinstance(arg1, DataSet):
                dataset = arg1
            elif isinstance(arg1, Key):
                key = arg1
            elif isinstance(arg1, list):
                keys = arg1
            else:
                raise TypeError(f"Expected DataSet, Key, or List[Key], got {type(arg1)}")

        # Handle single Key
        if key is not None:
            namespace = key.namespace
            set_name = key.set_name
            # For single key queries, we'll need to handle this in QueryBuilder
            # For now, create a query builder and store the key
            builder = QueryBuilder(
                client=self._async_client,
                namespace=namespace,
                set_name=set_name,
                behavior=behavior,
                indexes_monitor=self._indexes_monitor,
                namespace_mode_resolver=namespace_mode_resolver,
            )
            builder._single_key = key
            return builder

        # Handle multiple Keys
        if keys is not None:
            if not keys:
                raise ValueError("keys list cannot be empty")
            namespace = keys[0].namespace
            set_name = keys[0].set_name
            builder = QueryBuilder(
                client=self._async_client,
                namespace=namespace,
                set_name=set_name,
                behavior=behavior,
                indexes_monitor=self._indexes_monitor,
                namespace_mode_resolver=namespace_mode_resolver,
            )
            builder._keys = keys
            return builder

        # Handle DataSet
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None:
            pass
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - query(dataset=DataSet(...))\n"
                "  - query(key=Key(...))\n"
                "  - query(keys=[Key(...), ...])\n"
                "  - query(namespace=..., set_name=...)"
            )

        return QueryBuilder(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
            behavior=behavior,
            indexes_monitor=self._indexes_monitor,
            namespace_mode_resolver=namespace_mode_resolver,
            namespace_mode_resolver_blocking=namespace_mode_resolver_blocking,
        )

    @overload
    def index(
        self,
        *,
        dataset: DataSet,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create an index builder from a DataSet."""
        ...

    @overload
    def index(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create an index builder with explicit namespace/set."""
        ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """
        Create an index builder.

        Supports multiple calling styles:

        1. Using a DataSet::

              users = DataSet.of("test", "users")
              await client.index(dataset=users).on_bin("age").named("age_idx").numeric().create()

        2. Explicit namespace/set (original style)::

              await client.index(
                  namespace="test",
                  set_name="users"
              ).on_bin("age").named("age_idx").numeric().create()

        Args:
            namespace: The namespace name (if not using DataSet).
            set_name: The set name (if not using DataSet).
            dataset: Optional DataSet to use for namespace/set.
            behavior: Reserved for symmetry with :meth:`query`; not applied to
                index operations yet.

        Returns:
            An IndexBuilder for chaining index operations.
        """
        _ = behavior
        # Handle DataSet
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        # Handle explicit namespace/set (original style)
        elif namespace is not None and set_name is not None:
            pass
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - index(dataset=DataSet(...))\n"
                "  - index(namespace=..., set_name=...)"
            )

        return IndexBuilder(
            client=self._async_client,
            namespace=namespace,
            set_name=set_name,
        )

    def transaction_session(
        self, behavior: Optional[Behavior] = None,
    ) -> "TransactionalSession":
        """Create a multi-record transaction (MRT) session.

        Allocates a fresh :class:`~aerospike_async.Txn` on entry. Operations
        chained off the returned session (``tx.upsert(...)``, ``tx.query(...)``,
        ``tx.batch()``, ...) auto-participate in the transaction — every
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

            async with client.transaction_session() as tx:
                await tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
                await tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()
        """
        # Late import breaks the client -> transactional_session -> session ->
        # client cycle (TransactionalSession subclasses Session, and Session
        # imports Client at module level).
        from aerospike_sdk.aio.transactional_session import TransactionalSession
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
            fast = Behavior.DEFAULT.derive_with_changes(
                name="fast",
                total_timeout=timedelta(seconds=5),
            )
            session = client.create_session(fast)

        See Also:
            :class:`~aerospike_sdk.policy.behavior.Behavior`: Available presets.
        """
        from aerospike_sdk.aio.session import Session

        if behavior is None:
            behavior = Behavior.DEFAULT

        return Session(client=self, behavior=behavior)

    async def register_udf(
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

            task = await client.register_udf("my_module", udf_source_code)
            await task.wait_till_complete()
        """
        return await self._async_client.register_udf(
            body, server_path, language, policy=policy)

    async def register_udf_from_file(
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

            task = await client.register_udf_from_file("scripts/my_module.lua", "my_module.lua")
            await task.wait_till_complete()
        """
        return await self._async_client.register_udf_from_file(
            client_path, server_path, language, policy=policy)

    async def remove_udf(
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

            task = await client.remove_udf("my_module")
            await task.wait_till_complete()
        """
        return await self._async_client.remove_udf(server_path, policy=policy)

