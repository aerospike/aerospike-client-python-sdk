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

"""Session - Main interface for database operations with Behavior configuration."""

from __future__ import annotations

import typing
from typing import Any, Awaitable, Dict, List, Optional, overload, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from aerospike_sdk.aio.transactional_session import TransactionalSession
    from aerospike_sdk.record_result import RecordResult

from aerospike_async import Key, Record, ResultCode, Txn

from aerospike_sdk.aio.background import BackgroundTaskSession
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.info import InfoCommands
from aerospike_sdk.aio.operations.batch import BatchOperationBuilder
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior, OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import to_read_policy, to_write_policy


class Session:
    """Perform reads and writes against Aerospike with a fixed :class:`~aerospike_sdk.policy.behavior.Behavior`.

    A session binds a connected :class:`Client` to policy defaults (timeouts,
    retries, replica preferences) for every operation started from it. Create
    sessions with :meth:`Client.create_session`; do not construct
    ``Session`` directly.

    Example:
        async with Client("localhost:3000") as client:
            session = client.create_session(Behavior.DEFAULT)
            users = DataSet.of("test", "users")
            stream = await session.query(users.id(1)).execute()
            first = await stream.first_or_raise()
            await session.upsert(users.id(2)).put({"name": "Tim"}).execute()

    See Also:
        :meth:`Client.create_session`: How to obtain a session.
        :meth:`query`: Point reads, batch reads, and secondary-index queries.
        :meth:`upsert`: Create-or-update writes.
    """

    def __init__(self, client: Client, behavior: Behavior) -> None:
        """Attach a client and behavior; prefer :meth:`Client.create_session`.

        Args:
            client: Connected (or not yet connected) :class:`Client`.
            behavior: Policy bundle for operations from this session.

        Note:
            Application code should not call ``Session(...)`` directly.

        See Also:
            :meth:`Client.create_session`.
        """
        self._client = client
        self._behavior = behavior
        # Pre-compute base policies once per session so QueryBuilders
        # skip per-op policy_mapper calls for the common no-override path.
        self._cached_read_policy = to_read_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT))
        self._cached_write_policy = to_write_policy(
            behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        # Cache the raw PAC client for fast-path methods.
        self._pac_client = client._async_client
        # Transaction hook. Non-transactional sessions always return None;
        # TransactionalSession overrides this to yield its active Txn so every
        # builder spawned from the session auto-participates.
        self._txn: Optional[Txn] = None

    def _bind_txn(self, builder):
        """Stamp the session's current txn onto a builder if one is active.

        Fast-path helper used by every builder factory on :class:`Session`
        so that operations started inside a
        :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
        auto-participate in the transaction. Returns the builder for fluent
        use; no-op outside an MRT.
        """
        if self._txn is not None:
            builder.with_txn(self._txn)
        return builder

    def get_current_transaction(self) -> Optional[Txn]:
        """Return the active transaction for this session, or ``None``.

        Regular :class:`Session` instances always return ``None``; only
        :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
        inside its ``async with`` block returns a live
        :class:`~aerospike_async.Txn`. Builders created from this session
        call this hook at construction and thread the result through every
        policy they hand to the PAC.

        Returns:
            The active :class:`~aerospike_async.Txn`, or ``None`` outside a
            transaction.

        Example:
            >>> session = client.create_session()
            >>> session.get_current_transaction() is None
            True
            >>> async with session.begin_transaction() as tx:
            ...     assert tx.get_current_transaction() is tx.txn

        See Also:
            :meth:`begin_transaction`: Enter a multi-record transaction.
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
        """
        return self._txn

    # -- Fast-path single-key operations ------------------------------------
    # These bypass the QueryBuilder/OperationSpec/RecordStream chain for
    # simple single-key reads and writes, calling the PAC directly.

    async def get(
        self, key: Key, bins: Optional[List[str]] = None,
    ) -> Record:
        """Direct single-key point read — returns ``Record`` or raises.

        Bypasses the builder chain (``session.query(key).execute()``) and
        the :class:`~aerospike_sdk.record_stream.RecordStream` wrapper: one
        ``await`` reaches the underlying client and the resulting
        :class:`~aerospike_async.Record` is returned unwrapped. Use when
        you have a single key and want minimum per-op overhead; use
        :meth:`query` when you need filters, projections, or streaming.

        Args:
            key: Target :class:`~aerospike_async.Key`.
            bins: Optional bin-name projection. ``None`` (default) reads
                all bins.

        Returns:
            The :class:`~aerospike_async.Record` for ``key``.

        Raises:
            AerospikeError: Server or client errors (including
                ``KEY_NOT_FOUND_ERROR``) are raised from the underlying
                client without being wrapped in a
                :class:`~aerospike_sdk.record_result.RecordResult`.

        Example:
            >>> users = DataSet.of("test", "users")
            >>> rec = await session.get(users.id(1))
            >>> name = rec.bins["name"]

        See Also:
            :meth:`query`: Builder-based reads for projections, streams,
                and secondary-index queries.
            :meth:`put`: Direct single-key upsert.
        """
        if self._txn is None:
            return await self._pac_client.get(
                self._cached_read_policy, key, bins)
        policy = to_read_policy(
            self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        policy.txn = self._txn
        return await self._pac_client.get(policy, key, bins)

    async def put(
        self, key: Key, bins: Dict[str, Any],
    ) -> None:
        """Direct single-key upsert — returns ``None`` or raises.

        Bypasses the builder chain (``session.upsert(key).put(...).execute()``)
        and the :class:`~aerospike_sdk.record_stream.RecordStream` wrapper:
        one ``await`` reaches the underlying client. Use when you have a
        single key and want minimum per-op overhead; use :meth:`upsert`
        when you need atomic multi-op semantics, TTL overrides,
        generation checks, durable delete, or filter expressions.

        Args:
            key: Target :class:`~aerospike_async.Key`.
            bins: Mapping of bin name to value to write. An empty mapping
                is permitted.

        Returns:
            ``None`` on success.

        Raises:
            AerospikeError: Server or client errors are raised from the
                underlying client.

        Example:
            >>> users = DataSet.of("test", "users")
            >>> await session.put(users.id(1), {"name": "Tim", "age": 30})

        See Also:
            :meth:`upsert`: Builder-based writes with full feature set.
            :meth:`get`: Direct single-key point read.
        """
        if self._txn is None:
            await self._pac_client.put(
                self._cached_write_policy, key, bins)
            return
        policy = to_write_policy(
            self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        policy.txn = self._txn
        await self._pac_client.put(policy, key, bins)

    @property
    def behavior(self) -> Behavior:
        """Policy bundle applied to operations created from this session.

        Returns:
            The :class:`~aerospike_sdk.policy.behavior.Behavior` passed to
            :meth:`Client.create_session`.
        """
        return self._behavior

    @property
    def client(self) -> Client:
        """SDK client that owns the connection used by this session.

        Returns:
            The parent :class:`Client`.
        """
        return self._client

    # Delegate all Client operations to maintain same API

    def batch(self) -> "BatchOperationBuilder":
        """Start a multi-key batch of mixed write operations executed in one server round trip.

        Chain ``insert``, ``update``, ``upsert``, ``replace``, ``delete``, and related
        bin builders, then ``await ...execute()`` to obtain per-key outcomes.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.batch.BatchOperationBuilder`
            for chaining operations.

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            results = await (
                session.batch()
                .insert(key1).put({"name": "Alice", "age": 25})
                .update(key2).bin("counter").add(1)
                .upsert(key3).put({"status": "active"})
                .delete(key4)
                .execute()
            )
            for row in results:
                print(row.key, row.result_code)

        See Also:
            :meth:`upsert`: Single-record writes without batching.
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        return BatchOperationBuilder(
            self._client._client, self._behavior, txn=self._txn,
        )

    def background_task(self) -> "BackgroundTaskSession":
        """Configure a server-side background job (query + scan scope) on a dataset.

        Call ``update``, ``delete``, ``touch``, or ``execute_udf`` on the returned
        object, add optional filters (for example ``where`` on supported builders),
        then ``await ...execute()`` to start work and receive an async task handle.

        Returns:
            A :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`
            for chaining the operation type and execution.

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            task = await (
                session.background_task()
                .delete(DataSet.of("test", "scratch"))
                .where("$.flag == 1")
                .execute()
            )
            await task.wait_till_complete(sleep_time=0.2, max_attempts=50)

        See Also:
            :meth:`execute_udf`: Foreground UDF on explicit keys.
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        return BackgroundTaskSession(self)

    def execute_udf(self, *keys: Key) -> "UdfFunctionBuilder":
        """Run a registered server-side UDF on one or more keys (foreground).

        Chain ``function(package, name)`` (package is the registered module name
        without ``.lua``), optional ``passing(*args)`` for Lua parameters, optional
        ``where`` for a filter expression, then ``await ...execute()`` to obtain a
        :class:`~aerospike_sdk.record_stream.RecordStream`. Multiple keys use a
        batch UDF; results preserve per-key order where applicable.

        Args:
            *keys: One or more :class:`~aerospike_async.Key` targets in the same
                namespace and set.

        Returns:
            :class:`~aerospike_sdk.aio.operations.udf.UdfFunctionBuilder` —
            call ``function`` next.

        Raises:
            ValueError: If no keys are given.
            RuntimeError: If the client is not connected.

        Example::

            users = DataSet.of("test", "users")
            stream = await (
                session.execute_udf(users.id("a"))
                .function("my_module", "my_fn")
                .passing("binName", 42)
                .execute()
            )
            value = await stream.first_udf_result()

        See Also:
            :meth:`query`: Read bins without UDF.
            :meth:`background_task`: Dataset-scoped background UDF.
        """
        if not keys:
            raise ValueError("At least one key is required")
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        first = keys[0]
        qb = QueryBuilder(
            self._client._client,
            first.namespace,
            first.set_name,
            self._behavior,
            indexes_monitor=self._client._indexes_monitor,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            txn=self._txn,
        )
        qb._set_current_keys_from_varargs(keys)
        return UdfFunctionBuilder(qb)

    # -- Internal helpers -----------------------------------------------------

    @staticmethod
    def _resolve_keys(
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *more_keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> List[Key]:
        """Resolve mixed positional/keyword arguments into a flat list of Keys."""
        all_keys: List[Key] = []

        if arg1 is not None:
            if isinstance(arg1, Key):
                all_keys.append(arg1)
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                all_keys.extend(more_keys)
            elif isinstance(arg1, list):
                if not arg1:
                    raise ValueError("keys list cannot be empty")
                all_keys.extend(arg1)
            else:
                raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        elif key is not None:
            all_keys.append(key)
        elif key_value is not None:
            if dataset is not None:
                all_keys.append(dataset.id(key_value))
            elif namespace is not None and set_name is not None:
                all_keys.append(Key(namespace, set_name, key_value))
            else:
                raise ValueError(
                    "Either dataset or (namespace and set_name) must be provided with key_value"
                )

        if not all_keys:
            raise ValueError("At least one key must be provided")
        return all_keys

    def _build_write_segment(
        self,
        op_type: str,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *more_keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Resolve keys and create a :class:`WriteSegmentBuilder`."""
        all_keys = self._resolve_keys(
            arg1, arg2, *more_keys,
            key=key, dataset=dataset,
            namespace=namespace, set_name=set_name, key_value=key_value,
        )
        first = all_keys[0]
        qb = QueryBuilder(
            client=self._client._client,
            namespace=first.namespace,
            set_name=first.set_name,
            behavior=self._behavior,
            indexes_monitor=self._client._indexes_monitor,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            txn=self._txn,
        )
        target: Union[Key, List[Key]] = all_keys[0] if len(all_keys) == 1 else all_keys
        return qb._start_write_verb(op_type, target)

    def _fast_write_segment(self, op_type: str, key: Key) -> WriteSegmentBuilder:
        """Single-key write shortcut: bypass QueryBuilder entirely."""
        return _SingleKeyWriteSegment(
            client=self._client._async_client,
            key=key,
            op_type=op_type,
            behavior=self._behavior,
            write_policy=self._cached_write_policy,
            read_policy=self._cached_read_policy,
            txn=self._txn,
        )

    # -- Read entry point -----------------------------------------------------

    @typing.overload
    def query(
        self,
        dataset: DataSet,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder from a DataSet."""
        ...

    @typing.overload
    def query(
        self,
        key: Key,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for a single Key (point read)."""
        ...

    @typing.overload
    def query(
        self,
        keys: List[Key],
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @typing.overload
    def query(
        self,
        *keys: Key,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @typing.overload
    def query(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Create a query builder with explicit namespace/set."""
        ...

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[Union[str, Key]] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
        behavior: Optional[Behavior] = None,
    ) -> QueryBuilder:
        """Start a read or secondary-index query for keys or a whole set.

        This session's :attr:`behavior` is applied to the underlying
        :class:`~aerospike_sdk.aio.operations.query.QueryBuilder`. Supported
        shapes include a :class:`~aerospike_sdk.dataset.DataSet` (set-wide
        query), a single :class:`~aerospike_async.Key`, multiple keys (list or
        varargs), or explicit ``namespace`` / ``set_name`` for index scans.

        Args:
            arg1: Positional dataset, key, list of keys, or namespace string
                (when paired with ``arg2`` as set name).
            arg2: When ``arg1`` is a namespace, the set name; otherwise may be
                a second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional argument is a key.
            namespace: Keyword namespace (with ``set_name``) when not using a
                dataset.
            set_name: Keyword set name (with ``namespace``).
            dataset: Keyword :class:`~aerospike_sdk.dataset.DataSet`.
            key: Keyword single key.
            keys_list: Keyword list of keys when not using ``arg1`` or varargs;
                forwarded to the client as ``keys``.
            behavior: Optional override for this query; defaults to the session's
                :attr:`behavior`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.QueryBuilder` to
            chain ``where``, ``bins``, ``execute``, etc.

        Raises:
            TypeError: If positional types do not match the supported overloads.
            ValueError: If a key list is empty or arguments are inconsistent.

        Example:
            users = DataSet.of("test", "users")
            rs = await session.query(users.id(1)).bins(["name"]).execute()
            row = await rs.first_or_raise()

        Example:
            users = DataSet.of("test", "users")
            rs = await session.query(users.ids(1, 2, 3)).bins(["name"]).execute()
            rows = await rs.collect()

        See Also:
            :meth:`Client.query`: Same shapes without session behavior.
            :meth:`upsert`: Writes for the same keys.
        """
        b = self._behavior if behavior is None else behavior
        # Handle positional arguments (SDK API)
        if arg1 is not None:
            if isinstance(arg1, DataSet):
                return self._bind_txn(
                    self._client.query(dataset=arg1, behavior=b))
            elif isinstance(arg1, Key):
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                else:
                    # Fast path for single-key queries: construct the
                    # QueryBuilder directly with cached policies to skip
                    # Client.query() overhead and per-op policy rebuilds.
                    builder = QueryBuilder(
                        client=self._client._async_client,
                        namespace=arg1.namespace,
                        set_name=arg1.set_name,
                        behavior=b,
                        indexes_monitor=self._client._indexes_monitor,
                        cached_read_policy=self._cached_read_policy,
                        cached_write_policy=self._cached_write_policy,
                        txn=self._txn,
                    )
                    builder._single_key = arg1
                    return builder
                return self._bind_txn(
                    self._client.query(keys=all_keys, behavior=b))
            elif isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(f"Expected List[Key], but first element is {type(arg1[0])}")
                return self._bind_txn(
                    self._client.query(keys=arg1, behavior=b))
            elif isinstance(arg1, str) and arg2 is not None:
                return self._bind_txn(
                    self._client.query(namespace=arg1, set_name=arg2, behavior=b))

        if keys:
            keys_list = list(keys)
            if arg1 is not None and isinstance(arg1, Key):
                keys_list.insert(0, arg1)
            if arg2 is not None and isinstance(arg2, Key):
                keys_list.insert(1 if arg1 is not None and isinstance(arg1, Key) else 0, arg2)
            return self._bind_txn(
                self._client.query(keys=keys_list, behavior=b))

        return self._bind_txn(self._client.query(  # type: ignore[call-overload]
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            key=key,
            keys=keys_list,
            behavior=b,
        ))

    @typing.overload
    def index(
        self,
        *,
        dataset: DataSet,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Create an index builder from a DataSet."""
        ...

    @typing.overload
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
        Create a secondary index builder for a namespace and set.

        Args:
            namespace: Namespace name when not using ``dataset``.
            set_name: Set name when not using ``dataset``.
            dataset: Optional :class:`~aerospike_sdk.dataset.DataSet` that
                supplies namespace and set.
            behavior: Reserved for symmetry with :meth:`query`; forwarded to
                :meth:`Client.index` but not used by index operations yet.

        Returns:
            :class:`~aerospike_sdk.aio.operations.index.IndexBuilder` for
                chaining index definition and creation.

        Raises:
            ValueError: If ``dataset`` is not given and ``namespace`` or
                ``set_name`` is missing.

        Example::

            users = DataSet.of("test", "users")
            await session.index(dataset=users).on_bin("age").named("age_idx").numeric().create()

        See Also:
            :meth:`Client.index`
        """
        if dataset is not None:
            return self._client.index(dataset=dataset, behavior=behavior)
        elif namespace is not None and set_name is not None:
            return self._client.index(
                namespace, set_name, behavior=behavior,
            )
        else:
            raise ValueError(
                "Invalid arguments. Use either:\n"
                "  - index(dataset=DataSet(...))\n"
                "  - index(namespace=..., set_name=...)"
            )

    def transaction_session(self) -> "TransactionalSession":
        """Create a transactional session using this session's behavior.

        Alias for :meth:`begin_transaction`.

        Returns:
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
            bound to this session's client and behavior.

        See Also:
            :meth:`begin_transaction`: Preferred entry point.
            :meth:`aerospike_sdk.aio.client.Client.transaction_session`
        """
        return self.begin_transaction()

    def begin_transaction(self) -> "TransactionalSession":
        """Start a multi-record transaction (MRT) using this session's behavior.

        Returns an async context manager that allocates a fresh
        :class:`~aerospike_async.Txn`. Every operation run on the returned
        session auto-participates in the transaction — builders stamp
        ``policy.txn = tx.txn`` under the hood, so user code never touches a
        policy object. On clean exit the transaction is committed; if an
        exception propagates out of the ``async with`` block the transaction
        is aborted.

        Example:
            >>> async with session.begin_transaction() as tx:
            ...     await tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
            ...     await tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

        Returns:
            :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
            bound to this session's client and behavior.

        See Also:
            :meth:`transaction_session`: Alias for this method.
            :meth:`do_in_transaction`: Run a callable inside a retrying MRT.
        """
        return self._client.transaction_session(behavior=self._behavior)

    @overload
    def info(self) -> InfoCommands: ...

    @overload
    def info(self, command: str) -> Awaitable[Dict[str, str]]: ...

    def info(
        self, command: Optional[str] = None
    ) -> Union[InfoCommands, Awaitable[Dict[str, str]]]:
        """
        Execute info commands or get the InfoCommands helper.

        With no argument, returns an InfoCommands instance for high-level
        helpers (namespaces(), namespace_details(), etc.) and for
        info_on_all_nodes().

        With a command string, runs the raw info command and returns its
        result (awaitable).

        Args:
            command: Optional. If given, the raw info command to run
                (e.g. "sindex-list", "build").

        Returns:
            If command is None: InfoCommands instance.
            If command is given: awaitable dict (node -> response).

        Example::

                # Raw command (no double .info)
                response = await session.info("sindex-list")

                # High-level helpers
                info = session.info()
                namespaces = await info.namespaces()
                by_node = await info.info_on_all_nodes("build")
        """
        if command is not None:
            return self._client._async_client.info(command)
        return InfoCommands(self)

    async def is_namespace_sc(self, namespace: str) -> bool:
        """
        Check if a namespace is in strong consistency (SC) mode.

        Strong consistency mode provides linearizable reads and writes
        at the cost of availability during network partitions.

        Args:
            namespace: The namespace name to check.

        Returns:
            True if the namespace is in strong consistency mode, False otherwise.

        Raises:
            ValueError: If the namespace is unknown or the info command fails.

        Example::

                if await session.is_namespace_sc("test"):
                    print("Namespace 'test' is in strong consistency mode")
                else:
                    print("Namespace 'test' is in AP (availability) mode")
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        try:
            # Query namespace configuration via info command
            result = await self._client._client.info(f"namespace/{namespace}")

            # Parse the result - it's a dict with node addresses as keys
            for node_result in result.values():
                # Parse semicolon-separated key=value pairs
                for pair in node_result.split(";"):
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        if key == "strong-consistency":
                            return value.lower() == "true"

            # If we didn't find the strong-consistency key, default to False (AP mode)
            return False

        except Exception as e:
            raise ValueError(f"Failed to check namespace '{namespace}': {e}") from e

    async def do_in_transaction(
        self,
        operation: typing.Callable[["TransactionalSession"], typing.Awaitable[typing.Any]],
        *,
        max_attempts: int = 5,
        sleep_between_retries: float = 0.0,
    ) -> typing.Any:
        """Run an async callable inside a retrying multi-record transaction.

        Creates a :class:`TransactionalSession`, invokes ``operation(tx)``
        inside ``async with``, and retries the whole block when the server
        signals a transient conflict (``MRT_BLOCKED``,
        ``MRT_VERSION_MISMATCH``, or ``TXN_FAILED``). On any non-transient
        failure the transaction is aborted and the exception re-raised.

        Args:
            operation: Async callable accepting a :class:`TransactionalSession`
                and performing zero or more operations on it. Its return
                value is returned from :meth:`do_in_transaction`.
            max_attempts: Maximum total attempts (initial + retries). Must
                be ``>= 1``. Defaults to ``5``.
            sleep_between_retries: Optional seconds to ``await asyncio.sleep``
                between retries. ``0`` (the default) retries immediately.

        Returns:
            Whatever ``operation`` returns on the successful attempt.

        Raises:
            ValueError: If ``max_attempts < 1``.
            AerospikeError: The last-seen transient error after
                ``max_attempts`` exhausted retries, or any non-transient
                error raised by ``operation``.

        Example:
            >>> async def transfer(tx):
            ...     await tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
            ...     await tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
            ...     return "ok"
            >>> result = await session.do_in_transaction(transfer)

        See Also:
            :meth:`begin_transaction`: Manual MRT lifecycle.
            :class:`TransactionalSession`
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        import asyncio
        from aerospike_sdk.exceptions import AerospikeError

        # Transient MRT conflicts that are safe to retry automatically.
        retryable_codes = {
            ResultCode.MRT_BLOCKED,
            ResultCode.MRT_VERSION_MISMATCH,
        }
        # TXN_FAILED is a rolled-up code used when the MRT monitor reports
        # that one or more ops failed — retrying is safe because we abort
        # and start fresh on each attempt.
        txn_failed = getattr(ResultCode, "TXN_FAILED", None)
        if txn_failed is not None:
            retryable_codes.add(txn_failed)

        last_exc: Optional[BaseException] = None
        for attempt in range(max_attempts):
            try:
                async with self.begin_transaction() as tx_session:
                    return await operation(tx_session)
            except AerospikeError as exc:
                last_exc = exc
                if exc.result_code not in retryable_codes:
                    raise
                if attempt + 1 >= max_attempts:
                    raise
                if sleep_between_retries > 0:
                    await asyncio.sleep(sleep_between_retries)
        # Unreachable — last iteration always raises — but keep mypy happy.
        assert last_exc is not None
        raise last_exc

    # -- Write entry points ---------------------------------------------------

    def upsert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a create-or-replace write for one or more keys.

        If the record exists, bins are merged according to the chained operations;
        if it does not exist, it is created. Use :meth:`insert` when the record
        must not already exist.

        Args:
            arg1: A single :class:`~aerospike_async.Key`, a list of keys, or omit
                and pass ``key`` / ``dataset`` + ``key_value`` / ``namespace`` +
                ``set_name`` + ``key_value``.
            arg2: Optional second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional is a key.
            key: Single key (keyword form).
            dataset: Dataset used with ``key_value`` to build a key.
            namespace: Namespace used with ``set_name`` and ``key_value``.
            set_name: Set name used with ``namespace`` and ``key_value``.
            key_value: User key value with ``dataset`` or ``namespace``/``set_name``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`
            for ``put``, ``bin``, ``where``, ``execute``, etc.

        Raises:
            ValueError: If no keys are resolved or lists are empty.
            TypeError: If positional arguments are not keys or lists of keys.

        Example:
            users = DataSet.of("test", "users")
            await session.upsert(users.id(1)).put({"name": "Tim", "age": 30}).execute()

        See Also:
            :meth:`insert`: Fails if the record already exists.
            :meth:`update`: Fails if the record does not exist.
            :meth:`replace`: Replace-entire-record semantics when configured.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("upsert", arg1)
        return self._build_write_segment(
            "upsert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def insert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a create-only write; fails on execute if the record already exists.

        Key resolution matches :meth:`upsert`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            await session.insert(users.id(99)).put({"name": "new"}).execute()

        See Also:
            :meth:`upsert`: Create or update.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("insert", arg1)
        return self._build_write_segment(
            "insert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def update(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start an update-only write; fails on execute if the record is missing.

        Key resolution matches :meth:`upsert`.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`upsert`: Create if missing.
            :meth:`replace_if_exists`: Replace semantics when the record exists.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("update", arg1)
        return self._build_write_segment(
            "update", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a full-record replace write (bins replaced per builder rules).

        Key resolution matches :meth:`upsert`. Prefer :meth:`replace_if_exists`
        when the record must already exist.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`replace_if_exists`: Replace only when the record exists.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("replace", arg1)
        return self._build_write_segment(
            "replace", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace_if_exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a replace write that requires an existing record.

        Key resolution matches :meth:`upsert`. On execute, missing keys surface
        as errors according to error strategy (default may raise).

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`replace`: Unconditional replace semantics.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("replace_if_exists", arg1)
        return self._build_write_segment(
            "replace_if_exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def delete(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a delete for one or more keys.

        Key resolution matches :meth:`upsert`. Chain filters or durable-delete
        options on the builder, then ``await ...execute()``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            await session.delete(users.id(1)).execute()
            await session.delete(users.ids(10, 11)).execute()

        See Also:
            :meth:`background_task`: Delete many records via a server job.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("delete", arg1)
        return self._build_write_segment(
            "delete", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def touch(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start a touch to refresh TTL without changing bins.

        Key resolution matches :meth:`upsert`. Use the builder to set TTL or
        related policy, then ``await ...execute()``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        See Also:
            :meth:`upsert`: Writes that can also set expiration via the builder.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("touch", arg1)
        return self._build_write_segment(
            "touch", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> WriteSegmentBuilder:
        """Start an existence check for one or more keys.

        Key resolution matches :meth:`upsert`. After ``execute``, use
        :meth:`~aerospike_sdk.record_result.RecordResult.as_bool` on each
        :class:`~aerospike_sdk.record_result.RecordResult` or inspect
        ``result_code``.

        Returns:
            A :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example:
            users = DataSet.of("test", "users")
            rs = await session.exists(users.id(1)).execute()
            exists = (await rs.first()).as_bool()

        See Also:
            :meth:`query`: Read record data when the key is known to exist.
        """
        if (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        ):
            return self._fast_write_segment("exists", arg1)
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    async def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set; this cannot be undone.

        Args:
            dataset: The DataSet to truncate.
            before_nanos: Optional timestamp in nanoseconds. Only records with
                last update time (LUT) less than this value are truncated.
                If None, all records in the set are truncated.

        Returns:
            None

        Raises:
            RuntimeError: If the client is not connected.

        Example::

            users = DataSet.of("test", "users")
            await session.truncate(users)

            cutoff_time = time.time_ns() - (24 * 60 * 60 * 10**9)  # 24 hours ago
            await session.truncate(users, before_nanos=cutoff_time)
        """
        # Access the underlying async client and call its truncate method
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        await self._client._client.truncate(
            dataset.namespace,
            dataset.set_name,
            before_nanos
        )

    def __repr__(self) -> str:
        """String representation of the session."""
        return f"Session(behavior={self._behavior.name!r})"

