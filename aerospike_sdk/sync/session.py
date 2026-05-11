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

"""Synchronous :class:`~aerospike_sdk.aio.session.Session` wrapper."""

from __future__ import annotations

import typing
from typing import Dict, List, Optional, TYPE_CHECKING, overload, Union

from aerospike_async import Key

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.session import NamespaceScStatus, Session as AsyncSession
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.background import SyncBackgroundTaskSession
from aerospike_sdk.sync.client import _EventLoopManager
from aerospike_sdk.sync.info import SyncInfoCommands
from aerospike_sdk.sync.operations.batch import SyncBatchOperationBuilder
from aerospike_sdk.sync.operations.index import SyncIndexBuilder
from aerospike_sdk.sync.operations.query import SyncQueryBuilder, SyncWriteSegmentBuilder
from aerospike_sdk.sync.operations.udf import SyncUdfFunctionBuilder

if TYPE_CHECKING:
    from aerospike_sdk.sync.transactional_session import SyncTransactionalSession


class SyncSession:
    """Run session-scoped reads and writes without ``async``/``await``.

    Constructed by :meth:`SyncClient.create_session
    <aerospike_sdk.sync.client.SyncClient.create_session>`, not by
    calling ``SyncSession(...)`` directly. Each method delegates to
    :class:`~aerospike_sdk.aio.session.Session` on a shared per-thread loop;
    return types are sync wrappers where the async API would return a coroutine
    or async stream.

    See Also:
        :class:`~aerospike_sdk.aio.session.Session`: Async API and behavior
            semantics.
    """

    def __init__(self, async_session: AsyncSession, loop_manager: _EventLoopManager) -> None:
        """Wrap ``async_session``; use :meth:`SyncClient.create_session` instead.

        Args:
            async_session: Connected async session (same behavior binding).
            loop_manager: Loop manager shared with the parent
                :class:`~aerospike_sdk.sync.client.SyncClient`.
        """
        self._async_session = async_session
        self._loop_manager = loop_manager

    @property
    def behavior(self) -> Behavior:
        """Get the behavior configuration for this session."""
        return self._async_session.behavior

    @property
    def client(self) -> Client:
        """Get the underlying Client."""
        return self._async_session.client

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
    ) -> "SyncWriteSegmentBuilder":
        """Delegate to async session's write segment builder and wrap in sync."""
        wsb = self._async_session._build_write_segment(
            op_type, arg1, arg2, *more_keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    def _fast_write_segment(self, op_type: str, key: Key) -> "SyncWriteSegmentBuilder":
        """Single-key write shortcut: delegate to async fast path and wrap."""
        wsb = self._async_session._fast_write_segment(op_type, key)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

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
    ) -> "SyncQueryBuilder":
        """Start a read or secondary-index query (synchronous session).

        Same shapes as :meth:`aerospike_sdk.aio.session.Session.query`, with
        this session's behavior applied on the underlying async builder.

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
            keys_list: Keyword list of keys for batch read.
            behavior: Optional per-query behavior override (same as async session).

        Returns:
            A :class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder`.
        """
        # Delegate to async session.query() - pass positional args as positional, keyword args as keyword
        if arg1 is not None or arg2 is not None or keys:
            # Has positional arguments - pass them positionally
            async_builder = self._async_session.query(  # type: ignore[call-overload]
                arg1, arg2, *keys, behavior=behavior,
            )
        else:
            # Only keyword arguments
            async_builder = self._async_session.query(  # type: ignore[call-overload]
                namespace=namespace,
                set_name=set_name,
                dataset=dataset,
                key=key,
                keys_list=keys_list,
                behavior=behavior,
            )
        return SyncQueryBuilder(
            async_client=self._async_session._client,
            namespace=async_builder._namespace,
            set_name=async_builder._set_name,
            loop_manager=self._loop_manager,
            query_builder=async_builder,
        )

    def batch(self) -> "SyncBatchOperationBuilder":
        """Start a multi-key batch of mixed write operations (synchronous).

        Chain ``insert``, ``update``, ``upsert``, ``replace``, ``delete``, and bin
        builders, then :meth:`~aerospike_sdk.sync.operations.batch.SyncBatchOperationBuilder.execute`
        for a :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

        Returns:
            A :class:`~aerospike_sdk.sync.operations.batch.SyncBatchOperationBuilder`.

        Raises:
            RuntimeError: If the client is not connected (from the async session).

        Example::

            stream = (
                session.batch()
                .insert(key1).put({"name": "Alice", "age": 25})
                .update(key2).bin("counter").add(1)
                .execute()
            )
            for row in stream:
                print(row.key, row.result_code)

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.batch`
        """
        inner = self._async_session.batch()
        return SyncBatchOperationBuilder(inner, self._loop_manager)

    def transaction_session(self) -> "SyncTransactionalSession":
        """Alias for :meth:`begin_transaction`.

        Returns:
            :class:`~aerospike_sdk.sync.transactional_session.SyncTransactionalSession`
            bound to this session's client and behavior.

        See Also:
            :meth:`begin_transaction`: Preferred entry point.
            :meth:`~aerospike_sdk.aio.session.Session.transaction_session`: Async equivalent.
        """
        return self.begin_transaction()

    def begin_transaction(self) -> "SyncTransactionalSession":
        """Start a multi-record transaction (MRT) using this session's behavior.

        Returns a context manager that allocates a fresh
        :class:`~aerospike_async.Txn`. Every operation run on the returned
        session auto-participates in the transaction — builders stamp
        ``policy.txn = tx.txn`` under the hood, so user code never touches a
        policy object. On clean exit the transaction is committed; if an
        exception propagates out of the ``with`` block the transaction is
        aborted.

        Example:
            >>> with session.begin_transaction() as tx:
            ...     tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
            ...     tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

        Returns:
            :class:`~aerospike_sdk.sync.transactional_session.SyncTransactionalSession`
            bound to this session's client and behavior.

        See Also:
            :meth:`transaction_session`: Alias for this method.
            :meth:`do_in_transaction`: Run a callable inside a retrying MRT.
            :meth:`~aerospike_sdk.aio.session.Session.begin_transaction`: Async equivalent.
        """
        from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

        async_txn_session = self._async_session.begin_transaction()
        return SyncTransactionalSession(async_txn_session, self._loop_manager)

    def do_in_transaction(
        self,
        operation: "typing.Callable[[SyncTransactionalSession], typing.Any]",
        *,
        max_attempts: int = 5,
        sleep_between_retries: float = 0.0,
    ) -> "typing.Any":
        """Run a callable inside a retrying multi-record transaction.

        Creates a :class:`SyncTransactionalSession`, invokes ``operation(tx)``
        inside ``with``, and retries the whole block when the server signals
        a transient conflict (``MRT_BLOCKED``, ``MRT_VERSION_MISMATCH``, or
        ``TXN_FAILED``). On any non-transient failure the transaction is
        aborted and the exception re-raised.

        Args:
            operation: Synchronous callable accepting a
                :class:`SyncTransactionalSession` and performing zero or more
                operations on it. Its return value is returned from
                :meth:`do_in_transaction`.
            max_attempts: Maximum total attempts (initial + retries). Must
                be ``>= 1``. Defaults to ``5``.
            sleep_between_retries: Optional seconds to ``time.sleep`` between
                retries. ``0`` (the default) retries immediately.

        Returns:
            Whatever ``operation`` returns on the successful attempt.

        Raises:
            ValueError: If ``max_attempts < 1``.
            AerospikeError: The last-seen transient error after
                ``max_attempts`` exhausted retries, or any non-transient
                error raised by ``operation``.

        Example:
            >>> def transfer(tx):
            ...     tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
            ...     tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
            ...     return "ok"
            >>> result = session.do_in_transaction(transfer)

        See Also:
            :meth:`begin_transaction`: Manual MRT lifecycle.
            :class:`SyncTransactionalSession`
            :meth:`~aerospike_sdk.aio.session.Session.do_in_transaction`: Async equivalent.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        import time
        from aerospike_async import ResultCode
        from aerospike_sdk.exceptions import AerospikeError

        retryable_codes = {
            ResultCode.MRT_BLOCKED,
            ResultCode.MRT_VERSION_MISMATCH,
        }
        txn_failed = getattr(ResultCode, "TXN_FAILED", None)
        if txn_failed is not None:
            retryable_codes.add(txn_failed)

        last_exc: Optional[BaseException] = None
        for attempt in range(max_attempts):
            try:
                with self.begin_transaction() as tx_session:
                    return operation(tx_session)
            except AerospikeError as exc:
                last_exc = exc
                if exc.result_code not in retryable_codes:
                    raise
                if attempt + 1 >= max_attempts:
                    raise
                if sleep_between_retries > 0:
                    time.sleep(sleep_between_retries)
        assert last_exc is not None
        raise last_exc

    def background_task(self) -> "SyncBackgroundTaskSession":
        """Start a background dataset task chain (synchronous).

        Returns:
            :class:`~aerospike_sdk.sync.background.SyncBackgroundTaskSession`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.background_task`.
        """
        inner = self._async_session.background_task()
        return SyncBackgroundTaskSession(inner, self._loop_manager)

    def execute_udf(self, *keys: Key) -> "SyncUdfFunctionBuilder":
        """Begin a foreground UDF invocation on the given keys (synchronous).

        Returns:
            :class:`~aerospike_sdk.sync.operations.udf.SyncUdfFunctionBuilder`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.execute_udf`.
        """
        inner = self._async_session.execute_udf(*keys)
        return SyncUdfFunctionBuilder(
            inner, self._loop_manager, self._async_session.client)

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> "SyncIndexBuilder":
        """Create a secondary-index builder for this namespace/set (synchronous).

        Raises:
            ValueError: If ``namespace`` and ``set_name`` are missing and no
                ``dataset`` is provided.

        Returns:
            :class:`~aerospike_sdk.sync.operations.index.SyncIndexBuilder`.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.index`.
        """
        _ = behavior
        # Resolve namespace and set_name from dataset if provided
        if dataset:
            namespace = dataset.namespace
            set_name = dataset.set_name

        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")

        return SyncIndexBuilder(
            async_client=self._async_session._client,
            namespace=namespace,
            set_name=set_name,
            loop_manager=self._loop_manager,
        )

    def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """Truncate (delete all records) from a set (synchronous)."""
        async def _truncate():
            await self._async_session.truncate(dataset, before_nanos)

        self._loop_manager.run_async(_truncate())

    def namespace_sc_status(self, namespace: str) -> NamespaceScStatus:
        """Describe whether a namespace is SC; includes a reason when it is not.

        See :meth:`aerospike_sdk.aio.session.Session.namespace_sc_status`.

        Args:
            namespace: The namespace name to check.

        Returns:
            :class:`~aerospike_sdk.aio.session.NamespaceScStatus`.

        Raises:
            RuntimeError: If the underlying client is not connected.
            ValueError: If the info command fails.
        """
        return self._loop_manager.run_async(
            self._async_session.namespace_sc_status(namespace)
        )

    def is_namespace_sc(self, namespace: str) -> bool:
        """Check if a namespace is in strong-consistency (SC) mode.

        Args:
            namespace: The namespace name to check.

        Returns:
            ``True`` if the namespace is configured for strong consistency,
            ``False`` otherwise.

        Raises:
            RuntimeError: If the underlying client is not connected.
            ValueError: If the namespace is unknown or the info command fails.

        Example::

            if session.is_namespace_sc("test_sc"):
                print("Namespace is SC — MRTs are supported here.")

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.namespace_sc_status`:
                Async equivalent (also available sync on :class:`SyncSession`).
            :meth:`~aerospike_sdk.aio.session.Session.is_namespace_sc`:
                Async equivalent.
        """
        return self._loop_manager.run_async(
            self._async_session.is_namespace_sc(namespace)
        )

    @overload
    def info(self) -> "SyncInfoCommands": ...

    @overload
    def info(self, command: str) -> Dict[str, str]: ...

    def info(
        self, command: Optional[str] = None
    ) -> Union["SyncInfoCommands", Dict[str, str]]:
        """
        Execute info commands or get the SyncInfoCommands helper (synchronous).

        With no argument, returns SyncInfoCommands for high-level helpers and
        info_on_all_nodes(). With a command string, runs the raw info command
        and returns its result.

        Example::

                response = session.info("sindex-list")
                info = session.info()
                by_node = info.info_on_all_nodes("build")
        """
        if command is not None:
            async def _info():
                return await self._async_session.info(command)
            return self._loop_manager.run_async(_info())
        return SyncInfoCommands(self._async_session.info(), self._loop_manager)

    def _is_single_key(
        self, arg1, arg2, keys, key, dataset, namespace, key_value,
    ) -> bool:
        return (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        )

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
    ) -> "SyncWriteSegmentBuilder":
        """Create an upsert write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("upsert", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create an insert write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("insert", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create an update write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("update", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create a replace write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("replace", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create a replace-if-exists write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("replace_if_exists", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create a delete write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("delete", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create a touch write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("touch", arg1)  # type: ignore[arg-type]
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
    ) -> "SyncWriteSegmentBuilder":
        """Create an exists-check write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
