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

"""Synchronous SDK session.

IO methods call PAC's ``_blocking`` entries; builder factories return
the synchronous builders (:class:`QueryBuilder`, etc.).
"""

from __future__ import annotations

import time
import typing
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union, overload

from aerospike_async import Key, Record, Txn, UDFLang

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.session_shared import NamespaceScStatus, SessionBase
from aerospike_sdk.policy.behavior import Behavior, OpKind, OpShape
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.policy_mapper import to_read_policy, to_write_policy
from aerospike_sdk.sync.background import SyncBackgroundTaskSession
from aerospike_sdk.sync.info import InfoCommands
from aerospike_sdk.sync.operations.index import IndexBuilder
from aerospike_sdk.sync.operations.query import (
    QueryBuilder, WriteSegmentBuilder,
)
from aerospike_sdk.sync.operations.udf import UdfFunctionBuilder

if TYPE_CHECKING:
    from aerospike_async import AdminPolicy, RegisterTask, UdfRemoveTask
    from aerospike_sdk.sync.client import SyncClient
    from aerospike_sdk.sync.transactional_session import TransactionalSession


class Session(SessionBase[WriteSegmentBuilder, QueryBuilder, "TransactionalSession"]):
    """Run session-scoped reads and writes without ``async``/``await``.

    Construct via :meth:`SyncClient.create_session
    <aerospike_sdk.sync.client.SyncClient.create_session>`, not directly.

    See Also:
        :class:`~aerospike_sdk.aio.session.Session`: Async equivalent.
    """

    def __init__(
        self, client: SyncClient, behavior: Behavior,
    ) -> None:
        """Attach a client and behavior; prefer :meth:`SyncClient.create_session`."""
        self._client = client
        self._behavior = behavior
        # Pre-compute base policies once per session so the fast-path
        # get/put + builder bypasses skip the policy_mapper for the common
        # no-override case. Cache both AP and SC variants so bypass paths
        # can pick the right policy per resolved namespace mode without
        # rebuilding. `_cached_*_policy` stays as the AP alias.
        self._refresh_cached_policies()
        # Config hot-reload pushes rebuilt policies into live sessions
        # (weak registration; no per-operation check).
        behavior._register_session(self)
        # Cache the PAC client for fast-path methods.
        self._pac_client = client.underlying_client
        # Non-transactional sessions always return None;
        # TransactionalSession overrides this to yield its active Txn.
        self._txn: Optional[Txn] = None

    # -- State accessors ------------------------------------------------------

    @property
    def client(self) -> SyncClient:
        """The owning :class:`SyncClient`."""
        return self._client

    def _resolve_namespace_mode_blocking(self, namespace: str) -> Mode:
        """Resolve AP vs SC for ``namespace`` synchronously (delegates to client)."""
        return self._client._resolve_namespace_mode_blocking(namespace)

    # -- Direct single-key fast paths -----------------------------------------

    def get(
        self, key: Key, bins: Optional[List[str]] = None,
    ) -> Record:
        """Direct single-key point read — returns ``Record`` or raises.

        Bypasses the builder chain (``session.query(key).execute()``) and
        the :class:`~aerospike_sdk.sync.record_stream.RecordStream` wrapper:
        one blocking call reaches the underlying client and the resulting
        :class:`~aerospike_async.Record` is returned unwrapped. Passes the
        AP + SC cached policies; PAC picks the right one based on the key's
        namespace mode (from the in-memory partition map).

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

        Example::

            users = DataSet.of("test", "users")
            rec = session.get(users.id(1))
            name = rec.bins["name"]

        See Also:
            :meth:`query`: Builder-based reads for projections, streams, and secondary-index queries.
            :meth:`put`: Direct single-key upsert.
        """
        if self._txn is None:
            return self._pac_client.get_blocking(
                key, bins,
                policy=self._cached_read_policy,
                policy_sc=self._cached_read_policy_sc,
            )
        # Under MRT the cached policies are skipped (txn not stamped);
        # rebuild a per-call policy from behavior.
        policy = to_read_policy(self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        policy.txn = self._txn
        return self._pac_client.get_blocking(key, bins, policy=policy)

    def put(self, key: Key, bins: Dict[str, Any]) -> None:
        """Direct single-key upsert — no builder, no stream — synchronous.

        Passes the AP + SC cached policies; PAC picks the right one based
        on the key's namespace mode.
        """
        if self._txn is None:
            self._pac_client.put_blocking(
                key, bins,
                policy=self._cached_write_policy,
                policy_sc=self._cached_write_policy_sc,
            )
            return
        policy = to_write_policy(
            self._behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        policy.txn = self._txn
        self._pac_client.put_blocking(key, bins, policy=policy)

    def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """Truncate a set, synchronously (PAC ``truncate_blocking``)."""
        self._pac_client.truncate_blocking(
            dataset.namespace, dataset.set_name, before_nanos,
        )

    # -- Info / namespace SC --------------------------------------------------

    def namespace_sc_status(self, namespace: str) -> NamespaceScStatus:
        """Describe whether a namespace is SC; includes a reason when it is not."""
        from aerospike_sdk.aio.session import _parse_namespace_info_body
        try:
            result = self._pac_client.info_blocking(f"namespace/{namespace}")
        except Exception as e:
            raise ValueError(f"Failed to check namespace '{namespace}': {e}") from e

        missing = False
        sc_val: Optional[bool] = None
        for node_result in result.values():
            if not node_result:
                continue
            exists, sc_opt = _parse_namespace_info_body(node_result)
            if not exists:
                missing = True
                break
            if sc_opt is not None:
                sc_val = sc_opt

        if missing:
            return NamespaceScStatus(
                False,
                f"Namespace {namespace!r} is not defined on this cluster "
                "(info reports type=unknown). Create it or set "
                "AEROSPIKE_SC_NAMESPACE to an existing SC namespace.",
            )
        if sc_val is True:
            return NamespaceScStatus(True, "")
        if sc_val is False:
            return NamespaceScStatus(
                False,
                f"Namespace {namespace!r} exists but strong-consistency is false "
                "(AP mode). Point AEROSPIKE_SC_NAMESPACE at a namespace with "
                "strong-consistency enabled.",
            )
        return NamespaceScStatus(
            False,
            f"Namespace {namespace!r} info did not report strong-consistency; treating as non-SC.",
        )

    def is_namespace_sc(self, namespace: str) -> bool:
        """``True`` if ``namespace`` is in strong-consistency mode."""
        return self.namespace_sc_status(namespace).is_sc

    @overload
    def info(self) -> InfoCommands: ...
    @overload
    def info(self, command: str) -> Dict[str, str]: ...

    def info(
        self, command: Optional[str] = None,
    ) -> Union[InfoCommands, Dict[str, str]]:
        """Sync info: return :class:`~aerospike_sdk.sync.info.InfoCommands` or raw blocking result."""
        if command is not None:
            return self._pac_client.info_blocking(command)
        return InfoCommands(self._pac_client)

    # -- Builder factories ----------------------------------------------------

    # ``query`` is inherited from SessionBase (shared arg normalization); only
    # the tree-specific builder construction lives here, behind the hooks the
    # base routes to. Both delegate to :meth:`_build_sync_query_builder`, which
    # is also reused by :meth:`execute_udf`.

    def _fast_query_builder(self, key: Key, behavior: Behavior) -> QueryBuilder:
        """Single-key query builder (bench-hot ``session.query(key)`` shape)."""
        return self._build_sync_query_builder(
            dataset=None, key=key, keys=None,
            namespace=None, set_name=None, behavior=behavior,
        )

    def _build_query_builder(
        self,
        *,
        dataset: Optional[DataSet],
        key: Optional[Key],
        keys: Optional[List[Key]],
        namespace: Optional[str],
        set_name: Optional[str],
        behavior: Behavior,
    ) -> QueryBuilder:
        """Dataset / multi-key / namespace query builder (non-single-key shapes)."""
        return self._build_sync_query_builder(
            dataset=dataset, key=key, keys=keys,
            namespace=namespace, set_name=set_name, behavior=behavior,
        )

    def _build_sync_query_builder(
        self,
        *,
        dataset: Optional[DataSet],
        key: Optional[Key],
        keys: Optional[List[Key]],
        namespace: Optional[str],
        set_name: Optional[str],
        behavior: Behavior,
    ) -> QueryBuilder:
        """Construct a :class:`QueryBuilder` with full session context.

        Returns the builder pre-populated with behavior, indexes monitor,
        cached policies, txn, and namespace-mode resolver.
        """
        if key is not None:
            builder = QueryBuilder(
                client=self._pac_client,
                namespace=key.namespace,
                set_name=key.set_name,
                behavior=behavior,
                indexes_monitor=self._client._indexes_monitor,
                cached_read_policy=self._cached_read_policy,
                cached_write_policy=self._cached_write_policy,
                cached_read_policy_sc=self._cached_read_policy_sc,
                cached_write_policy_sc=self._cached_write_policy_sc,
                txn=self._txn,
                namespace_mode_resolver=None,
                namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
                sdk_client=self._client,
            )
            builder._single_key = key
            return builder

        if keys is not None:
            ns = keys[0].namespace
            sn = keys[0].set_name
            builder = QueryBuilder(
                client=self._pac_client,
                namespace=ns,
                set_name=sn,
                behavior=behavior,
                indexes_monitor=self._client._indexes_monitor,
                cached_read_policy=self._cached_read_policy,
                cached_write_policy=self._cached_write_policy,
                cached_read_policy_sc=self._cached_read_policy_sc,
                cached_write_policy_sc=self._cached_write_policy_sc,
                txn=self._txn,
                namespace_mode_resolver=None,
                namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
                sdk_client=self._client,
            )
            builder._keys = keys
            return builder

        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        if namespace is None or set_name is None:
            raise ValueError(
                "Invalid arguments. Use one of: query(dataset=...), query(key=...), "
                "query(keys=[...]), or query(namespace=..., set_name=...).",
            )
        return QueryBuilder(
            client=self._pac_client,
            namespace=namespace,
            set_name=set_name,
            behavior=behavior,
            indexes_monitor=self._client._indexes_monitor,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            cached_read_policy_sc=self._cached_read_policy_sc,
            cached_write_policy_sc=self._cached_write_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=None,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            sdk_client=self._client,
        )

    def background_task(self) -> SyncBackgroundTaskSession:
        """Start a background dataset task chain (synchronous)."""
        from aerospike_sdk.aio.background import BackgroundTaskSession as _BTS

        # BackgroundTaskSession needs a session-like parent for behavior etc.
        # The aio variant accepts our sync session via duck typing; if not,
        # we'd need a thin proxy. The aio constructor only reads state, no IO.
        inner = _BTS(self)  # type: ignore[arg-type]
        return SyncBackgroundTaskSession(inner)

    def execute_udf(self, *keys: Key) -> UdfFunctionBuilder:
        """Begin a foreground UDF invocation (synchronous)."""
        if not keys:
            raise ValueError("execute_udf requires at least one key")
        builder = self._build_sync_query_builder(
            dataset=None, key=keys[0] if len(keys) == 1 else None,
            keys=list(keys) if len(keys) > 1 else None,
            namespace=None, set_name=None, behavior=self._behavior,
        )
        self._bind_txn(builder)
        builder._op_type = "execute_udf"
        return UdfFunctionBuilder(builder)

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> IndexBuilder:
        """Synchronous secondary-index builder."""
        _ = behavior
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")
        return IndexBuilder(
            async_client=self._client,
            namespace=namespace,
            set_name=set_name,
        )

    def _txn_session_cls(self) -> "type[TransactionalSession]":
        """Return the sync transactional-session class (late import breaks the cycle)."""
        from aerospike_sdk.sync.transactional_session import TransactionalSession
        return TransactionalSession

    def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF module from bytes (synchronous).

        Exposed here so the ``ClusterDefinition`` ➜ ``Cluster`` ➜ ``Session``
        path is self-sufficient.

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf`
        """
        return self._client._register_udf(body, server_path, language, policy=policy)

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
        return self._client._register_udf_from_file(
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
        return self._client._register_udf_from_resource(
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
        return self._client._remove_udf(server_path, policy=policy)

    def list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_udf`
        """
        return self._client._list_udf()

    def list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_indexes`
        """
        return self._client._list_indexes()

    def do_in_transaction(
        self,
        operation: "typing.Callable[[TransactionalSession], typing.Any]",
        *,
        max_attempts: int = 5,
        sleep_between_retries: float = 0.0,
    ) -> Any:
        """Run a callable inside a retrying multi-record transaction (synchronous)."""
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

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
                with self.transaction() as tx_session:
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

    # -- Write-verb factories -------------------------------------------------
    # The eight public write verbs (upsert/insert/.../exists) and the
    # single-key predicate live on `SessionBase`; these two helpers stay
    # per-leaf because they construct the sync builders and wire the sync
    # namespace-mode resolver.

    def _fast_write_segment(self, op_type: str, key: Key) -> WriteSegmentBuilder:
        """Single-key fast-path write segment (sync)."""
        from aerospike_sdk.sync.operations.query import _SingleKeyWriteSegment

        return _SingleKeyWriteSegment(
            client=self._pac_client,
            key=key,
            op_type=op_type,
            behavior=self._behavior,
            write_policy=self._cached_write_policy,
            read_policy=self._cached_read_policy,
            write_policy_sc=self._cached_write_policy_sc,
            read_policy_sc=self._cached_read_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=None,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            sdk_client=self._client,
        )

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
        """Build a multi-key / dataset write segment via aio QueryBuilder."""
        # Reduce overload args to either a single key, a list of keys, or a dataset.
        single_key: Optional[Key] = None
        many_keys: Optional[List[Key]] = None
        if key is not None:
            single_key = key
        elif isinstance(arg1, Key) and not more_keys and arg2 is None:
            single_key = arg1
        elif isinstance(arg1, list):
            many_keys = list(arg1)
        elif isinstance(arg1, Key):
            many_keys = [arg1]
            if isinstance(arg2, Key):
                many_keys.append(arg2)
            many_keys.extend(more_keys)
        elif dataset is not None:
            pass
        elif namespace is not None and set_name is not None:
            if key_value is not None:
                ds = DataSet.of(namespace, set_name)
                single_key = ds.id(key_value)
            # else: keyless dataset op (rare for write segments)
        elif key_value is not None and dataset is None:
            raise ValueError("key_value requires dataset or namespace+set_name")

        qb = self._build_sync_query_builder(
            dataset=dataset, key=single_key, keys=many_keys,
            namespace=namespace, set_name=set_name,
            behavior=self._behavior,
        )
        self._bind_txn(qb)
        qb._op_type = op_type
        return WriteSegmentBuilder(qb)


# Path-differentiated bare name is the committed convention (same as the aio
# class); the ``Sync``-prefixed alias stays importable for one deprecation
# cycle (removed at GA).
SyncSession = Session
