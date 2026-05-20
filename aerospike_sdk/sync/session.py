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
synchronous wrappers (:class:`SyncQueryBuilder`,
:class:`SyncBatchOperationBuilder`, etc.).
"""

from __future__ import annotations

import time
import typing
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union, overload

from aerospike_async import Key, Record, Txn

from aerospike_sdk.aio.session import NamespaceScStatus
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior, OpKind, OpShape
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.policy_mapper import to_read_policy, to_write_policy
from aerospike_sdk.sync.background import SyncBackgroundTaskSession
from aerospike_sdk.sync.info import SyncInfoCommands
from aerospike_sdk.sync.operations.batch import SyncBatchOperationBuilder
from aerospike_sdk.sync.operations.index import SyncIndexBuilder
from aerospike_sdk.sync.operations.query import (
    SyncQueryBuilder, SyncWriteSegmentBuilder,
)
from aerospike_sdk.sync.operations.udf import SyncUdfFunctionBuilder

if TYPE_CHECKING:
    from aerospike_sdk.sync.client import SyncClient
    from aerospike_sdk.sync.transactional_session import SyncTransactionalSession


class SyncSession:
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
        # get/put skip the policy_mapper for the common no-override case.
        self._cached_read_policy = to_read_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT))
        self._cached_write_policy = to_write_policy(
            behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
        # Cache the PAC client for fast-path methods.
        self._pac_client = client.underlying_client
        # Non-transactional sessions always return None;
        # SyncTransactionalSession overrides this to yield its active Txn.
        self._txn: Optional[Txn] = None

    # -- State accessors ------------------------------------------------------

    @property
    def behavior(self) -> Behavior:
        """The behavior configuration for this session."""
        return self._behavior

    @property
    def client(self) -> SyncClient:
        """The owning :class:`SyncClient`."""
        return self._client

    def _resolve_namespace_mode_blocking(self, namespace: str) -> Mode:
        """Resolve AP vs SC for ``namespace`` synchronously (delegates to client)."""
        return self._client._resolve_namespace_mode_blocking(namespace)

    def _bind_txn(self, builder):
        """Stamp the session's current txn onto a builder if one is active."""
        if self._txn is not None:
            builder.with_txn(self._txn)
        return builder

    def get_current_transaction(self) -> Optional[Txn]:
        """Return the active transaction for this session, or ``None``."""
        return self._txn

    # -- Direct single-key fast paths -----------------------------------------

    def get(
        self, key: Key, *, bins: Optional[List[str]] = None,
    ) -> Optional[Record]:
        """Direct single-key read — no builder, no stream — synchronous.

        Calls PAC ``get_blocking`` once with the session-cached
        :class:`~aerospike_async.ReadPolicy`.
        """
        if self._txn is None:
            return self._pac_client.get_blocking(key, bins, policy=self._cached_read_policy)
        policy = to_read_policy(
            self._behavior.get_settings(OpKind.READ, OpShape.POINT))
        policy.txn = self._txn
        return self._pac_client.get_blocking(key, bins, policy=policy)

    def put(self, key: Key, bins: Dict[str, Any]) -> None:
        """Direct single-key upsert — no builder, no stream — synchronous.

        Calls PAC ``put_blocking`` once with the session-cached
        :class:`~aerospike_async.WritePolicy`.
        """
        if self._txn is None:
            self._pac_client.put_blocking(key, bins, policy=self._cached_write_policy)
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
            f"Namespace {namespace!r} info did not report strong-consistency; "
            "treating as non-SC.",
        )

    def is_namespace_sc(self, namespace: str) -> bool:
        """``True`` if ``namespace`` is in strong-consistency mode."""
        return self.namespace_sc_status(namespace).is_sc

    @overload
    def info(self) -> SyncInfoCommands: ...
    @overload
    def info(self, command: str) -> Dict[str, str]: ...

    def info(
        self, command: Optional[str] = None,
    ) -> Union[SyncInfoCommands, Dict[str, str]]:
        """Sync info: return :class:`SyncInfoCommands` or raw blocking result."""
        if command is not None:
            return self._pac_client.info_blocking(command)
        return SyncInfoCommands(self._pac_client)

    # -- Builder factories ----------------------------------------------------

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
    ) -> SyncQueryBuilder:
        """Start a synchronous read or secondary-index query.

        Same shapes as :meth:`Session.query
        <aerospike_sdk.aio.session.Session.query>`. Always returns
        :class:`SyncQueryBuilder` whose ``execute()`` runs synchronously.
        """
        b = self._behavior if behavior is None else behavior

        # Normalize positional/kw args.
        if arg1 is not None:
            if isinstance(arg1, DataSet):
                dataset = arg1
            elif isinstance(arg1, Key):
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                if len(all_keys) == 1:
                    key = arg1
                else:
                    keys_list = all_keys
            elif isinstance(arg1, list):
                if not arg1:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(
                        f"Expected List[Key], got first element {type(arg1[0])}",
                    )
                keys_list = arg1
            elif isinstance(arg1, str) and arg2 is not None and isinstance(arg2, str):
                namespace = arg1
                set_name = arg2
            else:
                raise TypeError(f"Unsupported arg1 type: {type(arg1)}")

        sync_builder = self._build_sync_query_builder(
            dataset=dataset, key=key, keys=keys_list,
            namespace=namespace, set_name=set_name, behavior=b,
        )
        self._bind_txn(sync_builder)
        return sync_builder

    def _build_sync_query_builder(
        self,
        *,
        dataset: Optional[DataSet],
        key: Optional[Key],
        keys: Optional[List[Key]],
        namespace: Optional[str],
        set_name: Optional[str],
        behavior: Behavior,
    ) -> SyncQueryBuilder:
        """Construct a :class:`SyncQueryBuilder` with full session context.

        Returns the builder pre-populated with behavior, indexes monitor,
        cached policies, txn, and namespace-mode resolver.
        """
        if key is not None:
            builder = SyncQueryBuilder(
                client=self._pac_client,
                namespace=key.namespace,
                set_name=key.set_name,
                behavior=behavior,
                indexes_monitor=self._client._indexes_monitor,
                cached_read_policy=self._cached_read_policy,
                cached_write_policy=self._cached_write_policy,
                txn=self._txn,
                namespace_mode_resolver=None,
                namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            )
            builder._single_key = key
            return builder

        if keys is not None:
            ns = keys[0].namespace
            sn = keys[0].set_name
            builder = SyncQueryBuilder(
                client=self._pac_client,
                namespace=ns,
                set_name=sn,
                behavior=behavior,
                indexes_monitor=self._client._indexes_monitor,
                cached_read_policy=self._cached_read_policy,
                cached_write_policy=self._cached_write_policy,
                txn=self._txn,
                namespace_mode_resolver=None,
                namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
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
        return SyncQueryBuilder(
            client=self._pac_client,
            namespace=namespace,
            set_name=set_name,
            behavior=behavior,
            indexes_monitor=self._client._indexes_monitor,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            txn=self._txn,
            namespace_mode_resolver=None,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
        )

    def batch(self) -> SyncBatchOperationBuilder:
        """Start a multi-key batch of mixed write operations (synchronous)."""
        from aerospike_sdk.aio.operations.batch import BatchOperationBuilder as _Batch

        inner = _Batch(
            client=self._pac_client,
            behavior=self._behavior,
            txn=self._txn,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
        )
        return SyncBatchOperationBuilder(inner)

    def background_task(self) -> SyncBackgroundTaskSession:
        """Start a background dataset task chain (synchronous)."""
        from aerospike_sdk.aio.background import BackgroundTaskSession as _BTS

        # BackgroundTaskSession needs a session-like parent for behavior etc.
        # The aio variant accepts our sync session via duck typing; if not,
        # we'd need a thin proxy. The aio constructor only reads state, no IO.
        inner = _BTS(self)  # type: ignore[arg-type]
        return SyncBackgroundTaskSession(inner)

    def execute_udf(self, *keys: Key) -> SyncUdfFunctionBuilder:
        """Begin a foreground UDF invocation (synchronous)."""
        from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder as _UFB

        if not keys:
            raise ValueError("execute_udf requires at least one key")
        builder = self._build_sync_query_builder(
            dataset=None, key=keys[0] if len(keys) == 1 else None,
            keys=list(keys) if len(keys) > 1 else None,
            namespace=None, set_name=None, behavior=self._behavior,
        )
        self._bind_txn(builder)
        builder._op_type = "execute_udf"
        inner = _UFB(builder)
        return SyncUdfFunctionBuilder(inner, self._client)

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> SyncIndexBuilder:
        """Synchronous secondary-index builder."""
        _ = behavior
        if dataset is not None:
            namespace = dataset.namespace
            set_name = dataset.set_name
        if not namespace or not set_name:
            raise ValueError("namespace and set_name are required (or provide dataset)")
        return SyncIndexBuilder(
            async_client=self._client,
            namespace=namespace,
            set_name=set_name,
        )

    def transaction_session(self) -> SyncTransactionalSession:
        """Alias for :meth:`begin_transaction`."""
        return self.begin_transaction()

    def begin_transaction(self) -> SyncTransactionalSession:
        """Start a multi-record transaction (synchronous)."""
        from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

        return SyncTransactionalSession(client=self._client, behavior=self._behavior)

    def do_in_transaction(
        self,
        operation: "typing.Callable[[SyncTransactionalSession], typing.Any]",
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

    # -- Write-verb factories -------------------------------------------------

    def _is_single_key(
        self, arg1, arg2, keys, key, dataset, namespace, key_value,
    ) -> bool:
        return (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        )

    def _fast_write_segment(self, op_type: str, key: Key) -> SyncWriteSegmentBuilder:
        """Single-key fast-path write segment (sync)."""
        from aerospike_sdk.sync.operations.query import SyncSingleKeyWriteSegment

        return SyncSingleKeyWriteSegment(
            client=self._pac_client,
            key=key,
            op_type=op_type,
            behavior=self._behavior,
            write_policy=self._cached_write_policy,
            read_policy=self._cached_read_policy,
            txn=self._txn,
            namespace_mode_resolver=None,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
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
    ) -> SyncWriteSegmentBuilder:
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
        return SyncWriteSegmentBuilder(qb)

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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
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
    ) -> SyncWriteSegmentBuilder:
        """Create an exists-check write segment (synchronous)."""
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )
