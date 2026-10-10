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

import asyncio
import os
import typing
from datetime import timedelta
from typing import (
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    overload,
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    from aerospike_native import (
        AdminPolicy,
        CollectionIndexType,
        CTX,
        DropIndexTask,
        FilterExpression,
        IndexTask,
        IndexType,
        RegisterTask,
        UdfRemoveTask,
    )
    from aerospike_sdk.aio.client import Client
    from aerospike_sdk.aio.transactional_session import TransactionalSession

from aerospike_native import Key, Record, Txn, UDFLang

from aerospike_sdk.txn_shared import (
    is_retryable_txn_error,
    resolve_retry_plan,
    retry_delay_seconds,
)
from aerospike_sdk.aio.background import BackgroundTaskSession
from aerospike_sdk.aio.info import InfoCommands
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import (
    DataSetWriteBuilder,
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.info_types import NamespaceDetail
from aerospike_sdk.exceptions import (
    AerospikeError,
    PncAerospikeError,
    PncServerError,
    _convert_pnc_exception,
)
from aerospike_sdk.policy.behavior import Behavior, OpKind, OpShape
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.policy_mapper import to_read_policy, to_write_policy
from aerospike_sdk.metrics import usage
from aerospike_sdk.session_shared import (
    NamespaceScStatus,
    SessionBase,
    _namespace_mode_from_partition_map,
    _namespace_sc_status,
    _probe_namespace_mode_blocking,
)


# Gates for the transparent same-tick coalescer (on by default; PSDK_COALESCE=0
# disables both directions, PSDK_COALESCE_WRITES=0 leaves reads fusing but sends
# writes direct). Fusing mechanism + transparency contract are documented at the
# dispatch sites (:meth:`Session._coalesced_get`, :meth:`Session._coalesced_put`);
# the measured lift is in docs/guide/performance.md ("Async operation coalescing").
# Writes get their own gate because the two directions carry different overheads —
# a buffered write also copies its payload — so their break-even fan-in differs.
_COALESCE = os.environ.get("PSDK_COALESCE", "1") != "0"
_COALESCE_WRITES = _COALESCE and os.environ.get("PSDK_COALESCE_WRITES", "1") != "0"


def _convert_window_slots(slots: List[Any]) -> List[Any]:
    # Only reached when the window submission reported at least one failed
    # slot, so failure-free windows never pay this scan.
    return [
        _convert_pnc_exception(slot)
        if isinstance(slot, (PncServerError, PncAerospikeError)) else slot
        for slot in slots
    ]


class Session(
    SessionBase[WriteSegmentBuilder, QueryBuilder, "TransactionalSession", DataSetWriteBuilder],
):
    """Perform reads and writes against Aerospike with a fixed :class:`~aerospike_sdk.policy.behavior.Behavior`.

    A session binds a connected ``Client`` to policy defaults (timeouts,
    retries, replica preferences) for every operation started from it. Create
    sessions with :meth:`Client.create_session`; do not construct
    ``Session`` directly.

    Example::

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
            client: Connected (or not yet connected) ``Client``.
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
        # Cache both AP and SC variants so the bypass paths can pick the
        # right policy per resolved namespace mode without rebuilding.
        # `_cached_*_policy` stays as the AP alias (matches behavior's default
        # mode); session.get/put still use it.
        self._refresh_cached_policies()
        # Config hot-reload pushes rebuilt policies into live sessions
        # (weak registration; no per-operation check).
        behavior._register_session(self)
        # Cache the raw PNC client for fast-path methods.
        self._pnc_client = client._async_client
        # Transaction hook. Non-transactional sessions always return None;
        # TransactionalSession overrides this to yield its active Txn so every
        # builder spawned from the session auto-participates.
        self._txn: Optional[Txn] = None
        # Transparent coalescer state (prototype; single-loop assumption).
        # Only touched when _COALESCE is enabled. Reads and writes buffer
        # separately because they are distinct submissions, but share one armed
        # flush so a mixed tick still pays a single call_soon.
        self._coalesce_keys: List[Key] = []
        self._coalesce_futs: List[Any] = []
        self._coalesce_write_keys: List[Key] = []
        self._coalesce_write_bins: List[Dict[str, Any]] = []
        self._coalesce_write_futs: List[Any] = []
        self._coalesce_scheduled = False

    def session_for(self, behavior: Behavior) -> "Session":
        """A new session on the same cluster with a different behavior.

        Shorthand for ``cluster.create_session(behavior)``; this session is
        left as it is. Called on a transactional session it returns a plain
        session, outside the transaction.

        Args:
            behavior: The behavior for the new session.

        Returns:
            A new :class:`Session`.

        Example::

            reads = session.session_for(Behavior.READ_FAST)
            row = await (await reads.query(key).execute()).first_or_raise()
        """
        return self._client.create_session(behavior)

    async def _resolve_namespace_mode(self, namespace: str) -> Mode:
        """Return :class:`Mode`.SC or AP for *namespace* (cached on the client)."""
        cache = self._client._namespace_mode_cache
        mode = cache.get(namespace)
        if mode is not None:
            return mode
        mode = _namespace_mode_from_partition_map(self._pnc_client, cache, namespace)
        if mode is not None:
            return mode
        return Mode.SC if await self.is_namespace_sc(namespace) else Mode.AP

    def _resolve_namespace_mode_blocking(self, namespace: str) -> Mode:
        """Sync equivalent of :meth:`_resolve_namespace_mode`.

        Shares the per-client cache, so either resolver primes it for both.
        Used by the ``execute_blocking`` family on builders when the sync
        builder chain runs without an asyncio loop.
        """
        cache = self._client._namespace_mode_cache
        mode = cache.get(namespace)
        if mode is not None:
            return mode
        mode = _namespace_mode_from_partition_map(self._pnc_client, cache, namespace)
        if mode is not None:
            return mode
        return _probe_namespace_mode_blocking(self._pnc_client, namespace)

    # -- Fast-path single-key operations ------------------------------------
    # These bypass the QueryBuilder/OperationSpec/RecordStream chain for
    # simple single-key reads and writes, calling the PNC directly.

    async def get(
        self, key: Key, bins: Optional[List[str]] = None,
    ) -> Record:
        """Direct single-key point read — returns ``Record`` or raises.

        Bypasses the builder chain (``session.query(key).execute()``) and
        the :class:`~aerospike_sdk.record_stream.RecordStream` wrapper: one
        ``await`` reaches the underlying client and the resulting
        :class:`~aerospike_native.Record` is returned unwrapped. Use when
        you have a single key and want minimum per-op overhead; use
        :meth:`query` when you need filters, projections, or streaming.

        Args:
            key: Target :class:`~aerospike_native.Key`.
            bins: Optional bin-name projection. ``None`` (default) reads
                all bins.

        Returns:
            The :class:`~aerospike_native.Record` for ``key``.

        Raises:
            AerospikeError: Server or client errors (including
                ``KEY_NOT_FOUND_ERROR``) are raised as the SDK exception
                type for the failure, without being wrapped in a
                :class:`~aerospike_sdk.record_result.RecordResult`.

        Example::

            users = DataSet.of("test", "users")
            rec = await session.get(users.id(1))
            name = rec.bins["name"]

        See Also:
            :meth:`query`: Builder-based reads for projections, streams, and secondary-index queries.
            :meth:`put`: Direct single-key upsert.
        """
        try:
            if self._txn is None:
                if _COALESCE and bins is None:
                    return await self._coalesced_get(key)
                return await self._pnc_client.get(
                    key, bins,
                    policy=self._cached_read_policy,
                    policy_sc=self._cached_read_policy_sc,
                )
            policy = to_read_policy(
                self._behavior.get_settings(OpKind.READ, OpShape.POINT))
            policy.txn = self._txn
            return await self._pnc_client.get(key, bins, policy=policy)
        except (PncServerError, PncAerospikeError) as e:
            raise _convert_pnc_exception(e) from e

    async def _coalesced_get(self, key: Key) -> Record:
        """Opportunistic same-tick coalescing for a no-projection read.

        The *first* ``get`` of an event-loop iteration finds no flush armed, so
        it dispatches **directly** (paying none of the coalescer's buffer /
        create_task machinery) and arms a single :meth:`_flush_coalesced`.
        Any *followers* in the same tick see the armed flush and enqueue, so
        they fuse into one ``_submit_coalesced_read`` crossing that resolves
        each future the instant its own key returns (per-op delivery, no
        head-of-line). A lone or low-rate ``get`` is therefore just a direct
        :meth:`get`; under load all but the tick's first op coalesce. The record
        (or raised exception) is byte-identical to a direct read either way.
        """
        loop = asyncio.get_running_loop()
        if self._coalesce_scheduled:
            fut = loop.create_future()
            self._coalesce_keys.append(key)
            self._coalesce_futs.append(fut)
            return await fut
        self._coalesce_scheduled = True
        loop.call_soon(self._flush_coalesced)
        return await self._pnc_client.get(
            key, None,
            policy=self._cached_read_policy,
            policy_sc=self._cached_read_policy_sc,
        )

    async def _coalesced_put(self, key: Key, bins: Dict[str, Any]) -> None:
        """Opportunistic same-tick coalescing for a point write.

        Write-side mirror of :meth:`_coalesced_get`: the tick's first ``put``
        dispatches directly and arms the flush, and followers enqueue to fuse
        into one ``_submit_coalesced_write`` crossing carrying a per-key
        payload each. ``None`` (or the raised exception) is delivered exactly
        as a direct write delivers it.

        Buffered payloads are copied because the submission is deferred to the
        flush: a caller that reuses one dict across concurrent writes would
        otherwise have every buffered op see its final contents, where a direct
        write converts the payload before returning.
        """
        loop = asyncio.get_running_loop()
        if self._coalesce_scheduled:
            fut = loop.create_future()
            self._coalesce_write_keys.append(key)
            self._coalesce_write_bins.append(dict(bins))
            self._coalesce_write_futs.append(fut)
            await fut
            return
        self._coalesce_scheduled = True
        loop.call_soon(self._flush_coalesced)
        await self._pnc_client.put(
            key, bins,
            policy=self._cached_write_policy,
            policy_sc=self._cached_write_policy_sc,
        )

    def _flush_coalesced(self) -> None:
        """Fuse the tick's follower ops into one crossing per direction.

        Either buffer can be empty — the tick's first op of that kind went
        direct, and a read-only or write-only tick never fills the other.

        This runs as a loop callback, so an exception escaping it would be
        reported to the loop's handler while the callers of the drained buffers
        awaited futures nothing would ever complete. Every submission is
        therefore responsible for resolving the futures it took ownership of,
        including when it fails.
        """
        self._coalesce_scheduled = False
        # Fire-and-forget: PNC resolves each future through the bridge's drainer
        # the instant its own op completes (per-op delivery — no head-of-line).
        keys = self._coalesce_keys
        if keys:
            futs = self._coalesce_futs
            self._coalesce_keys = []
            self._coalesce_futs = []
            try:
                self._pnc_client._submit_coalesced_read(
                    keys, futs, None,
                    policy=self._cached_read_policy,
                    policy_sc=self._cached_read_policy_sc,
                )
            except Exception as exc:
                # A read window carries no per-key conversion, so the only way
                # to get here is a whole-submission failure (e.g. a client
                # closed mid-tick) that applies equally to every key.
                for fut in futs:
                    if not fut.done():
                        fut.set_exception(exc)
        write_keys = self._coalesce_write_keys
        if write_keys:
            write_futs = self._coalesce_write_futs
            write_bins = self._coalesce_write_bins
            self._coalesce_write_keys = []
            self._coalesce_write_futs = []
            self._coalesce_write_bins = []
            try:
                self._pnc_client._submit_coalesced_write(
                    write_keys, write_futs, write_bins,
                    policy=self._cached_write_policy,
                    policy_sc=self._cached_write_policy_sc,
                )
            except Exception:
                self._resubmit_writes_individually(
                    write_keys, write_futs, write_bins)

    def _resubmit_writes_individually(
        self, keys: List[Key], futs: List[Any], bins_list: List[Dict[str, Any]],
    ) -> None:
        """Retry a failed write window one op at a time.

        The batched crossing converts every payload before submitting anything,
        so a single unconvertible value aborts the whole window. Dispatching the
        survivors individually keeps a buffered write's outcome identical to a
        direct one: only the caller that passed a bad payload sees the error.
        """
        for key, fut, bins in zip(keys, futs, bins_list):
            if fut.done():
                continue
            try:
                self._pnc_client._submit_coalesced_write(
                    [key], [fut], [bins],
                    policy=self._cached_write_policy,
                    policy_sc=self._cached_write_policy_sc,
                )
            except Exception as exc:
                fut.set_exception(exc)

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
            key: Target :class:`~aerospike_native.Key`.
            bins: Mapping of bin name to value to write. An empty mapping
                is permitted.

        Returns:
            ``None`` on success.

        Raises:
            AerospikeError: Server or client errors are raised as the SDK
                exception type for the failure.

        Example::

            users = DataSet.of("test", "users")
            await session.put(users.id(1), {"name": "Tim", "age": 30})

        See Also:
            :meth:`upsert`: Builder-based writes with full feature set.
            :meth:`get`: Direct single-key point read.
        """
        try:
            if self._txn is None:
                if _COALESCE_WRITES:
                    await self._coalesced_put(key, bins)
                    return
                await self._pnc_client.put(
                    key, bins,
                    policy=self._cached_write_policy,
                    policy_sc=self._cached_write_policy_sc,
                )
                return
            policy = to_write_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
            policy.txn = self._txn
            await self._pnc_client.put(key, bins, policy=policy)
        except (PncServerError, PncAerospikeError) as e:
            raise _convert_pnc_exception(e) from e

    async def get_many(
        self, keys: List[Key], bins: Optional[List[str]] = None,
    ) -> List[Union[Record, AerospikeError]]:
        """Direct point reads for a window of keys — one await, one result list.

        Client-side fusion of independent single-record reads: one call
        submits the whole window and one completion delivers all results,
        so per-op submission and wakeup overhead is amortized across the
        window. These are NOT a server batch request — each key remains an
        independent wire op (use :meth:`query` with a key list for wire
        batching).

        Args:
            keys: Target keys; results are positional.
            bins: Optional bin-name projection shared by the window.

        Returns:
            A list the same length as ``keys``. Each slot is the
            :class:`~aerospike_native.Record` for that key, or the
            :class:`~aerospike_sdk.exceptions.AerospikeError` instance (not
            raised) for that key — check with
            ``isinstance(slot, AerospikeError)``. One failed key never fails
            its window-mates.

            Records are handed back raw rather than wrapped in
            :class:`~aerospike_sdk.record_result.RecordResult`: a wrapper per
            slot would cost one allocation per key, which is the per-op cost
            this API exists to amortize. The asymmetry with the builder
            surface is deliberate.

        Raises:
            AerospikeError: When the whole-window submission fails (for
                example, the client was closed); per-key failures land in
                result slots instead.

        Example::

            users = DataSet.of("test", "users")
            records = await session.get_many([users.id(i) for i in range(16)])
            found = [r for r in records if not isinstance(r, AerospikeError)]

        See Also:
            :meth:`get`: Single-key point read.
            :meth:`put_many`: Window counterpart for writes.
        """
        try:
            if self._txn is None:
                slots, failures = await self._pnc_client._submit_many_read(
                    keys, bins,
                    policy=self._cached_read_policy,
                    policy_sc=self._cached_read_policy_sc,
                )
            else:
                policy = to_read_policy(
                    self._behavior.get_settings(OpKind.READ, OpShape.POINT))
                policy.txn = self._txn
                slots, failures = await self._pnc_client._submit_many_read(
                    keys, bins, policy=policy)
        except (PncServerError, PncAerospikeError) as e:
            raise _convert_pnc_exception(e) from e
        if failures:
            return _convert_window_slots(slots)
        return slots

    async def put_many(
        self, keys: List[Key], bins: Dict[str, Any],
    ) -> List[Optional[AerospikeError]]:
        """Direct upserts of one payload to a window of keys — one await.

        Write counterpart of :meth:`get_many`: the ``bins`` payload is
        converted once and written to every key in the window as
        independent wire ops with client-side fused submission/completion.

        Args:
            keys: Target keys; results are positional.
            bins: Mapping of bin name to value written to every key.

        Returns:
            A list the same length as ``keys``: ``None`` for each success,
            or the :class:`~aerospike_sdk.exceptions.AerospikeError`
            instance (not raised) for that key. Slots are bare rather than
            :class:`~aerospike_sdk.record_result.RecordResult` for the same
            allocation reason as :meth:`get_many`.

        Raises:
            AerospikeError: When the whole-window submission fails; per-key
                failures land in result slots instead.

        Example::

            users = DataSet.of("test", "users")
            outcomes = await session.put_many(
                [users.id(i) for i in range(16)], {"active": True})
            errors = [e for e in outcomes if e is not None]

        See Also:
            :meth:`put`: Single-key upsert.
            :meth:`get_many`: Window counterpart for reads.
        """
        try:
            if self._txn is None:
                slots, failures = await self._pnc_client._submit_many_write(
                    keys, bins,
                    policy=self._cached_write_policy,
                    policy_sc=self._cached_write_policy_sc,
                )
            else:
                policy = to_write_policy(
                    self._behavior.get_settings(
                        OpKind.WRITE_NON_RETRYABLE, OpShape.POINT))
                policy.txn = self._txn
                slots, failures = await self._pnc_client._submit_many_write(
                    keys, bins, policy=policy)
        except (PncServerError, PncAerospikeError) as e:
            raise _convert_pnc_exception(e) from e
        if failures:
            return _convert_window_slots(slots)
        return slots

    # Delegate all Client operations to maintain same API

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
            await task.wait_till_complete(sleep_time=0.2, timeout=10.0)

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
            *keys: One or more :class:`~aerospike_native.Key` targets in the same
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
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            cached_read_policy_sc=self._cached_read_policy_sc,
            cached_write_policy_sc=self._cached_write_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=self._resolve_namespace_mode,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            sdk_client=self._client,
        )
        qb._set_current_keys_from_varargs(keys)
        return UdfFunctionBuilder(qb)

    # -- Internal helpers -----------------------------------------------------

    def _build_write_segment(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Resolve keys and create a :class:`WriteSegmentBuilder`."""
        if isinstance(arg1, Key):
            all_keys = [arg1, *more_keys]
        elif isinstance(arg1, list) and not more_keys:
            if not arg1:
                raise ValueError("keys list cannot be empty")
            all_keys = list(arg1)
        else:
            raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        first = all_keys[0]
        qb = QueryBuilder(
            client=self._client._client,
            namespace=first.namespace,
            set_name=first.set_name,
            behavior=self._behavior,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            cached_read_policy_sc=self._cached_read_policy_sc,
            cached_write_policy_sc=self._cached_write_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=self._resolve_namespace_mode,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            sdk_client=self._client,
        )
        target: Union[Key, List[Key]] = all_keys[0] if len(all_keys) == 1 else all_keys
        return qb._start_write_segment(op_type, target)

    def _dataset_write_builder(self, op_type: str, dataset: DataSet) -> DataSetWriteBuilder:
        """Dataset-scoped tabular write; rows arrive through ``bins(...)``."""
        qb = QueryBuilder(
            client=self._client._client,
            namespace=dataset.namespace,
            set_name=dataset.set_name,
            behavior=self._behavior,
            cached_read_policy=self._cached_read_policy,
            cached_write_policy=self._cached_write_policy,
            cached_read_policy_sc=self._cached_read_policy_sc,
            cached_write_policy_sc=self._cached_write_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=self._resolve_namespace_mode,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
            sdk_client=self._client,
        )
        return DataSetWriteBuilder(qb, op_type, dataset)

    def _fast_write_segment(self, op_type: str, key: Key) -> WriteSegmentBuilder:
        """Single-key write shortcut: bypass QueryBuilder entirely.

        Bench-hot: every arg stays positional (matching the
        ``_SingleKeyWriteSegmentBase.__init__`` order) so this call never
        materializes a kwargs dict. All eight write verbs route their
        single-key shape through here from the shared session base.
        """
        return _SingleKeyWriteSegment(
            self._client._async_client,
            key,
            op_type,
            self._behavior,
            self._cached_write_policy,
            self._cached_read_policy,
            self._txn,
            self._resolve_namespace_mode,
            self._resolve_namespace_mode_blocking,
            self._cached_write_policy_sc,
            self._cached_read_policy_sc,
            self._client,
        )

    # -- Read entry point -----------------------------------------------------
    # ``query`` itself is inherited from SessionBase (shared arg normalization);
    # only the tree-specific builder construction lives here, behind the hooks
    # the base routes to.

    def _fast_query_builder(self, key: Key) -> QueryBuilder:
        """Single-key query builder: skip ``Client.query()`` and per-op policy rebuilds.

        Bench-hot: the common ``session.query(key)`` shape lands here directly
        from the shared session base, bypassing the general key-resolution path.
        Every arg stays positional (matching the ``_QueryBuilderBase.__init__``
        order) so this call never materializes a kwargs dict — offsetting the
        one added dispatch frame the shared-base ``query`` costs this path.
        """
        builder = QueryBuilder(
            self._client._async_client,
            key.namespace,
            key.set_name,
            self._behavior,
            self._cached_read_policy,
            self._cached_write_policy,
            self._cached_read_policy_sc,
            self._cached_write_policy_sc,
            self._cached_read_operate_policy,
            self._cached_read_operate_policy_sc,
            self._txn,
            self._resolve_namespace_mode,
            self._resolve_namespace_mode_blocking,
            self._client,
        )
        builder._single_key = key
        return builder

    def _build_query_builder(
        self, *, dataset: Optional[DataSet], keys: Optional[List[Key]],
    ) -> QueryBuilder:
        """Dataset / multi-key query builder (non-single-key shapes)."""
        return self._client._query(
            dataset=dataset,
            keys=keys,
            behavior=self._behavior,
            namespace_mode_resolver=self._resolve_namespace_mode,
            namespace_mode_resolver_blocking=self._resolve_namespace_mode_blocking,
        )

    def index(self, dataset: DataSet, /) -> IndexBuilder:
        """Create a secondary index builder for a dataset.

        Args:
            dataset: Namespace and set to index; a set-less
                :class:`~aerospike_sdk.dataset.DataSet` covers the whole
                namespace.

        Returns:
            :class:`~aerospike_sdk.aio.operations.index.IndexBuilder` for
                chaining index definition and creation.

        Raises:
            TypeError: If ``dataset`` is not a :class:`~aerospike_sdk.dataset.DataSet`.

        Example::

            users = DataSet.of("test", "users")
            await session.index(users).on_bin("age").named("age_idx").integer().create()

        See Also:
            :meth:`create_index`: The same in one call.
            :meth:`drop_index`
        """
        return self._client.index(dataset)

    def _txn_session_cls(self) -> "type[TransactionalSession]":
        """Return the async transactional-session class (late import breaks the cycle)."""
        from aerospike_sdk.aio.transactional_session import TransactionalSession  # noqa: PLC0415
        return TransactionalSession

    async def create_index(
        self,
        dataset: DataSet,
        index_name: str,
        bin_name: Optional[str] = None,
        index_type: Optional[IndexType] = None,
        collection_type: Optional[CollectionIndexType] = None,
        ctx: Optional[List[CTX]] = None,
        expression: Optional[Union[str, FilterExpression]] = None,
    ) -> IndexTask:
        """Create a secondary index in one call.

        The short form of the :meth:`index` builder: each argument stands in
        for one chain step, and the same rules apply. A bin index needs
        ``bin_name`` and ``index_type``; an expression index needs
        ``expression`` and ``index_type``; a dataset and a name alone create a
        set index.

        Args:
            dataset: Namespace and set to index.
            index_name: Name the cluster stores the index under.
            bin_name: Bin to index; mutually exclusive with ``expression``.
            index_type: Value type indexed, such as ``IndexType.STRING``.
            collection_type: Collection variant for list or map bins; scalar
                when omitted.
            ctx: CDT context path to a nested element; bin indexes only.
            expression: A ``FilterExpression`` or an AEL string whose result
                is indexed; mutually exclusive with ``bin_name``.

        Returns:
            An :class:`~aerospike_native.IndexTask`; await
            ``wait_till_complete()`` before querying through the index.

        Raises:
            ValueError: If the arguments do not describe one index shape, for
                example a bin with no type, or a bin and an expression together.
            AerospikeError: On server or transport failure.

        Example::

            users = DataSet.of("test", "users")
            task = await session.create_index(users, "users_city_idx", "city", IndexType.STRING)
            await task.wait_till_complete()

            # A dataset and a name alone make a set index.
            await session.create_index(users, "users_set_idx")

        See Also:
            :meth:`index`: The builder this call drives.
            :meth:`drop_index`
        """
        builder = self.index(dataset)._configure(
            index_name, bin_name, index_type, collection_type, ctx, expression,
        )
        return await builder.create()

    async def drop_index(self, dataset: DataSet, index_name: str) -> DropIndexTask:
        """Drop a secondary index by name.

        The short form of ``index(dataset).named(index_name).drop()``.

        Args:
            dataset: Namespace and set the index was created on.
            index_name: Name the index was created with.

        Returns:
            A :class:`~aerospike_native.DropIndexTask`; await
            ``wait_till_complete()`` until every node has removed it.

        Raises:
            AerospikeError: On server or transport failure.

        Example::

            task = await session.drop_index(users, "users_city_idx")
            await task.wait_till_complete()

        See Also:
            :meth:`create_index`
        """
        return await self.index(dataset).named(index_name).drop()

    async def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF package from in-memory bytes on the cluster.

        Args:
            body: Raw module source (for example UTF-8 encoded Lua).
            server_path: Path name stored on the server (often ends ``.lua``).
            language: :class:`~aerospike_native.UDFLang`; default is Lua.
            policy: Optional :class:`~aerospike_native.AdminPolicy`; keyword-only.

        Returns:
            A :class:`~aerospike_native.RegisterTask`; await
            ``wait_till_complete(...)`` until propagation finishes.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On cluster or admin errors (via PNC).

        Example::

            source = b"function echo(rec, v) return v end\\n"
            task = await session.register_udf(source, "echo.lua")
            await task.wait_till_complete()

        See Also:
            :meth:`register_udf_from_file`, :meth:`register_udf_from_resource`,
            :meth:`remove_udf`.
        """
        return await self._client._register_udf(body, server_path, language, policy=policy)

    async def register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF by reading module bytes from a local file.

        Args:
            client_path: Filesystem path to the module on the client machine.
            server_path: Path name stored on the server (often ends ``.lua``).
            language: :class:`~aerospike_native.UDFLang`; default is Lua.
            policy: Optional :class:`~aerospike_native.AdminPolicy`; keyword-only.

        Returns:
            A :class:`~aerospike_native.RegisterTask`; await
            ``wait_till_complete(...)`` until propagation finishes.

        Raises:
            RuntimeError: If the client is not connected.
            OSError: If ``client_path`` cannot be read.
            AerospikeError: On cluster or admin errors (via PNC).

        Example::

            task = await session.register_udf_from_file("udfs/echo.lua", "echo.lua")
            await task.wait_till_complete()

        See Also:
            :meth:`register_udf`: Register from in-memory bytes.
        """
        return await self._client._register_udf_from_file(
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

        Reads the resource bytes via ``importlib.resources`` and registers them —
        the Pythonic analog of registering a module shipped as package data.

        Args:
            package: Importable package holding the resource (e.g. ``"myapp.udfs"``).
            resource: Resource name within the package (e.g. ``"echo.lua"``).
            server_path: Path name stored on the server.
            language: :class:`~aerospike_native.UDFLang`; default is Lua.
            policy: Optional :class:`~aerospike_native.AdminPolicy`; keyword-only.

        Returns:
            A :class:`~aerospike_native.RegisterTask`; await
            ``wait_till_complete(...)`` until propagation finishes.

        Raises:
            RuntimeError: If the client is not connected.
            ModuleNotFoundError: If ``package`` cannot be imported.
            FileNotFoundError: If ``resource`` is not found in the package.
            AerospikeError: On cluster or admin errors (via PNC).

        Example::

            task = await session.register_udf_from_resource(
                "myapp.udfs", "echo.lua", "echo.lua")
            await task.wait_till_complete()

        See Also:
            :meth:`register_udf_from_file`, :meth:`register_udf`.
        """
        return await self._client._register_udf_from_resource(
            package, resource, server_path, language, policy=policy)

    async def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "UdfRemoveTask":
        """Remove a registered UDF package from the cluster.

        Args:
            server_path: The server path used when the module was registered.
            policy: Optional :class:`~aerospike_native.AdminPolicy`; keyword-only.

        Returns:
            A :class:`~aerospike_native.UdfRemoveTask`; await
            ``wait_till_complete(...)`` until propagation finishes.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On cluster or admin errors (via PNC).

        Example::

            task = await session.remove_udf("echo.lua")
            await task.wait_till_complete()

        See Also:
            :meth:`register_udf`, :meth:`list_udf`.
        """
        return await self._client._remove_udf(server_path, policy=policy)

    async def list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster.

        Returns:
            One dict per module with ``name`` / ``hash`` / ``type`` keys; an
            empty list when nothing is registered.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On cluster or info errors (via PNC).

        Example::

            for module in await session.list_udf():
                print(module["name"], module["type"])

        See Also:
            :meth:`register_udf`, :meth:`remove_udf`.
        """
        return await self._client._list_udf()

    async def list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster.

        Returns:
            One dict per index with ``namespace`` / ``set`` / ``bin`` / ``name``
            keys, plus ``type`` / ``index_type`` / ``context`` when the server
            reports them (``context`` is present for CDT indexes). An empty list
            when no secondary indexes are defined.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On cluster or info errors (via PNC).

        Example::

            for idx in await session.list_indexes():
                print(idx["name"], idx["namespace"], idx["bin"])

        See Also:
            :meth:`index`: Create or drop a secondary index.
        """
        return await self._client._list_indexes()

    @overload
    def info(self) -> InfoCommands: ...

    @overload
    def info(self, command: str) -> Awaitable[Dict[str, str]]: ...

    def info(self, command: Optional[str] = None) -> Union[InfoCommands, Awaitable[Dict[str, str]]]:
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

    async def namespace_sc_status(self, namespace: str) -> NamespaceScStatus:
        """Describe whether a namespace is configured for strong consistency (SC).

        Uses the ``namespace/<name>`` info command. Prefer this over
        :meth:`is_namespace_sc` when you need a human-readable reason for
        why a namespace is treated as non-SC (missing namespace vs AP mode).

        Args:
            namespace: Namespace name to inspect.

        Returns:
            :class:`NamespaceScStatus` with ``is_sc`` and ``detail``.

        Raises:
            RuntimeError: If the client is not connected.
            ValueError: If the info command fails.
        """
        if self._client._client is None:
            raise RuntimeError("Client is not connected")

        try:
            result = await self._pnc_client.info(f"namespace/{namespace}")
        except Exception as e:
            raise ValueError(f"Failed to check namespace '{namespace}': {e}") from e

        return _namespace_sc_status(
            namespace, NamespaceDetail.from_response(result, namespace)
        )

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
            RuntimeError: If the client is not connected.
            ValueError: If the info command fails.

        Example::

                if await session.is_namespace_sc("test"):
                    print("Namespace 'test' is in strong consistency mode")
                else:
                    print("Namespace 'test' is in AP (availability) mode")

        See Also:
            :meth:`namespace_sc_status`: Same check with a ``detail`` message when false.
        """
        return (await self.namespace_sc_status(namespace)).is_sc

    async def do_in_transaction(
        self,
        operation: typing.Callable[["TransactionalSession"], typing.Awaitable[typing.Any]],
        *,
        max_attempts: Optional[int] = None,
        sleep_between_retries: Optional[Union[float, timedelta]] = None,
    ) -> typing.Any:
        """Run an async callable inside a retrying multi-record transaction.

        Creates a :class:`TransactionalSession`, invokes ``operation(tx)``
        inside ``async with``, and retries the whole block when the attempt
        ends in a transient conflict (``MRT_BLOCKED`` or
        ``MRT_VERSION_MISMATCH``) or in a failed commit. On any non-transient
        failure the transaction is aborted and the exception re-raised.

        Args:
            operation: Async callable accepting a :class:`TransactionalSession`
                and performing zero or more operations on it. Its return
                value is returned from :meth:`do_in_transaction`.
            max_attempts: Maximum total attempts (initial + retries). Must
                be ``>= 1``. Omit to use the cluster's
                :class:`~aerospike_sdk.policy.system_settings.TransactionSettings`.
            sleep_between_retries: How long to wait before retrying a
                blocked record or failed commit, as seconds or a
                :class:`datetime.timedelta`; each wait is jittered +/- 50%.
                A version mismatch always retries immediately. Omit to use
                the cluster's transaction settings; pass ``0`` to never wait.

        Returns:
            Whatever ``operation`` returns on the successful attempt.

        Raises:
            ValueError: If ``max_attempts < 1``.
            CommitError: The commit's roll-forward was abandoned (not
                retried: the server will eventually commit those writes).
            AerospikeError: The last-seen transient error after
                ``max_attempts`` exhausted retries, or any non-transient
                error raised by ``operation``.

        Example::

            async def transfer(tx):
                await tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
                await tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
                return "ok"

            result = await session.do_in_transaction(transfer)

        See Also:
            :meth:`transaction`: Manual MRT lifecycle.
            :class:`TransactionalSession`
        """
        attempts, sleep_seconds = resolve_retry_plan(
            self._client._sdk_settings.transactions,
            max_attempts,
            sleep_between_retries,
        )

        # One transaction for the usage counters however many attempts it
        # takes; the per-attempt sessions do not count themselves.
        usage.record_transaction(self._client)
        last_exc: Optional[BaseException] = None
        for attempt in range(attempts):
            try:
                tx = self.transaction()
                tx._count_on_enter = False
                async with tx as tx_session:
                    return await operation(tx_session)
            except AerospikeError as exc:
                last_exc = exc
                if not is_retryable_txn_error(exc):
                    raise
                if attempt + 1 >= attempts:
                    raise
                delay = retry_delay_seconds(exc, sleep_seconds)
                if delay > 0:
                    await asyncio.sleep(delay)
        # Unreachable — last iteration always raises — but keep mypy happy.
        assert last_exc is not None
        raise last_exc

    # The eight public write-verb factories (upsert/insert/update/replace/
    # replace_if_exists/delete/touch/exists) live on `SessionBase`; they route
    # the single-key shape through `_fast_write_segment` and everything else
    # through `_build_write_segment` (both defined above on this leaf).

    async def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set; this cannot be undone.

        .. warning::

            A ``dataset`` with no set truncates the **entire namespace**, every
            set and the null set. ``DataSet.of("prod")`` and
            ``DataSet.of("prod", None)`` both mean that. Name the set unless a
            namespace-wide truncate is what you intend.

        Args:
            dataset: The DataSet to truncate. With no set name, the whole
                namespace is truncated.
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

        client = self._client
        if client._record_on:
            usage.record_call(client, (usage.ADMIN_TRUNCATE,))
        await self._client._client.truncate(dataset.namespace, dataset.set_name, before_nanos)

    def __repr__(self) -> str:
        """String representation of the session."""
        return f"Session(behavior={self._behavior.name!r})"

