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

"""Chainable builders for reads, writes, and chained multi-operation queries."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import (
    Any,
    List,
    Optional,
    Sequence,
)


from aerospike_async import (
    BatchReadOp,
    BatchReadPolicy,
    BatchWritePolicy,
    ExecuteTask,
    Key,
    Operation,
    PartitionFilter,
    QueryPolicy,
    ReadPolicy,
    WritePolicy,
)
from aerospike_async.exceptions import ResultCode

from typing_extensions import deprecated

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.operations_shared import (
    _OP_TYPE_TO_REA,
    _SingleKeyWriteSegmentBase,
    _cmd_cluster,
    _cmd_done,
    _CMD_DEBUG,
    _cmd_enabled,
    _WriteSegmentBuilderBase,
    _WriteVerbs,
)

from aerospike_sdk.policy.policy_mapper import (
    to_batch_read_policy,
    to_query_policy,
    to_read_policy,
    to_write_policy,
)

from aerospike_sdk.error_strategy import (
    ErrorHandler,
    OnError,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_sdk.implicit_txn import (
    run_in_implicit_txn,
    stamp_txn,
)
from aerospike_sdk.exceptions import (
    AerospikeError,
    _convert_pac_exception,
)
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.record_result import RecordResult
from aerospike_sdk.record_stream import RecordStream


# Shared chain layer — re-exported so existing import paths keep resolving.
from aerospike_sdk.query_shared import (  # noqa: F401
    QueryBinBuilder,
    QueryHint,
    WriteBinBuilder,
    _FilterRecord,
    _OperationSpec,
    _QueryBuilderBase,
    _SupportsAddOperation,
    _bit_policy_or_default,
    _resize_flags_or_default,
    _resolve_hll_flags,
)

log = logging.getLogger(SdkLoggers.QUERY)


class QueryBuilder(_QueryBuilderBase, _WriteVerbs["WriteSegmentBuilder"]):
    """Chain reads, writes, UDF calls, filters, and policies before ``execute``.

    Start from :meth:`~aerospike_sdk.aio.session.Session.query` or
    :meth:`~aerospike_sdk.aio.session.Session.query`. Use :meth:`where`
    or :meth:`filter_expression` for server-side predicates, :meth:`bins` or
    :meth:`bin` for projections, and transition methods such as :meth:`upsert`
    for writes. Await :meth:`execute` for a :class:`~aerospike_sdk.record_stream.RecordStream`.

    Example::

        Set-wide read with filter and projection::

            stream = await (
                session.query(users)
                    .where("$.status == 'active'")
                    .bins(["user_id", "name"])
                    .execute()
            )
            async for row in stream:
                if row.is_ok and row.record:
                    print(row.record.bins)

        Point read on a key, then chain an upsert::

            stream = await (
                session.query(users.id("u1"))
                    .bins(["name"])
                    .upsert(users.id("u1"))
                    .put({"last_seen": 123})
                    .execute()
            )

    See Also:
        :class:`WriteSegmentBuilder`: Bin writes after a write verb.
        :class:`QueryBinBuilder`: Per-bin read operations.
    """

    async def _ensure_namespace_mode(self) -> None:
        """Resolve AP vs SC once per builder so behavior scopes match the namespace."""
        if self._namespace_mode is not None:
            return
        if self._namespace_mode_resolver is not None:
            self._namespace_mode = await self._namespace_mode_resolver(self._namespace)
        else:
            self._namespace_mode = Mode.AP
        if self._namespace_mode == Mode.SC:
            self._base_read_policy = self._base_read_policy_sc
            self._base_write_policy = self._base_write_policy_sc

    async def _ensure_batch_namespace_modes(self) -> None:
        """Resolve modes for every namespace the finalized specs touch.

        Call after :meth:`_ensure_namespace_mode` on batch dispatch paths.
        Single-namespace batches (the overwhelmingly common case) exit after
        one scan of the keys; only genuinely multi-namespace batches resolve
        further modes, enabling per-row policy scoping and SC escalation of
        the parent batch policy (see ``_resolved_batch_mode``).
        """
        if self._namespace_mode == Mode.SC:
            self._batch_any_sc = True
        extra = self._collect_extra_batch_namespaces()
        if not extra:
            return
        resolver = self._namespace_mode_resolver
        modes: dict[str, Mode] = {}
        for ns in extra:
            modes[ns] = (await resolver(ns)) if resolver is not None else Mode.AP
        self._batch_namespace_modes = modes
        if not self._batch_any_sc and any(m == Mode.SC for m in modes.values()):
            self._batch_any_sc = True






    






    

















    
    

    # -- Chain-level defaults -------------------------------------------------
    # -- Query stacking -------------------------------------------------------
    # -- Write transitions (QueryBuilder -> WriteSegmentBuilder) ---------------
    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Execute the query and return a :class:`RecordStream`.

        Handles single-key, batch-key, and dataset queries.  When a chain
        contains multiple operations, each operation is executed and results
        are combined into a single stream.

        Args:
            on_error: Controls how per-record errors are surfaced.

                - ``None`` (default): single-key operations raise on error;
                  batch / multi-key operations embed errors in the stream.
                - ``ErrorStrategy.IN_STREAM``: always embed errors in the
                  stream as ``RecordResult`` entries.
                - A callable ``(key, index, exception) -> None``: errors are
                  dispatched to the callback and excluded from the stream.

        Returns:
            A :class:`~aerospike_sdk.record_stream.RecordStream` of
            :class:`~aerospike_sdk.record_result.RecordResult` rows.

        Raises:
            AerospikeError: If the builder mixes dataset query with per-bin read
                operations (unsupported combination).
            AerospikeError: Typed subclasses for timeouts, connection failures, etc.
                when the client raises instead of embedding errors.

        Example::

            Single-key read with default error handling::

                stream = await session.query(key).bins(["x"]).execute()
                row = await stream.first_or_raise()

            Multi-key read, keep errors in the stream::

                stream = await (
                    session.query(k1, k2, k3)
                        .execute(on_error=ErrorStrategy.IN_STREAM)
                )
                rows = await stream.collect()

        See Also:
            :class:`~aerospike_sdk.error_strategy.ErrorStrategy`: ``on_error`` options.
        """
        # Ultra-early bypass: virgin single-key read shape from
        # `session.query(key).execute()`. Skips _finalize_current_spec +
        # _OperationSpec allocation + _execute_single_key_direct spec-unpacking
        # and dispatches directly to PAC get. Any chained
        # method (.bin/.bins/.where/.with_txn/.ensure_generation/.expire_record/
        # write verbs) flips a tracked field that disqualifies this path,
        # so correctness for those flows is preserved.
        if (
            self._single_key is not None
            and not self._specs
            and not self._operations
            and self._op_type is None
            and self._bins is None
            and not self._with_no_bins
            and self._filter_expression is None
            and self._default_filter_expression is None
            and self._where_ael is None
            and self._generation is None
            and self._ttl_seconds is None
            and self._default_ttl_seconds is None
            and self._durable_delete is None
            and self._udf_function is None
            and on_error is None
        ):
            # Hot path: hand AP + SC base policies to PAC, let Rust resolve
            # namespace mode (cached) and pick. Skips the per-op
            # `_ensure_namespace_mode` await on the SDK side entirely when
            # both policies are pre-built (the common no-txn case).
            rp_ap = self._base_read_policy
            rp_sc = self._base_read_policy_sc
            if rp_ap is not None and rp_sc is not None:
                key = self._single_key
                cmd_t0 = perf_counter() if _cmd_enabled(_CMD_DEBUG) else 0.0
                try:
                    record = await self._client.get(
                        key, None,
                        policy=rp_ap,
                        policy_sc=rp_sc,
                        txn=self._txn,
                    )
                except Exception as e:
                    return self._handle_error(key, e, _ErrorDisposition.THROW, None)
                if cmd_t0:
                    _cmd_done(
                        None, key.namespace, key.set_name, 1, cmd_t0,
                        self._client,
                    )
                return RecordStream._from_single(key, record)
            # Fall through when an AP-only policy is cached (e.g. txn nulled
            # them): legacy path with explicit mode resolution.

        self._finalize_current_spec()
        await self._ensure_namespace_mode()
        await self._ensure_batch_namespace_modes()

        if self._specs:
            # Fast path for the common single-spec case: skip the
            # sum/log overhead that only benefits multi-spec debugging.
            if len(self._specs) == 1:
                spec0 = self._specs[0]
                is_single = len(spec0.keys) == 1
                cmd_t0 = perf_counter() if _cmd_enabled(_CMD_DEBUG) else 0.0

                # Ultra-fast path: single-key operations with no spec-level
                # overrides bypass the full _execute_spec → policy-build →
                # RecordStream chain and call the PAC directly.
                # Namespace mode is already resolved via _ensure_namespace_mode();
                # the direct path applies _resolved_namespace_mode() in policy helpers.
                # Durable-delete / record-delete specs are excluded below so SC delete
                # semantics stay on the full _execute_spec path.
                if (
                    is_single
                    and on_error is None
                    and spec0.filter_expression is None
                    and spec0.generation is None
                    and spec0.ttl_seconds is None
                    and spec0.durable_delete is None
                    and spec0.durable_delete_command_default is None
                    and not spec0.contains_record_delete_op
                ):
                    result = await self._execute_single_key_direct(spec0)
                    if result is not None:
                        if cmd_t0:
                            _cmd_done(
                                spec0.op_type, self._namespace,
                                self._set_name, 1, cmd_t0, self._client,
                            )
                        return result

                disp = _resolve_disposition(on_error, is_single)
                handler = on_error if callable(on_error) else None
                stream = await self._execute_spec(spec0, disp, handler)
                if cmd_t0:
                    _cmd_done(
                        spec0.op_type, self._namespace, self._set_name,
                        len(spec0.keys), cmd_t0, self._client,
                    )
                return stream
            if __debug__ and log.isEnabledFor(logging.DEBUG):
                log.debug(
                    "execute: %s.%s specs=%d keys=%d",
                    self._namespace, self._set_name,
                    len(self._specs), sum(len(s.keys) for s in self._specs),
                    extra={"aerospike.cluster": _cmd_cluster(self._client)},
                )
            is_single = False
            disp = _resolve_disposition(on_error, is_single)
            handler = on_error if callable(on_error) else None
            if self._specs_require_sequential_run():
                sub_disp = _resolve_disposition(on_error, is_single_key=False)
                streams: List[RecordStream] = []
                for spec in self._specs:
                    streams.append(await self._execute_spec(spec, sub_disp, handler))
                return RecordStream.chain(streams)
            batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
            all_ops: list = []
            all_keys: List[Key] = []
            for spec in self._specs:
                all_keys.extend(spec.keys)
                all_ops.extend(self._spec_to_batch_ops(spec))
            cmd_t0 = perf_counter() if _cmd_enabled(_CMD_DEBUG) else 0.0
            try:
                if (
                    self._implicit_txn_precheck(all_keys)
                    and any(not isinstance(op, BatchReadOp) for op in all_ops)
                    and await self._sdk_client._supports_mrt()
                ):
                    batch_records = await run_in_implicit_txn(
                        self._client, self._implicit_txn_settings(),
                        lambda txn: self._client.batch(
                            all_ops, batch_policy=stamp_txn(batch_policy, txn)))
                else:
                    batch_records = await self._client.batch(all_ops, batch_policy=batch_policy)
            except Exception as e:
                return self._handle_batch_error(all_keys, e, disp, handler)
            if cmd_t0:
                _cmd_done(
                    "batch", self._namespace, self._set_name,
                    len(all_keys), cmd_t0, self._client,
                )
            return self._filtered_batch_stream(batch_records, disp, handler)

        # Dataset query path (no keys were specified)
        if self._operations:
            raise AerospikeError(
                "Bin-level read operations are not supported on dataset/index "
                "queries (requires Advanced Bin Projection, not yet available)",
                result_code=ResultCode.OP_NOT_APPLICABLE,
            )
        return await self._execute_dataset_query()

    async def stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Execute lazily — results stream back as each node responds.

        The streaming counterpart to :meth:`execute`. Where :meth:`execute`
        awaits every result then returns a materialized stream (writes
        complete on return), this dispatches the key-batch through PAC's
        lazy ``batch_stream`` and yields each ``(index, RecordResult)`` as
        its node responds — the first results are available as soon as the
        first node responds, without waiting for the rest, and peak memory
        stays bounded to the in-flight node responses. Results arrive in
        **completion order**, not input order; use :attr:`RecordResult.index`
        to correlate.

        **No writes-complete-on-return guarantee.** Per-node work dispatches
        lazily; a caller that awaits this but never drains the stream may
        leave writes in-flight. For writes-done-on-return semantics use
        :meth:`execute` (buffered).

        Args:
            on_error: Same semantics as :meth:`execute`.

        Returns:
            A lazy :class:`RecordStream`.
        """
        self._finalize_current_spec()
        await self._ensure_namespace_mode()
        await self._ensure_batch_namespace_modes()

        # Dataset/index queries and scans already stream lazily from the
        # server; the order-sensitive sequential-spec case can't collapse to
        # one batch. Both delegate to execute() (which is lazy for the
        # dataset path and correct-but-buffered for sequential).
        if not self._specs or self._specs_require_sequential_run():
            return await self.execute(on_error)

        handler = on_error if callable(on_error) else None
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        all_ops: list = []
        all_keys: List[Key] = []
        for spec in self._specs:
            all_keys.extend(spec.keys)
            all_ops.extend(self._spec_to_batch_ops(spec))
        try:
            pac_stream = await self._client.batch_stream(all_ops, batch_policy=batch_policy)
        except Exception as e:
            disp = _resolve_disposition(on_error, is_single_key=False)
            return self._handle_batch_error(all_keys, e, disp, handler)
        # The lazy path only honors the callback form of on_error; an
        # ErrorStrategy enum collapses to inline errors (the stream default).
        return RecordStream._from_pac_batch_stream(pac_stream, on_error=handler)

    @deprecated("Renamed to stream(); execute_stream() will be removed at GA.")
    async def execute_stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Deprecated alias for :meth:`stream`.

        :meta private:
        """
        return await self.stream(on_error)

    async def execute_background_task(self) -> ExecuteTask:
        """Run a background write against all records matching this dataset query.

        Returns a server task handle; poll with ``wait_till_complete`` or
        ``query_status``. Requires :meth:`with_write_operations`; only scalar
        ``Operation`` / expression writes are allowed.

        Raises:
            ValueError: If the builder targets keys or has no write operations.
            AerospikeError: If unsupported operation types are present.
        """
        self._finalize_current_spec()
        await self._ensure_namespace_mode()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if not self._operations:
            raise ValueError(
                "At least one write operation is required; use with_write_operations(...).",
            )
        self._reject_unsupported_background_write_ops(self._operations)
        log.debug(
            "background task: %s.%s ops=%d",
            self._namespace, self._set_name, len(self._operations),
        )
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        try:
            return await self._client.query_operate(
                statement, list(self._operations), write_policy=wp)
        except Exception as e:
            raise _convert_pac_exception(e) from e

    async def execute_udf_background_task(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Apply a registered UDF to matching records as a background task.

        Do not use :meth:`with_write_operations` on the same builder.

        Raises:
            ValueError: If the builder targets keys or has write operations set.
        """
        self._finalize_current_spec()
        await self._ensure_namespace_mode()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if self._operations:
            raise ValueError(
                "Do not combine with_write_operations with execute_udf_background_task.",
            )
        log.debug(
            "background UDF: %s.%s %s.%s",
            self._namespace, self._set_name, package_name, function_name,
        )
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        py_args: Optional[List[Any]] = list(args) if args is not None else None
        try:
            return await self._client.query_execute_udf(
                statement, package_name, function_name, py_args, write_policy=wp)
        except Exception as e:
            raise _convert_pac_exception(e) from e



    # -- Private helpers -------------------------------------------------------







    async def _execute_spec(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        """Execute a single :class:`_OperationSpec`."""
        keys = spec.keys
        op_type = spec.op_type
        if __debug__ and log.isEnabledFor(logging.DEBUG):
            log.debug(
                "_execute_spec: op=%s keys=%d ops=%d",
                op_type or "read", len(keys), len(spec.operations),
            )

        if op_type is None:
            has_ops = bool(spec.operations)
            if len(keys) == 1:
                if has_ops:
                    return await self._execute_single_key_operate(spec, disp, handler)
                return await self._execute_single_key_read(spec, disp, handler)
            if has_ops:
                return await self._execute_batch_read_operate(spec, disp, handler)
            return await self._execute_batch_read(spec, disp, handler)

        if op_type == "udf":
            if len(keys) == 1:
                return await self._execute_single_key_udf(spec, disp, handler)
            return await self._execute_batch_udf(spec, disp, handler)

        if op_type == "delete":
            if len(keys) == 1:
                return await self._execute_single_key_delete(spec, disp, handler)
            return await self._execute_batch_delete(spec, disp, handler)

        if op_type == "touch":
            if len(keys) == 1:
                return await self._execute_single_key_touch(spec, disp, handler)
            return await self._execute_batch_touch(spec, disp, handler)

        if op_type == "exists":
            if len(keys) == 1:
                return await self._execute_single_key_exists(spec, disp, handler)
            return await self._execute_batch_exists(spec, disp, handler)

        if len(keys) == 1:
            return await self._execute_single_key_write(spec, disp, handler)
        return await self._execute_batch_write(spec, disp, handler)



    async def _execute_single_key_udf(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        pkg = spec.udf_package
        fn = spec.udf_function
        if pkg is None or fn is None:
            raise ValueError("UDF spec missing package or function name")
        wp = self._make_udf_write_policy(spec)
        try:
            val = await self._client.execute_udf(
                key, pkg, fn, spec.udf_args, policy=wp)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="udf")
        return RecordStream._from_list([
            RecordResult(
                key=key,
                record=None,
                result_code=ResultCode.OK,
                index=0,
                udf_result=val,
            ),
        ])

    async def _execute_batch_udf(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        pkg = spec.udf_package
        fn = spec.udf_function
        if pkg is None or fn is None:
            raise ValueError("UDF spec missing package or function name")
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        if self._spec_modes_mixed(spec):
            # The batch UDF entry takes one policy for every key, and the
            # mixed-batch API has no per-row UDF op — group keys by mode,
            # apply per group, and merge results back into request order.
            groups: dict[Mode, list[tuple[int, Any]]] = {}
            for i, key in enumerate(spec.keys):
                groups.setdefault(
                    self._mode_for_namespace(key.namespace), []).append((i, key))
            merged: list = [None] * len(spec.keys)
            try:
                for mode, pairs in groups.items():
                    recs = await self._client.batch_apply(
                        [k for _, k in pairs], pkg, fn, spec.udf_args,
                        batch_policy=batch_policy,
                        udf_policy=self._make_batch_udf_policy(spec, mode),
                    )
                    for (i, _), rec in zip(pairs, recs):
                        merged[i] = rec
            except Exception as e:
                return self._handle_batch_error(spec.keys, e, disp, handler)
            return self._filtered_batch_stream(merged, disp, handler, op_type="udf")
        udf_policy = self._make_batch_udf_policy(spec)
        try:
            if (
                self._implicit_txn_precheck(spec.keys)
                and await self._sdk_client._supports_mrt()
            ):
                batch_records = await run_in_implicit_txn(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_apply(
                        spec.keys, pkg, fn, spec.udf_args,
                        batch_policy=stamp_txn(batch_policy, txn),
                        udf_policy=udf_policy))
            else:
                batch_records = await self._client.batch_apply(
                    spec.keys,
                    pkg,
                    fn,
                    spec.udf_args,
                    batch_policy=batch_policy,
                    udf_policy=udf_policy,
                )
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(
            batch_records, disp, handler, op_type="udf")













    async def _execute_single_key_direct(
        self, spec: _OperationSpec,
    ) -> Optional[RecordStream]:
        """Ultra-fast path for single-key reads and writes.

        Calls the PAC directly, bypassing _execute_spec, policy
        construction, and full RecordStream wrapping.  Returns
        ``None`` if the operation type is not supported by this path
        (caller falls back to the normal chain).  On PAC exceptions,
        uses the standard error disposition (single-key default = THROW).
        """
        key = spec.keys[0]
        op_type = spec.op_type
        has_ops = bool(spec.operations)

        if op_type is None and not has_ops:
            # Simple read — fast path via PAC's get when the session-cached
            # base ReadPolicy is available. PAC builds the per-call policy
            # in Rust from base + filter / txn.
            if self._base_read_policy is None and self._behavior is not None:
                self._base_read_policy = self._apply_txn(to_read_policy(
                    self._behavior.get_settings(
                        OpKind.READ, OpShape.POINT, self._resolved_namespace_mode())))
            if self._base_read_policy is not None:
                try:
                    record = await self._client.get(
                        key, spec.bins,
                        policy=self._base_read_policy,
                        filter_expression=spec.filter_expression,
                        txn=self._txn,
                    )
                except Exception as e:
                    return self._handle_error(key, e, _ErrorDisposition.THROW, None)
                return RecordStream._from_single(key, record)
            # No base policy — fall back to the legacy build-in-Python path.
            rp = self._apply_txn(ReadPolicy())
            try:
                record = await self._client.get(key, spec.bins, policy=rp)
            except Exception as e:
                return self._handle_error(key, e, _ErrorDisposition.THROW, None)
            return RecordStream._from_single(key, record)

        if has_ops and op_type not in ("delete", "touch", "exists", "udf"):
            # Write via operate — fast path via PAC's operate when the
            # session-cached base WritePolicy is available. PAC builds the
            # per-call policy in Rust from base + REA + overrides.
            if self._base_write_policy is None and self._behavior is not None:
                self._base_write_policy = self._apply_txn(to_write_policy(
                    self._behavior.get_settings(
                        OpKind.WRITE_NON_RETRYABLE, OpShape.POINT,
                        self._resolved_namespace_mode())))
            if self._base_write_policy is not None:
                # A record-delete op inside the operate must honor the mode-resolved
                # durable-delete default (Scope.WRITES_SC sets it True) plus any explicit
                # spec override — a hardcoded False here made non-durable delete_record()
                # FailForbidden on SC. Non-delete operates keep durable_delete=False.
                durable_delete = False
                if spec.contains_record_delete_op and self._behavior is not None:
                    durable_delete = self._effective_point_durable_delete(
                        spec,
                        self._behavior.get_settings(
                            OpKind.WRITE_NON_RETRYABLE, OpShape.POINT,
                            self._resolved_namespace_mode()))
                try:
                    record = await self._client.operate(
                        key, spec.operations,
                        policy=self._base_write_policy,
                        record_exists_action=_OP_TYPE_TO_REA.get(op_type) if op_type else None,
                        durable_delete=durable_delete,
                        txn=self._txn,
                    )
                except Exception as e:
                    return self._handle_error(
                        key, e, _ErrorDisposition.THROW, None,
                        op_type=spec.op_type)
                return RecordStream._from_single(key, record)
            # No base policy — fall back to the legacy build-in-Python path.
            rea = _OP_TYPE_TO_REA.get(op_type) if op_type else None
            wp = self._apply_txn(WritePolicy())
            if rea is not None:
                wp.record_exists_action = rea
            try:
                record = await self._client.operate(key, spec.operations, policy=wp)
            except Exception as e:
                return self._handle_error(
                    key, e, _ErrorDisposition.THROW, None,
                    op_type=spec.op_type)
            return RecordStream._from_single(key, record)

        # Not a simple case — fall back to normal chain.
        return None







    async def _execute_single_key_read(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        read_policy = self._make_read_policy(spec)
        try:
            record = await self._client.get(key, spec.bins, policy=read_policy)
        except Exception as e:
            return self._handle_error(key, e, disp, handler)
        return RecordStream._from_single(key, record)

    async def _execute_single_key_operate(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        policy = self._apply_txn(WritePolicy())
        if spec.filter_expression is not None:
            policy.filter_expression = spec.filter_expression
        try:
            record = await self._client.operate(key, spec.operations, policy=policy)
        except Exception as e:
            return self._handle_error(key, e, disp, handler)
        return RecordStream._from_single(key, record)

    async def _execute_batch_read(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_read_policy = None
        if self._behavior is not None:
            settings = self._behavior.get_settings(
                OpKind.READ, OpShape.BATCH, self._resolved_namespace_mode())
            batch_read_policy = to_batch_read_policy(settings)
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        if spec.filter_expression is not None:
            if batch_read_policy is None:
                batch_read_policy = BatchReadPolicy()
            batch_read_policy.filter_expression = spec.filter_expression
        try:
            batch_records = await self._client.batch_read(
                spec.keys, spec.bins,
                batch_policy=batch_policy, read_policy=batch_read_policy)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler)


    async def _execute_batch_read_operate(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        ops_per_key = [spec.operations] * len(spec.keys)
        bwp = None
        if spec.filter_expression is not None:
            bwp = BatchWritePolicy()
            bwp.filter_expression = spec.filter_expression
        try:
            batch_records = await self._client.batch_operate(
                spec.keys, ops_per_key,
                batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler)

    # -- Write execution helpers ----------------------------------------------






    async def _execute_single_key_write(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            record = await self._client.operate(key, spec.operations, policy=wp)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type=spec.op_type)
        return RecordStream._from_single(key, record)

    async def _execute_single_key_delete(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            existed = await self._client.delete(key, policy=wp)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="delete")
        rc = ResultCode.OK if existed else ResultCode.KEY_NOT_FOUND_ERROR
        if self._should_include_result(rc, self._respond_all_keys, self._fail_on_filtered_out):
            return RecordStream._from_error(key, rc)
        return RecordStream._from_list([])

    async def _execute_spec_mixed_mode_batch(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
        op_type: str | None = None,
    ) -> RecordStream:
        """Dispatch one spec whose keys span AP and SC namespaces.

        The single-policy PAC entries (``batch_operate`` / ``batch_delete``)
        apply one write policy to every key, which cannot express per-row
        durable-delete defaults; route through the mixed-batch API instead,
        which carries a policy per row (``_spec_to_batch_ops`` resolves it
        per key's mode).
        """
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        all_ops = self._spec_to_batch_ops(spec)
        try:
            batch_records = await self._client.batch(all_ops, batch_policy=batch_policy)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler, op_type=op_type)

    async def _execute_batch_write(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        if spec.contains_record_delete_op and self._spec_modes_mixed(spec):
            return await self._execute_spec_mixed_mode_batch(
                spec, disp, handler, op_type=spec.op_type)
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bwp = self._make_batch_write_policy(spec)
        ops_per_key = [spec.operations] * len(spec.keys)
        try:
            if (
                self._implicit_txn_precheck(spec.keys)
                and await self._sdk_client._supports_mrt()
            ):
                batch_records = await run_in_implicit_txn(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_operate(
                        spec.keys, ops_per_key,
                        batch_policy=stamp_txn(batch_policy, txn),
                        write_policy=bwp))
            else:
                batch_records = await self._client.batch_operate(
                    spec.keys, ops_per_key,
                    batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler, op_type=spec.op_type)

    async def _execute_batch_delete(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        if self._spec_modes_mixed(spec):
            return await self._execute_spec_mixed_mode_batch(
                spec, disp, handler, op_type="delete")
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bdp = self._make_batch_delete_policy(spec)
        try:
            if (
                self._implicit_txn_precheck(spec.keys)
                and await self._sdk_client._supports_mrt()
            ):
                batch_records = await run_in_implicit_txn(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_delete(
                        spec.keys,
                        batch_policy=stamp_txn(batch_policy, txn),
                        delete_policy=bdp))
            else:
                batch_records = await self._client.batch_delete(
                    spec.keys, batch_policy=batch_policy, delete_policy=bdp)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler, op_type="delete")

    async def _execute_single_key_touch(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        wp = self._make_write_policy(spec)
        try:
            await self._client.touch(key, policy=wp)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="touch")
        if self._should_include_result(
            ResultCode.OK, self._respond_all_keys, self._fail_on_filtered_out
        ):
            return RecordStream._from_error(key, ResultCode.OK)
        return RecordStream._from_list([])

    async def _execute_batch_touch(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bwp = self._make_batch_write_policy(spec)
        touch_ops = [Operation.touch()]
        ops_per_key = [touch_ops] * len(spec.keys)
        try:
            if (
                self._implicit_txn_precheck(spec.keys)
                and await self._sdk_client._supports_mrt()
            ):
                batch_records = await run_in_implicit_txn(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_operate(
                        spec.keys, ops_per_key,
                        batch_policy=stamp_txn(batch_policy, txn),
                        write_policy=bwp))
            else:
                batch_records = await self._client.batch_operate(
                    spec.keys, ops_per_key,
                    batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        return self._filtered_batch_stream(batch_records, disp, handler, op_type="touch")

    async def _execute_single_key_exists(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        key = spec.keys[0]
        if self._read_policy is not None:
            rp = self._read_policy
        elif self._behavior is not None:
            rp = self._apply_txn(to_read_policy(
                self._behavior.get_settings(
                    OpKind.READ, OpShape.POINT, self._resolved_namespace_mode())))
        else:
            rp = self._apply_txn(ReadPolicy())
        if spec.filter_expression is not None:
            rp.filter_expression = spec.filter_expression
        try:
            found = await self._client.exists(key, policy=rp)
        except Exception as e:
            return self._handle_error(key, e, disp, handler, op_type="exists")
        rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
        if self._should_include_result(rc, self._respond_all_keys, self._fail_on_filtered_out):
            return RecordStream._from_error(key, rc)
        return RecordStream._from_list([])

    async def _execute_batch_exists(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> RecordStream:
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        brp = self._make_batch_read_policy(spec)
        try:
            found_list = await self._client.batch_exists(
                spec.keys, batch_policy=batch_policy, read_policy=brp)
        except Exception as e:
            return self._handle_batch_error(spec.keys, e, disp, handler)
        results = []
        for key, found in zip(spec.keys, found_list):
            rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
            if self._should_include_result(rc, self._respond_all_keys, self._fail_on_filtered_out):
                results.append(RecordResult(key, None, rc))
        return RecordStream._from_list(results)

    # -- Mixed-batch execution (multi-spec chains) ----------------------------
    async def _execute_dataset_query(self) -> RecordStream:
        log.debug(
            "dataset query: %s.%s filter=%s chunk=%s hint=%s",
            self._namespace, self._set_name,
            self._filter_expression is not None
            or self._where_ael is not None
            or bool(self._filter_records),
            self._chunk_size,
            self._query_hint is not None,
            extra={"aerospike.cluster": _cmd_cluster(self._client)},
        )
        if self._policy is not None:
            policy = self._policy
        elif self._behavior is not None:
            policy = self._apply_txn(to_query_policy(
                self._behavior.get_settings(
                    OpKind.READ, OpShape.QUERY, self._resolved_namespace_mode())))
        else:
            policy = self._apply_txn(QueryPolicy())
        chunk_total_limit = 0
        if self._chunk_size is not None and self._chunk_size > 0:
            # limit()/max_records() land on policy.max_records. Capture it as the
            # overall cap before chunk_size overwrites the field with the per-chunk
            # fetch size, then hand it to the stream's _chunk_limit below so the
            # total is enforced across chunks.
            chunk_total_limit = policy.max_records or 0
            policy.max_records = self._chunk_size
        hint = self._query_hint
        use_server_query_selection = self._use_server_query_selection()
        self._apply_dataset_query_policy_filter(
            policy, use_server_query_selection=use_server_query_selection,
        )

        if hint is not None and hint.query_duration is not None:
            policy.expected_duration = hint.query_duration

        partition_filter = self._partition_filter or PartitionFilter.all()

        statement = self._build_statement()

        try:
            recordset, plan = await self._run_dataset_query_async(
                policy, partition_filter, hint, statement,
                use_server_query_selection=use_server_query_selection,
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e

        if self._chunk_size is not None and self._chunk_size > 0:
            client = self._client

            if plan is not None:
                async def _reexecute(pf: PartitionFilter) -> Any:
                    return await client.query_with_plan(
                        statement, pf, plan, policy=policy,
                    )
            else:
                async def _reexecute(pf: PartitionFilter) -> Any:
                    return await client.query(statement, pf, policy=policy)

            return RecordStream._from_chunked_pac_recordset(
                recordset,
                reexecute=_reexecute,
                limit=chunk_total_limit,
            )

        return RecordStream._from_pac_recordset(recordset)


class WriteSegmentBuilder(_WriteSegmentBuilderBase["QueryBuilder"], _WriteVerbs["WriteSegmentBuilder"]):
    """Accumulate scalar and CDT writes for the current operation's key(s).

    Obtained from :class:`QueryBuilder` after a write verb or from
    :class:`WriteBinBuilder` when chaining. Call :meth:`put`, :meth:`bin`,
    expression helpers, optional :meth:`where` / TTL / generation guards, then
    :meth:`execute` on this object or transition with :meth:`query` /
    another write verb on the mixin.

    Example::

        Upsert two bins, then read the stream of results::

            stream = await (
                session.upsert(key)
                    .put({"name": "Ada", "score": 100})
                    .execute()
            )

    See Also:
        :meth:`QueryBuilder.execute`: Runs all chained operations.
    """

    __slots__ = ("_qb",)
    # -- Execution ------------------------------------------------------------

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Run the parent :class:`QueryBuilder` stack (same as ``self._qb.execute``).

        Args:
            on_error: Optional :class:`~aerospike_sdk.error_strategy.ErrorStrategy`
                or error callback; see :meth:`QueryBuilder.execute`.

        Returns:
            :class:`~aerospike_sdk.record_stream.RecordStream` of results.

        Example::
            stream = await session.upsert(key).put({"x": 1}).execute()
            await stream.first_or_raise()

        Raises:
            Same as :meth:`QueryBuilder.execute`.
        """
        return await self._qb.execute(on_error)

    async def stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Lazy streaming variant — see :meth:`QueryBuilder.stream`."""
        return await self._qb.stream(on_error)

    @deprecated("Renamed to stream(); execute_stream() will be removed at GA.")
    async def execute_stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Deprecated alias for :meth:`stream`.

        :meta private:
        """
        return await self.stream(on_error)


class _SingleKeyWriteSegment(_SingleKeyWriteSegmentBase, WriteSegmentBuilder):
    """Lightweight single-key write path that bypasses QueryBuilder overhead.

    On the hot path (put + execute), calls the PAC directly without
    ``_finalize_current_spec``, ``_OperationSpec``, or ``execute()`` dispatch.
    Advanced features (``where``, TTL, generation, chaining) trigger in-place
    promotion: ``self._qb`` is populated so all inherited
    ``WriteSegmentBuilder`` methods work naturally.
    """

    __slots__ = (
        "_client_fast", "_key", "_op_type_fast", "_ops",
        "_write_policy", "_behavior_fast", "_read_policy",
        "_write_policy_sc", "_read_policy_sc",
        "_txn", "_namespace_mode_resolver", "_namespace_mode_resolver_blocking",
        # _sdk_client_fast must stay slotted: it is stored on every
        # `session.upsert(...)` fast ctor, and an unslotted store would
        # materialize the (otherwise untouched) instance __dict__ per op.
        "_sdk_client_fast",
        # _dd_command_default, _dd_override, _record_delete_in_fast_ops:
        # class-level defaults on _SingleKeyWriteSegmentBase (skip them
        # here so reads fall through to class default and writes go to
        # __dict__ inherited from the non-slotted base).
    )




    # -- Operation methods ---------------------------------------------------
    # On the fast path (_qb is None) these use self._ops directly.
    # After promotion (_qb is set) they delegate to the QB's list.










    # -- In-place promotion --------------------------------------------------

    def _promote(self) -> None:
        """Populate ``self._qb`` so inherited WriteSegmentBuilder methods work."""
        if self._qb is not None:
            return
        qb = QueryBuilder(
            client=self._client_fast,
            sdk_client=self._sdk_client_fast,
            namespace=self._key.namespace,
            set_name=self._key.set_name,
            behavior=self._behavior_fast,
            cached_write_policy=self._write_policy,
            cached_read_policy=self._read_policy,
            cached_write_policy_sc=self._write_policy_sc,
            cached_read_policy_sc=self._read_policy_sc,
            txn=self._txn,
            namespace_mode_resolver=self._namespace_mode_resolver,
            namespace_mode_resolver_blocking=self._namespace_mode_resolver_blocking,
        )
        qb._op_type = self._op_type_fast
        qb._single_key = self._key
        qb._operations = self._ops
        qb._durable_delete_command_default = self._dd_command_default
        qb._durable_delete = self._dd_override
        qb._record_delete_in_operations = self._record_delete_in_fast_ops
        self._qb = qb











    # -- Error handling ------------------------------------------------------


    # -- Policy helpers ------------------------------------------------------


    # -- Execution -----------------------------------------------------------


    async def stream(  # type: ignore[override]
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Lazy streaming variant — see :meth:`QueryBuilder.stream`."""
        if self._qb is not None:
            return await self._qb.stream(on_error)
        # Still a single-key segment: one record, so buffered == lazy.
        return await self.execute(on_error)

    async def execute(  # type: ignore[override]
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        if self._qb is not None:
            return await self._qb.execute(on_error)
        if on_error is not None:
            self._promote()
            return await self._qb.execute(on_error)  # type: ignore[union-attr]
        # Durable-delete state requires the slow path: bypasses don't carry
        # _dd_override / _dd_command_default through to the per-call policy,
        # and SC namespaces require the right durable_delete flag (FailForbidden
        # otherwise). A record-delete op counts as durable-delete state too —
        # the fast operate below hardcodes durable_delete=False, which would
        # stomp the SC policy's default. Promote and defer to
        # QueryBuilder.execute().
        if (
            self._dd_override is not None
            or self._dd_command_default is not None
            or self._record_delete_in_fast_ops
        ):
            self._promote()
            return await self._qb.execute(on_error)  # type: ignore[union-attr]

        key = self._key
        op_type = self._op_type_fast
        cmd_t0 = perf_counter() if _cmd_enabled(_CMD_DEBUG) else 0.0

        # Hot path: when both AP + SC base policies are pre-built (the
        # common no-txn case), hand them to PAC and let Rust resolve
        # namespace mode (cached, lazy on first miss) and pick.
        # Eliminates the per-op `_namespace_mode_resolver` await + dict
        # lookup that would otherwise fire here. Delete/touch/exists keep
        # their own PAC entries below.
        if (
            op_type not in ("delete", "touch", "exists")
            and self._write_policy is not None
            and self._write_policy_sc is not None
        ):
            try:
                record = await self._client_fast.operate(
                    key,
                    self._ops,
                    policy=self._write_policy,
                    policy_sc=self._write_policy_sc,
                    record_exists_action=_OP_TYPE_TO_REA.get(op_type) if op_type else None,
                    durable_delete=False,
                    txn=self._txn,
                )
            except Exception as exc:
                return self._handle_fast_error(exc, op_type or "upsert")
            if cmd_t0:
                _cmd_done(
                    op_type or "upsert", key.namespace, key.set_name, 1, cmd_t0,
                    self._client_fast)
            return RecordStream._from_single(key, record)

        # Fallback (delete/touch/exists + txn-bound cells): resolve mode
        # explicitly, then dispatch to the right primitive.
        mode = Mode.AP
        if self._namespace_mode_resolver is not None:
            mode = await self._namespace_mode_resolver(key.namespace)
        if mode == Mode.SC:
            cached_wp = self._write_policy_sc
            cached_rp = self._read_policy_sc
        else:
            cached_wp = self._write_policy
            cached_rp = self._read_policy

        # -- delete (PAC returns bool, no record) --
        if op_type == "delete":
            wp = cached_wp if cached_wp is not None else self._get_write_policy()
            wp = self._apply_txn(wp)
            try:
                existed = await self._client_fast.delete(key, policy=wp)
            except Exception as exc:
                return self._handle_fast_error(exc, "delete")
            if cmd_t0:
                _cmd_done("delete", key.namespace, key.set_name, 1, cmd_t0, self._client_fast)
            if existed:
                return RecordStream._from_error(key, ResultCode.OK)
            return RecordStream._from_list([])

        # -- touch (no record returned) --
        if op_type == "touch":
            wp = cached_wp if cached_wp is not None else self._get_write_policy()
            wp = self._apply_txn(wp)
            try:
                await self._client_fast.touch(key, policy=wp)
            except Exception as exc:
                return self._handle_fast_error(exc, "touch")
            if cmd_t0:
                _cmd_done("touch", key.namespace, key.set_name, 1, cmd_t0, self._client_fast)
            return RecordStream._from_error(key, ResultCode.OK)

        # -- exists (uses ReadPolicy, returns bool) --
        if op_type == "exists":
            rp = cached_rp
            if rp is None and self._behavior_fast is not None:
                rp = self._apply_txn(to_read_policy(
                    self._behavior_fast.get_settings(
                        OpKind.READ, OpShape.POINT, mode)))
            if rp is None:
                rp = ReadPolicy()
            rp = self._apply_txn(rp)
            try:
                found = await self._client_fast.exists(key, policy=rp)
            except Exception as exc:
                return self._handle_fast_error(exc, "exists")
            if cmd_t0:
                _cmd_done("exists", key.namespace, key.set_name, 1, cmd_t0, self._client_fast)
            if found:
                return RecordStream._from_error(key, ResultCode.OK)
            return RecordStream._from_list([])

        # -- operate-based fallback when only one cached policy is set
        # (e.g. txn-bound segment nulled both policies). cached_wp here
        # is from the AP-or-SC pick above; use it if available, else
        # build from behavior.
        if cached_wp is not None:
            try:
                record = await self._client_fast.operate(
                    key,
                    self._ops,
                    policy=cached_wp,
                    record_exists_action=_OP_TYPE_TO_REA.get(op_type) if op_type else None,
                    durable_delete=False,
                    txn=self._txn,
                )
            except Exception as exc:
                return self._handle_fast_error(exc, op_type or "upsert")
            if cmd_t0:
                _cmd_done(
                    op_type or "upsert", key.namespace, key.set_name, 1, cmd_t0,
                    self._client_fast)
            return RecordStream._from_single(key, record)

        # Fall back to the legacy build-policy-in-Python path.
        rea = _OP_TYPE_TO_REA.get(op_type) if op_type else None
        if rea is not None:
            if self._behavior_fast is not None:
                wp = to_write_policy(
                    self._behavior_fast.get_settings(
                        OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, mode))
            else:
                wp = WritePolicy()
            wp.record_exists_action = rea
        else:
            wp = self._get_write_policy()
        wp = self._apply_txn(wp)

        try:
            record = await self._client_fast.operate(key, self._ops, policy=wp)
        except Exception as exc:
            return self._handle_fast_error(exc, op_type or "upsert")
        if cmd_t0:
            _cmd_done(op_type or "upsert", key.namespace, key.set_name, 1, cmd_t0,
                      self._client_fast)
        return RecordStream._from_single(key, record)

# Bind the async write-segment class onto the shared base's factory hook so
# `_start_write_segment` on an async QueryBuilder chains into the async
# segment type. The sync leaf overrides `_start_write_verb` and never uses
# this binding.
QueryBuilder._write_segment_cls = WriteSegmentBuilder
