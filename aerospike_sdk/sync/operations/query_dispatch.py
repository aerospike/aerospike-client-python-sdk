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

"""Blocking dispatch mixin for the synchronous query builder.

The methods here are the sync-runtime terminals' engine: Tier 1 fast path,
Tier 1b multi-spec, Tier 2 streaming, batch/background helpers — all against
PAC ``*_blocking`` entries. Mixed into
:class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder` alongside the
runtime-agnostic :class:`~aerospike_sdk.query_shared._QueryBuilderBase`;
kept out of the shared base so the base stays runtime-agnostic.
"""

from __future__ import annotations

import logging
from typing import (
    Any,
    List,
    Optional,
    Sequence,
)


from aerospike_async import (
    BatchReadOp,
    BatchWritePolicy,
    ExecuteTask,
    Key,
    Operation,
    PartitionFilter,
    QueryPolicy,
)
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.operations_shared import (
    _OP_TYPE_TO_REA,
    _cmd_cluster,
    _cmd_failed,
    _to_expiration,
)

from aerospike_sdk.query_shared import _OperationSpec  # noqa: E402

log = logging.getLogger(SdkLoggers.QUERY)

from aerospike_sdk.policy.policy_mapper import (
    to_batch_read_policy,
    to_query_policy,
)

from aerospike_sdk.error_strategy import (
    ErrorHandler,
    OnError,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_sdk.implicit_txn import (
    run_in_implicit_txn_blocking,
    stamp_txn,
)
from aerospike_sdk.exceptions import (
    _convert_pac_exception,
    _result_code_to_exception,
)
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.record_result import RecordResult


class _BlockingQueryDispatch:
    """Sync blocking dispatchers; see module docstring."""

    def _implicit_txn_gate_blocking(self) -> bool:
        """Full gate for blocking dispatchers (precheck + MRT capability)."""
        return self._implicit_txn_precheck() and self._sdk_client._supports_mrt_blocking()

    def _ensure_namespace_mode_blocking(self) -> None:
        """Sync counterpart of :meth:`_ensure_namespace_mode`.

        Resolves AP vs SC via the sync resolver (info_blocking-backed)
        when present; falls back to AP otherwise.
        """
        if self._namespace_mode is not None:
            return
        if self._namespace_mode_resolver_blocking is not None:
            self._namespace_mode = self._namespace_mode_resolver_blocking(self._namespace)
        else:
            self._namespace_mode = Mode.AP
        if self._namespace_mode == Mode.SC:
            self._base_read_policy = self._base_read_policy_sc
            self._base_write_policy = self._base_write_policy_sc

    def _execute_background_task_blocking(self) -> ExecuteTask:
        """Sync counterpart of :meth:`execute_background_task`.

        Uses PAC ``query_operate_blocking`` — zero asyncio.
        """
        self._finalize_current_spec()
        self._ensure_namespace_mode_blocking()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if not self._operations:
            raise ValueError(
                "At least one write operation is required; use with_write_operations(...).",
            )
        self._reject_unsupported_background_write_ops(self._operations)
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        try:
            return self._client.query_operate_blocking(
                statement, list(self._operations), write_policy=wp)
        except Exception as e:
            raise _convert_pac_exception(e) from e

    def _execute_udf_background_task_blocking(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Sync counterpart of :meth:`execute_udf_background_task`.

        Uses PAC ``query_execute_udf_blocking`` — zero asyncio.
        """
        self._finalize_current_spec()
        self._ensure_namespace_mode_blocking()
        if self._specs:
            raise ValueError(
                "Background task execution applies only to dataset queries.",
            )
        if self._operations:
            raise ValueError(
                "Do not combine with_write_operations with execute_udf_background_task.",
            )
        wp = self._make_background_write_policy()
        statement = self._build_statement()
        py_args: Optional[List[Any]] = list(args) if args is not None else None
        try:
            return self._client.query_execute_udf_blocking(
                statement, package_name, function_name, py_args, write_policy=wp)
        except Exception as e:
            raise _convert_pac_exception(e) from e

    def _execute_batch_udf_blocking(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_udf` — uses
        ``batch_apply_blocking``."""
        pkg = spec.udf_package
        fn = spec.udf_function
        if pkg is None or fn is None:
            raise ValueError("UDF spec missing package or function name")
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        udf_policy = self._make_batch_udf_policy(spec)
        try:
            if self._implicit_txn_gate_blocking():
                batch_records = run_in_implicit_txn_blocking(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_apply_blocking(
                        spec.keys, pkg, fn, spec.udf_args,
                        batch_policy=stamp_txn(batch_policy, txn),
                        udf_policy=udf_policy))
            else:
                batch_records = self._client.batch_apply_blocking(
                    spec.keys,
                    pkg,
                    fn,
                    spec.udf_args,
                    batch_policy=batch_policy,
                    udf_policy=udf_policy,
                )
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler, op_type="udf")

    def _execute_batch_touch_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_touch` — uses
        ``batch_operate_blocking`` with a single touch op per key."""
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bwp = self._make_batch_write_policy(spec)
        touch_ops = [Operation.touch()]
        ops_per_key = [touch_ops] * len(spec.keys)
        try:
            if self._implicit_txn_gate_blocking():
                batch_records = run_in_implicit_txn_blocking(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_operate_blocking(
                        spec.keys, ops_per_key,
                        batch_policy=stamp_txn(batch_policy, txn),
                        write_policy=bwp))
            else:
                batch_records = self._client.batch_operate_blocking(
                    spec.keys, ops_per_key,
                    batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler, op_type="touch")

    def _execute_batch_exists_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_exists` — uses
        ``batch_exists_blocking`` (returns ``list[bool]``, one per key)."""
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        brp = self._make_batch_read_policy(spec)
        try:
            found_list = self._client.batch_exists_blocking(
                spec.keys, batch_policy=batch_policy, read_policy=brp)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        results: list[RecordResult] = []
        for i, (key, found) in enumerate(zip(spec.keys, found_list)):
            rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
            if not found and self._is_actionable(rc, "exists") and disp is _ErrorDisposition.THROW:
                raise _result_code_to_exception(rc, str(rc), False)
            if not self._should_include_result(
                rc, self._respond_all_keys, self._fail_on_filtered_out,
            ):
                continue
            results.append(RecordResult(
                key=key, record=None, result_code=rc, index=i,
            ))
        return results

    def _execute_single_key_direct_blocking(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition = _ErrorDisposition.THROW,
        handler: ErrorHandler | None = None,
    ) -> Optional[List[RecordResult]]:
        """Sync counterpart of :meth:`_execute_single_key_direct`.

        Builds the policy via the same :meth:`_make_read_policy` /
        :meth:`_make_write_policy` / :meth:`_make_udf_write_policy`
        helpers as the async path (so filter expressions, generation/TTL/
        durable-delete overrides, and record-delete ops are honored),
        then dispatches via the matching PAC ``*_blocking`` entry — zero
        asyncio. Handles plain reads, ``operate``-style writes, ``delete``,
        ``touch``, ``exists`` and ``udf`` op types. Errors are routed via
        the supplied ``disp`` / ``handler``.

        Returns:
            A list of zero or one :class:`RecordResult`. Caller wraps with
            ``RecordStream.from_list``. ``None`` when the spec shape
            isn't handled (caller falls back to the async path).
        """
        key = spec.keys[0]
        op_type = spec.op_type
        has_ops = bool(spec.operations)

        if op_type == "delete":
            wp = self._make_write_policy(spec)
            try:
                existed = self._client.delete_blocking(key, policy=wp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(key, e, "delete", disp, handler)
            rc = ResultCode.OK if existed else ResultCode.KEY_NOT_FOUND_ERROR
            if self._should_include_result(
                rc, self._respond_all_keys, self._fail_on_filtered_out,
            ):
                return [RecordResult(
                    key=key, record=None, result_code=rc, index=0,
                )]
            return []

        if op_type == "touch":
            wp = self._make_write_policy(spec)
            try:
                self._client.touch_blocking(key, policy=wp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(key, e, "touch", disp, handler)
            if self._should_include_result(
                ResultCode.OK, self._respond_all_keys, self._fail_on_filtered_out,
            ):
                return [RecordResult(
                    key=key, record=None, result_code=ResultCode.OK, index=0,
                )]
            return []

        if op_type == "exists":
            rp = self._make_read_policy(spec)
            try:
                found = self._client.exists_blocking(key, policy=rp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(key, e, "exists", disp, handler)
            rc = ResultCode.OK if found else ResultCode.KEY_NOT_FOUND_ERROR
            if self._should_include_result(
                rc, self._respond_all_keys, self._fail_on_filtered_out,
            ):
                return [RecordResult(
                    key=key, record=None, result_code=rc, index=0,
                )]
            return []

        if op_type == "udf":
            pkg = spec.udf_package
            fn = spec.udf_function
            if pkg is None or fn is None:
                raise ValueError("UDF spec missing package or function name")
            wp = self._make_udf_write_policy(spec)
            try:
                val = self._client.execute_udf_blocking(
                    key, pkg, fn, spec.udf_args, policy=wp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(
                    key, e, "udf", disp, handler)
            return [RecordResult(
                key=key, record=None, result_code=ResultCode.OK,
                index=0, udf_result=val,
            )]

        if op_type is None and not has_ops:
            # Simple read — all bins or projected bins. Fast path: PAC's
            # get_blocking builds the per-call ReadPolicy in Rust from the
            # session-cached base + filter_expression / txn. Falls back to
            # legacy `_make_read_policy` only when no Behavior is bound or
            # a user-supplied read_policy is in play.
            if self._base_read_policy is not None and self._read_policy is None:
                try:
                    record = self._client.get_blocking(
                        key,
                        spec.bins,
                        policy=self._base_read_policy,
                        filter_expression=spec.filter_expression,
                        txn=self._txn,
                    )
                except Exception as e:
                    return self._handle_error_blocking_singlekey(
                        key, e, None, disp, handler)
                return [RecordResult(
                    key=key, record=record, result_code=ResultCode.OK,
                )]
            # Slow path: no base, or user-supplied read_policy.
            rp = self._make_read_policy(spec)
            try:
                record = self._client.get_blocking(key, spec.bins, policy=rp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(
                    key, e, None, disp, handler)
            return [RecordResult(
                key=key, record=record, result_code=ResultCode.OK,
            )]

        if has_ops:
            # Write via operate. Fast path: PAC's operate_blocking builds the
            # per-call WritePolicy in Rust from the session-cached base + the
            # small set of fields the SDK actually varies. Skips
            # _make_write_policy's Python work on the hot path.
            #
            # Fall back to the legacy path when:
            #  - no session-cached base policy (no Behavior bound), or
            #  - record-delete op present (applies_dd True; needs the spec's
            #    effective_dd resolution inside _make_write_policy).
            if self._base_write_policy is not None and not spec.contains_record_delete_op:
                rea = _OP_TYPE_TO_REA.get(op_type)
                exp = _to_expiration(spec.ttl_seconds) if spec.ttl_seconds is not None else None
                try:
                    record = self._client.operate_blocking(
                        key,
                        spec.operations,
                        policy=self._base_write_policy,
                        record_exists_action=rea,
                        expiration=exp,
                        generation=spec.generation,
                        durable_delete=False,
                        filter_expression=spec.filter_expression,
                        txn=self._txn,
                    )
                except Exception as e:
                    return self._handle_error_blocking_singlekey(
                        key, e, spec.op_type, disp, handler)
                return [RecordResult(
                    key=key, record=record, result_code=ResultCode.OK,
                )]
            # Slow path: complex durable-delete or no Behavior — fall back.
            wp = self._make_write_policy(spec)
            try:
                record = self._client.operate_blocking(key, spec.operations, policy=wp)
            except Exception as e:
                return self._handle_error_blocking_singlekey(
                    key, e, spec.op_type, disp, handler)
            return [RecordResult(
                key=key, record=record, result_code=ResultCode.OK,
            )]

        # Not eligible — caller falls back.
        return None

    def _execute_blocking_fast_path(
        self,
        on_error: Optional[OnError] = None,
    ) -> Optional[List[RecordResult]]:
        """Try a blocking dispatch path; fall back to async when ineligible.

        Handled shapes:

        - **Single key, single spec** — routes through
          :meth:`_execute_single_key_direct_blocking` (read, write, delete,
          touch, exists, and UDF shapes).
        - **Multi-key, single spec** — routes through the
          ``_execute_batch_*_blocking`` family for plain reads, writes,
          deletes, touch, exists, read-operate, and UDF.

        Returns:
            A list of :class:`RecordResult` on a hit. ``None`` when the
            shape is not eligible for this fast path (for example: no specs,
            more than one spec, dataset / SI queries, scans, or background
            UDF), so the caller falls back to the async execution path.
        """
        self._finalize_current_spec()
        self._ensure_namespace_mode_blocking()

        if not self._specs or len(self._specs) != 1:
            return None
        spec0 = self._specs[0]

        if len(spec0.keys) == 1:
            disp = _resolve_disposition(on_error, is_single_key=True)
            handler = on_error if callable(on_error) else None
            return self._execute_single_key_direct_blocking(spec0, disp, handler)

        if len(spec0.keys) > 1:
            disp = _resolve_disposition(on_error, is_single_key=False)
            handler = on_error if callable(on_error) else None

            # Multi-key plain read → batch_read_blocking
            if not spec0.operations and spec0.op_type is None:
                return self._execute_batch_read_blocking(spec0, disp, handler)

            # Multi-key delete → batch_delete_blocking
            if spec0.op_type == "delete":
                return self._execute_batch_delete_blocking(spec0, disp, handler)

            # Multi-key write (upsert / insert / update / replace) → batch_operate_blocking
            if spec0.op_type in (
                "upsert", "insert", "update",
                "replace", "replace_if_exists",
            ):
                return self._execute_batch_write_blocking(spec0, disp, handler)

            # Multi-key touch → batch_operate_blocking with touch op
            if spec0.op_type == "touch":
                return self._execute_batch_touch_blocking(spec0, disp, handler)

            # Multi-key exists → batch_exists_blocking
            if spec0.op_type == "exists":
                return self._execute_batch_exists_blocking(spec0, disp, handler)

            # Multi-key read-operate (bin projection, read-style ops on
            # batch) → batch_operate_blocking with a read-shaped policy
            if spec0.operations and spec0.op_type is None:
                return self._execute_batch_read_operate_blocking(spec0, disp, handler)

            # Multi-key UDF → batch_apply_blocking
            if spec0.op_type == "udf":
                return self._execute_batch_udf_blocking(spec0, disp, handler)

        return None

    def _execute_spec_blocking(
        self,
        spec: _OperationSpec,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync dispatch for one :class:`_OperationSpec`.

        Mirrors :meth:`_execute_spec` shape-for-shape, routing each
        (op_type, key-cardinality) combination to its blocking sibling.
        Single-key dispatches via :meth:`_execute_single_key_direct_blocking`
        (which itself branches on op_type). Multi-key dispatches via the
        ``_execute_batch_*_blocking`` family.
        """
        keys = spec.keys
        op_type = spec.op_type

        if len(keys) == 1:
            result = self._execute_single_key_direct_blocking(spec, disp, handler)
            if result is None:
                raise NotImplementedError(
                    f"blocking single-key dispatch missing for op_type={op_type}")
            return result

        if op_type is None:
            if spec.operations:
                return self._execute_batch_read_operate_blocking(spec, disp, handler)
            return self._execute_batch_read_blocking(spec, disp, handler)
        if op_type == "udf":
            return self._execute_batch_udf_blocking(spec, disp, handler)
        if op_type == "delete":
            return self._execute_batch_delete_blocking(spec, disp, handler)
        if op_type == "touch":
            return self._execute_batch_touch_blocking(spec, disp, handler)
        if op_type == "exists":
            return self._execute_batch_exists_blocking(spec, disp, handler)
        return self._execute_batch_write_blocking(spec, disp, handler)

    def _execute_multispec_blocking(
        self,
        on_error: Optional[OnError] = None,
    ) -> Optional[List[RecordResult]]:
        """Multi-spec blocking dispatch.

        For builders that ended up with more than one :class:`_OperationSpec`
        (chained queries), routes through either:

        - **Sequential**: when :meth:`_specs_require_sequential_run` is true
          (e.g. any UDF spec), per-spec dispatch via
          :meth:`_execute_spec_blocking`, results concatenated.
        - **Mixed batch**: combine all per-spec ops into a single PAC
          ``batch_blocking`` call.

        Returns:
            Concatenated list of :class:`RecordResult` on success. ``None``
            when the builder has 0 or 1 specs (caller falls back —
            single-spec handled by :meth:`_execute_blocking_fast_path`).
        """
        self._finalize_current_spec()
        self._ensure_namespace_mode_blocking()

        if not self._specs or len(self._specs) <= 1:
            return None

        disp = _resolve_disposition(on_error, is_single_key=False)
        handler = on_error if callable(on_error) else None

        if self._specs_require_sequential_run():
            all_results: List[RecordResult] = []
            for spec in self._specs:
                all_results.extend(self._execute_spec_blocking(spec, disp, handler))
            return all_results

        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        all_ops: list = []
        all_keys: List[Key] = []
        for spec in self._specs:
            all_keys.extend(spec.keys)
            all_ops.extend(self._spec_to_batch_ops(spec))
        try:
            if (
                self._implicit_txn_precheck()
                and any(not isinstance(op, BatchReadOp) for op in all_ops)
                and self._sdk_client._supports_mrt_blocking()
            ):
                batch_records = run_in_implicit_txn_blocking(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_blocking(
                        all_ops, batch_policy=stamp_txn(batch_policy, txn)))
            else:
                batch_records = self._client.batch_blocking(all_ops, batch_policy=batch_policy)
        except Exception as e:
            return self._handle_batch_error_list(all_keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler)

    def _execute_blocking_stream(
        self,
        on_error: Optional[OnError] = None,
    ) -> Optional[tuple]:
        """Dataset/SI/scan blocking dispatch returning a streaming source.

        For keyless query shapes (``session.query(dataset)`` or
        ``session.query(namespace, set_name)``), build the policy +
        statement synchronously and call PAC ``query_blocking``. Returns
        the raw :class:`Recordset` (Python iterator that blocks per
        record) so the caller can wrap it in :class:`RecordStream`
        without materializing — memory stays bounded for arbitrarily large
        result sets.

        Returns:
            ``(kind, payload)`` where ``kind`` is ``"recordset"`` for
            non-chunked or ``"chunked"`` for chunk-resumable queries. The
            payload for ``"recordset"`` is the PAC ``Recordset``; for
            ``"chunked"`` it is ``(recordset, reexecute_callable)``.
            Returns ``None`` when the spec shape isn't a keyless dataset
            query (caller falls back).
        """
        del on_error  # dataset queries don't currently honor per-record
                      # dispositions here; PSDK propagates errors at the
                      # iterator boundary.
        self._finalize_current_spec()
        self._ensure_namespace_mode_blocking()

        # Keyless query: either no specs (dataset query, attached at
        # builder construction) or specs without keys.
        is_keyless = not self._specs or all(not s.keys for s in self._specs)
        if not is_keyless:
            return None

        recordset, reexecute = self._execute_dataset_query_blocking()
        if reexecute is not None:
            return ("chunked", (recordset, reexecute))
        return ("recordset", recordset)

    def _execute_batch_read_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_read`.

        Same policy construction and disposition handling, but the dispatch
        is PAC ``batch_read_blocking`` — zero asyncio. Returns a list of
        :class:`RecordResult` that the caller (the sync builder) wraps with
        :class:`RecordStream.from_list`.
        """
        batch_read_policy = None
        if self._behavior is not None:
            settings = self._behavior.get_settings(
                OpKind.READ, OpShape.BATCH, self._resolved_namespace_mode())
            batch_read_policy = to_batch_read_policy(settings)
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        spec_brp = self._make_batch_read_policy(spec)
        if spec_brp is not None:
            # _make_batch_read_policy currently returns a fresh policy
            # carrying the spec's filter_expression. Merge into the behavior
            # policy when both exist; otherwise the spec policy wins.
            if batch_read_policy is None:
                batch_read_policy = spec_brp
            else:
                batch_read_policy.filter_expression = spec_brp.filter_expression
        try:
            batch_records = self._client.batch_read_blocking(
                spec.keys, spec.bins,
                batch_policy=batch_policy, read_policy=batch_read_policy)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler)

    def _execute_batch_write_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_write` — uses
        ``batch_operate_blocking``."""
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bwp = self._make_batch_write_policy(spec)
        ops_per_key = [spec.operations] * len(spec.keys)
        try:
            if self._implicit_txn_gate_blocking():
                batch_records = run_in_implicit_txn_blocking(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_operate_blocking(
                        spec.keys, ops_per_key,
                        batch_policy=stamp_txn(batch_policy, txn),
                        write_policy=bwp))
            else:
                batch_records = self._client.batch_operate_blocking(
                    spec.keys, ops_per_key,
                    batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler, op_type=spec.op_type)

    def _execute_batch_read_operate_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_read_operate` — uses
        ``batch_operate_blocking`` with read-style ops."""
        batch_policy = self._batch_policy_for(OpKind.READ, OpShape.BATCH)
        ops_per_key = [spec.operations] * len(spec.keys)
        bwp = None
        if spec.filter_expression is not None:
            bwp = BatchWritePolicy()
            bwp.filter_expression = spec.filter_expression
        try:
            batch_records = self._client.batch_operate_blocking(
                spec.keys, ops_per_key,
                batch_policy=batch_policy, write_policy=bwp)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler)

    def _execute_batch_delete_blocking(
        self, spec: _OperationSpec,
        disp: _ErrorDisposition, handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """Sync counterpart of :meth:`_execute_batch_delete` — uses
        ``batch_delete_blocking``."""
        batch_policy = self._batch_policy_for(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH)
        bdp = self._make_batch_delete_policy(spec)
        try:
            if self._implicit_txn_gate_blocking():
                batch_records = run_in_implicit_txn_blocking(
                    self._client, self._implicit_txn_settings(),
                    lambda txn: self._client.batch_delete_blocking(
                        spec.keys,
                        batch_policy=stamp_txn(batch_policy, txn),
                        delete_policy=bdp))
            else:
                batch_records = self._client.batch_delete_blocking(
                    spec.keys, batch_policy=batch_policy, delete_policy=bdp)
        except Exception as e:
            return self._handle_batch_error_list(spec.keys, e, disp, handler)
        return self._filtered_batch_list(batch_records, disp, handler, op_type="delete")

    def _execute_dataset_query_blocking(self) -> Any:
        """Sync counterpart of :meth:`_execute_dataset_query`.

        Builds the same policy + statement, then dispatches via PAC
        ``query_blocking`` which returns a :class:`Recordset` with the
        Python-iterator protocol (blocking ``__next__`` that releases the
        GIL while waiting on the underlying Tokio stream). The caller wraps
        the returned recordset in :class:`RecordStream`.

        Returns:
            The PAC ``Recordset`` (raw). For chunked queries the second
            return value is a sync ``reexecute`` callable; otherwise it is
            ``None``. ``(recordset, reexecute_or_none)``.

        Note:
            Mirrors the async path: when an AEL ``where()`` is present and
            an :class:`IndexesMonitor` is attached, blocks until the
            monitor's first refresh has completed so cached secondary-index
            metadata is available for filter generation.
        """
        log.debug(
            "dataset query (blocking): %s.%s filter=%s chunk=%s hint=%s",
            self._namespace, self._set_name,
            self._filter_expression is not None or bool(self._filter_records),
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
        if self._chunk_size is not None and self._chunk_size > 0:
            policy.max_records = self._chunk_size
        if self._filter_expression is not None:
            policy.filter_expression = self._filter_expression

        hint = self._query_hint
        if hint is not None and hint.query_duration is not None:
            policy.expected_duration = hint.query_duration

        if self._where_ael is not None and self._indexes_monitor is not None:
            # Lazy start (idempotent); mirrors the async path.
            self._indexes_monitor.start(self._client)
            self._indexes_monitor.wait_until_ready()

        self._resolve_index_context()

        partition_filter = self._partition_filter or PartitionFilter.all()

        if self._where_ael is not None and self._index_context is not None and (
            not self._supports_server_compiled_ael
        ):
            self._auto_generate_filters(hint, policy)

        statement = self._build_statement()

        try:
            recordset = self._client.query_blocking(statement, partition_filter, policy=policy)
        except Exception as e:
            raise _convert_pac_exception(e) from e

        if self._chunk_size is not None and self._chunk_size > 0:
            client = self._client

            def _reexecute_blocking(pf: PartitionFilter) -> Any:
                return client.query_blocking(statement, pf, policy=policy)

            return (recordset, _reexecute_blocking)

        return (recordset, None)
    def _handle_error_blocking_singlekey(
        self,
        key: Key,
        exc: Exception,
        op_type: Optional[str] = None,
        disp: _ErrorDisposition = _ErrorDisposition.THROW,
        handler: ErrorHandler | None = None,
    ) -> List[RecordResult]:
        """Mirror :meth:`_handle_error` for the blocking single-key path.

        Returns a list of zero or one :class:`RecordResult` — caller wraps
        with ``RecordStream.from_list``. THROW raises the converted
        exception on actionable codes; HANDLER dispatches to the callback
        and returns ``[]``; IN_STREAM embeds the error as a
        :class:`RecordResult`.
        """
        pfc_exc = _convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        in_doubt = pfc_exc.in_doubt
        _cmd_failed(op_type, rc, pfc_exc, self._client)

        if self._is_actionable(rc, op_type):
            if disp is _ErrorDisposition.THROW:
                raise pfc_exc from exc
            if disp is _ErrorDisposition.HANDLER and handler is not None:
                handler(key, 0, pfc_exc)
                return []

        if not self._should_include_result(rc, self._respond_all_keys, self._fail_on_filtered_out):
            return []
        return [RecordResult(
            key=key, record=None, result_code=rc,
            in_doubt=in_doubt, index=0, exception=pfc_exc,
        )]