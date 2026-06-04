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

"""Synchronous batch operation builders delegating to ``aio.operations.batch``."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union

from aerospike_async import (
    BatchDeletePolicy,
    FilterExpression,
    Key,
)

from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder as AsyncBatchBinBuilder,
    BatchKeyOperationBuilder as AsyncBatchKeyOperationBuilder,
    BatchOperationBuilder as AsyncBatchOperationBuilder,
)
from aerospike_sdk.operations_shared import BatchOpType, _build_pac_batch_ops
from aerospike_sdk.error_strategy import ErrorHandler, _filter_records_with_handler
from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.policy.behavior_settings import OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import resolve_durable_delete, to_batch_policy
from aerospike_sdk.record_result import batch_records_to_results
from aerospike_sdk.sync.record_stream import SyncRecordStream


# The sync wrappers in this module hold an :class:`AsyncBatchOperationBuilder`
# as ``self._inner`` purely as a state bag — fluent chaining
# (``.insert(k).bin(...).set_to(...)``) mutates that inner builder's
# ``_key_operations`` list. At ``execute()`` / ``execute_stream()`` time the
# sync wrappers call PAC's ``*_blocking`` entries directly; no asyncio loop is
# ever entered. Pure-Python op-construction helpers
# (:func:`_build_pac_batch_ops`, :func:`_write_policy_for_op_type`) live in
# :mod:`aerospike_sdk.operations_shared` and are imported by both surfaces — they
# aren't "async" code, just shared.


def _dispatch_batch_stream_blocking(inner: AsyncBatchOperationBuilder) -> Any:
    """Build a mixed PAC ops list from `inner`'s accumulated state and call
    ``batch_stream_blocking``. Returns the raw PAC ``BatchRecordStream``.

    Single source of truth for the streaming-sync dispatch shape — both the
    multi-key and single-key sync wrappers call this so the spec logic
    (per-op policies, ops-list construction, error conversion) lives in one
    place on the sync side.
    """
    if not inner._key_operations:
        raise ValueError(
            "No operations to execute. Add operations with insert(), update(), etc.")

    all_keys = [key_op._key for key_op in inner._key_operations]
    batch_mode = inner._resolved_mode_for_keys_blocking(all_keys)

    batch_policy = None
    if inner._behavior is not None:
        batch_policy = to_batch_policy(
            inner._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, batch_mode))
    if inner._txn is not None and batch_policy is None:
        from aerospike_async import BatchPolicy
        batch_policy = BatchPolicy()
    inner._apply_txn(batch_policy)

    delete_policy: Optional[BatchDeletePolicy] = None
    has_delete = any(k._op_type == BatchOpType.DELETE for k in inner._key_operations)
    if has_delete and inner._behavior is not None:
        delete_keys = [k._key for k in inner._key_operations
                       if k._op_type == BatchOpType.DELETE]
        bs = inner._behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE,
            OpShape.BATCH,
            inner._resolved_mode_for_keys_blocking(delete_keys),
        )
        if resolve_durable_delete(bs.durable_delete, None, None):
            delete_policy = BatchDeletePolicy()
            delete_policy.durable_delete = True

    ops = _build_pac_batch_ops(inner._key_operations, delete_policy)

    try:
        return inner._client.batch_stream_blocking(
            ops, batch_policy=batch_policy,
        )
    except Exception as e:
        raise _convert_pac_exception(e) from e


class SyncBatchBinBuilder:
    """Sync wrapper for :class:`~aerospike_sdk.aio.operations.batch.BatchBinBuilder`.

    See Also:
        :class:`~aerospike_sdk.aio.operations.batch.BatchBinBuilder`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBatchBinBuilder) -> None:
        self._inner = inner

    def set_to(self, value: Any) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.set_to(value))

    def set_to_geo_json(self, geo_json: str) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.set_to_geo_json(geo_json),
        )

    def add(self, value: int) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.add(value))

    def increment_by(self, value: int) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.increment_by(value))

    def append(self, value: str) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.append(value))

    def prepend(self, value: str) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.prepend(value))

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.select_from(expression, ignore_eval_failure=ignore_eval_failure),
        )

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.insert_from(
                expression,
                ignore_op_failure=ignore_op_failure,
                ignore_eval_failure=ignore_eval_failure,
                delete_if_null=delete_if_null,
            ),
        )

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.update_from(
                expression,
                ignore_op_failure=ignore_op_failure,
                ignore_eval_failure=ignore_eval_failure,
                delete_if_null=delete_if_null,
            ),
        )

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.upsert_from(
                expression,
                ignore_op_failure=ignore_op_failure,
                ignore_eval_failure=ignore_eval_failure,
                delete_if_null=delete_if_null,
            ),
        )

    # -- HyperLogLog ----------------------------------------------------------

    def hll_init(
        self,
        config: HllConfig,
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_init(
                config,
                create_only=create_only, update_only=update_only,
                no_fail=no_fail, allow_fold=allow_fold,
            ),
        )

    def hll_add(
        self,
        values: Sequence[Any],
        *,
        config: Optional[HllConfig] = None,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_add(
                values, config=config,
                create_only=create_only, update_only=update_only,
                no_fail=no_fail, allow_fold=allow_fold,
            ),
        )

    def hll_set_union(
        self,
        hll_list: Sequence[Any],
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_set_union(
                hll_list,
                create_only=create_only, update_only=update_only,
                no_fail=no_fail, allow_fold=allow_fold,
            ),
        )

    def hll_fold(self, index_bit_count: int) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_fold(index_bit_count),
        )

    def hll_refresh_count(self) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_refresh_count(),
        )

    def hll_get_count(self) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_get_count(),
        )

    def hll_describe(self) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_describe(),
        )

    def hll_get_union(self, hll_list: Sequence[Any]) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_get_union(hll_list),
        )

    def hll_get_union_count(self, hll_list: Sequence[Any]) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_get_union_count(hll_list),
        )

    def hll_get_intersect_count(self, hll_list: Sequence[Any]) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_get_intersect_count(hll_list),
        )

    def hll_get_similarity(self, hll_list: Sequence[Any]) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(
            self._inner.hll_get_similarity(hll_list),
        )


class SyncBatchKeyOperationBuilder:
    """Sync wrapper for :class:`~aerospike_sdk.aio.operations.batch.BatchKeyOperationBuilder`.

    See Also:
        :class:`~aerospike_sdk.aio.operations.batch.BatchKeyOperationBuilder`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBatchKeyOperationBuilder) -> None:
        self._inner = inner

    def bin(self, bin_name: str) -> SyncBatchBinBuilder:
        return SyncBatchBinBuilder(self._inner.bin(bin_name))

    def put(self, bins: Dict[str, Any]) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.put(bins))

    def insert(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.insert(key))

    def update(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.update(key))

    def upsert(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.upsert(key))

    def replace(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.replace(key))

    def replace_if_exists(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.replace_if_exists(key))

    def delete(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.delete(key))

    def execute(self, on_error: Optional[ErrorHandler] = None) -> SyncRecordStream:
        """Buffered sync batch execute — writes-complete-on-return.

        Mirrors :meth:`aerospike_sdk.aio.operations.batch.BatchOperationBuilder.execute`.
        For lazy per-record streaming, see :meth:`execute_stream`.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                Failed per-key results are dispatched to the handler and
                excluded from the returned stream.
        """
        raw = self._inner.execute_blocking()
        if on_error is not None:
            return SyncRecordStream.from_list(
                _filter_records_with_handler(
                    batch_records_to_results(raw), on_error,
                ),
            )
        return SyncRecordStream.from_batch_records(raw)

    def execute_stream(
        self, on_error: Optional[ErrorHandler] = None,
    ) -> SyncRecordStream:
        """Streaming sync batch execute — yields records in completion order.

        See :meth:`SyncBatchOperationBuilder.execute_stream` for full
        documentation, including the trade-offs vs :meth:`execute`.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                Per-key failures are dispatched to the handler and excluded
                from the returned stream; cluster-level errors still raise.
        """
        # Key-level builder holds no state of its own; the multi-key parent
        # at `self._inner._batch` is the source of truth for accumulated ops.
        pac_stream = _dispatch_batch_stream_blocking(self._inner._batch)
        return SyncRecordStream.from_pac_batch_stream(pac_stream, on_error=on_error)


class SyncBatchOperationBuilder:
    """Sync wrapper for :class:`~aerospike_sdk.aio.operations.batch.BatchOperationBuilder`.

    See Also:
        :class:`~aerospike_sdk.aio.operations.batch.BatchOperationBuilder`
        :meth:`~aerospike_sdk.sync.session.SyncSession.batch`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBatchOperationBuilder) -> None:
        self._inner = inner

    def insert(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.insert(key))

    def update(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.update(key))

    def upsert(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.upsert(key))

    def replace(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.replace(key))

    def replace_if_exists(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.replace_if_exists(key))

    def delete(self, key: Key) -> SyncBatchKeyOperationBuilder:
        return SyncBatchKeyOperationBuilder(self._inner.delete(key))

    def execute(self, on_error: Optional[ErrorHandler] = None) -> SyncRecordStream:
        """Buffered sync batch execute — writes-complete-on-return.

        Awaits all per-key results before returning a
        :class:`SyncRecordStream` backed by a fully-materialized list.
        Safe for "fire-and-forget" use; subsequent reads observe the new
        state without races.

        For true per-record streaming, see :meth:`execute_stream`.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, failed per-key results are dispatched to the
                callback and excluded from the returned stream — the stream
                contains only successes. Cluster-level errors still raise.

        Returns:
            A :class:`SyncRecordStream` of per-key :class:`RecordResult`
            items (positional via :attr:`RecordResult.index`).
        """
        raw = self._inner.execute_blocking()
        if on_error is not None:
            return SyncRecordStream.from_list(
                _filter_records_with_handler(
                    batch_records_to_results(raw), on_error,
                ),
            )
        return SyncRecordStream.from_batch_records(raw)

    def execute_stream(
        self, on_error: Optional[ErrorHandler] = None,
    ) -> SyncRecordStream:
        """Lazy sync batch execute — yields records in completion order.

        Dispatches all ops in a single mixed ``batch_stream_blocking`` call
        and returns a :class:`SyncRecordStream` whose ``__next__`` pulls
        ``(idx, BatchRecord)`` tuples from the PAC stream one at a time.

        **Caveats** — differ from :meth:`execute`:

        - **Yields completion order, not input order.** Each
          :class:`RecordResult` carries its originating op's input
          position in :attr:`RecordResult.index`; sort after collecting if
          you need positional results.
        - **Per-key errors inline** on :class:`RecordResult` (when
          ``on_error`` is unset); cluster-level errors raise from
          ``__next__``.
        - **No writes-complete-on-return guarantee.** Per-node tasks
          dispatch in the background; if the caller discards the stream
          without draining, server-side writes may still be in-flight.
          Tests / callers that follow "execute then immediately read"
          should use :meth:`execute` instead.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, per-key failures are dispatched to the handler
                as records arrive and excluded from the returned stream;
                cluster-level errors still raise from ``__next__``.

        Returns:
            A lazy :class:`SyncRecordStream`. Iterate to drive PAC's
            per-record yield.

        Raises:
            ValueError: If no operations have been added.
        """
        pac_stream = _dispatch_batch_stream_blocking(self._inner)
        return SyncRecordStream.from_pac_batch_stream(pac_stream, on_error=on_error)
