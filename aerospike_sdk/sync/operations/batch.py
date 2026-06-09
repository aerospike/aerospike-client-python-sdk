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

from aerospike_async import FilterExpression, Key

from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder as AsyncBatchBinBuilder,
    BatchKeyOperationBuilder as AsyncBatchKeyOperationBuilder,
    BatchOperationBuilder as AsyncBatchOperationBuilder,
)
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.sync.record_stream import SyncRecordStream


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

    def execute(self) -> SyncRecordStream:
        raw = self._inner.execute_blocking()
        return SyncRecordStream.from_batch_records(raw)


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

    def execute(self) -> SyncRecordStream:
        raw = self._inner.execute_blocking()
        return SyncRecordStream.from_batch_records(raw)
