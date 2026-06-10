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

"""BatchOperationBuilder - Builder for chaining operations across multiple keys."""

from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Union,
)

from aerospike_async import (
    BatchDeletePolicy,
    Client,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    FilterExpression,
    GeoJSON,
    HllOperation,
    Key,
    Operation,
    Txn,
)

from aerospike_sdk.aio.operations.query import _resolve_hll_flags
from aerospike_sdk.ael.parser import parse_ael
from aerospike_sdk.operations_shared import (
    BatchOpType,
    _build_exp_write_flags,
    _build_pac_batch_ops,
)
from aerospike_sdk.error_strategy import ErrorHandler, _filter_records_with_handler
from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import resolve_durable_delete, to_batch_policy
from aerospike_sdk.record_result import batch_records_to_results
from aerospike_sdk.record_stream import RecordStream

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.policy.behavior import Behavior

NamespaceModeResolver = Optional[Callable[[str], Awaitable[Mode]]]


class BatchBinBuilder:
    """
    Builder for chaining bin operations within a batch key operation.
    
    Example:
        batch.insert(key).bin("name").set_to("Alice").bin("age").set_to(25)
    """
    
    def __init__(self, key_op: BatchKeyOperationBuilder, bin_name: str) -> None:
        self._key_op = key_op
        self._bin_name = bin_name
    
    def set_to(self, value: Any) -> BatchKeyOperationBuilder:
        """
        Set a bin value.

        Args:
            value: The value to set.

        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._bins[self._bin_name] = value
        self._key_op._operations.append(Operation.put(self._bin_name, value))
        return self._key_op

    def set_to_geo_json(self, geo_json: str) -> BatchKeyOperationBuilder:
        """Set the bin to a GeoJSON value from its string form.

        The bin's server-side particle type is GEOJSON, not STRING. Equivalent
        to ``set_to(GeoJSON(geo_json))`` but reads naturally for spatial data.

        Args:
            geo_json: A GeoJSON string (e.g. a Point, Polygon, or AeroCircle).

        Returns:
            The parent :class:`BatchKeyOperationBuilder`.
        """
        value = GeoJSON(geo_json)
        self._key_op._bins[self._bin_name] = value
        self._key_op._operations.append(Operation.put(self._bin_name, value))
        return self._key_op

    def add(self, value: int) -> BatchKeyOperationBuilder:
        """Add *value* to the bin (numeric increment)."""
        self._key_op._operations.append(Operation.add(self._bin_name, value))
        return self._key_op

    def increment_by(self, value: int) -> BatchKeyOperationBuilder:
        """Alias for :meth:`add`."""
        return self.add(value)
    
    def append(self, value: str) -> BatchKeyOperationBuilder:
        """
        Append a string to a bin value.
        
        Args:
            value: The string to append.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._operations.append(Operation.append(self._bin_name, value))
        return self._key_op
    
    def prepend(self, value: str) -> BatchKeyOperationBuilder:
        """
        Prepend a string to a bin value.
        
        Args:
            value: The string to prepend.
        
        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        self._key_op._operations.append(Operation.prepend(self._bin_name, value))
        return self._key_op

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> BatchKeyOperationBuilder:
        """
        Read the result of an expression into this bin.

        Args:
            expression: AEL string or pre-built FilterExpression.
            ignore_eval_failure: If True, suppress evaluation errors.

        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        flags = ExpReadFlags.EVAL_NO_FAIL if ignore_eval_failure else ExpReadFlags.DEFAULT
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        self._key_op._operations.append(ExpOperation.read(self._bin_name, expr, flags))
        return self._key_op

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BatchKeyOperationBuilder:
        """
        Write expression result to this bin only if it does not already exist.

        Args:
            expression: AEL string or pre-built FilterExpression.
            ignore_op_failure: If True, suppress BIN_EXISTS_ERROR.
            ignore_eval_failure: If True, suppress evaluation errors.
            delete_if_null: If True, delete bin when expression evaluates to nil.

        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        flags = _build_exp_write_flags(
            ExpWriteFlags.CREATE_ONLY, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        self._key_op._operations.append(ExpOperation.write(self._bin_name, expr, flags))
        return self._key_op

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BatchKeyOperationBuilder:
        """
        Write expression result to this bin only if it already exists.

        Args:
            expression: AEL string or pre-built FilterExpression.
            ignore_op_failure: If True, suppress BIN_NOT_FOUND.
            ignore_eval_failure: If True, suppress evaluation errors.
            delete_if_null: If True, delete bin when expression evaluates to nil.

        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        flags = _build_exp_write_flags(
            ExpWriteFlags.UPDATE_ONLY, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        self._key_op._operations.append(ExpOperation.write(self._bin_name, expr, flags))
        return self._key_op

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> BatchKeyOperationBuilder:
        """
        Write expression result to this bin (create or update).

        Args:
            expression: AEL string or pre-built FilterExpression.
            ignore_op_failure: If True, suppress policy errors.
            ignore_eval_failure: If True, suppress evaluation errors.
            delete_if_null: If True, delete bin when expression evaluates to nil.

        Returns:
            The parent BatchKeyOperationBuilder for chaining.
        """
        flags = _build_exp_write_flags(
            ExpWriteFlags.DEFAULT, ignore_op_failure, ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        self._key_op._operations.append(ExpOperation.write(self._bin_name, expr, flags))
        return self._key_op

    # -- HyperLogLog ----------------------------------------------------------

    def hll_init(
        self,
        config: HllConfig,
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> BatchKeyOperationBuilder:
        """Initialize an empty HyperLogLog sketch in this bin.

        Semantics match
        :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_init`.
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        self._key_op._operations.append(HllOperation.init(
            self._bin_name, config.index_bit_count, config.min_hash_bit_count, flags,
        ))
        return self._key_op

    def hll_add(
        self,
        values: Sequence[Any],
        *,
        config: Optional[HllConfig] = None,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> BatchKeyOperationBuilder:
        """Add distinct values to the HyperLogLog sketch in this bin.

        Semantics match
        :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_add`.
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        index_bit_count = config.index_bit_count if config is not None else -1
        min_hash_bit_count = config.min_hash_bit_count if config is not None else -1
        self._key_op._operations.append(HllOperation.add(
            self._bin_name, list(values), index_bit_count, min_hash_bit_count, flags,
        ))
        return self._key_op

    def hll_set_union(
        self,
        hll_list: Sequence[Any],
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> BatchKeyOperationBuilder:
        """Merge other HyperLogLog sketches into this bin (destructive union)."""
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        self._key_op._operations.append(
            HllOperation.set_union(self._bin_name, list(hll_list), flags),
        )
        return self._key_op

    def hll_fold(self, index_bit_count: int) -> BatchKeyOperationBuilder:
        """Reduce sketch precision to a lower ``index_bit_count``."""
        self._key_op._operations.append(
            HllOperation.fold(self._bin_name, index_bit_count),
        )
        return self._key_op

    def hll_refresh_count(self) -> BatchKeyOperationBuilder:
        """Refresh the cached cardinality estimate."""
        self._key_op._operations.append(HllOperation.refresh_count(self._bin_name))
        return self._key_op

    def hll_get_count(self) -> BatchKeyOperationBuilder:
        """Read the estimated cardinality of the sketch."""
        self._key_op._operations.append(HllOperation.get_count(self._bin_name))
        return self._key_op

    def hll_describe(self) -> BatchKeyOperationBuilder:
        """Read the sketch's index and minhash bit widths."""
        self._key_op._operations.append(HllOperation.describe(self._bin_name))
        return self._key_op

    def hll_get_union(self, hll_list: Sequence[Any]) -> BatchKeyOperationBuilder:
        """Read the union of this sketch with other sketches (non-destructive)."""
        self._key_op._operations.append(
            HllOperation.get_union(self._bin_name, list(hll_list)),
        )
        return self._key_op

    def hll_get_union_count(self, hll_list: Sequence[Any]) -> BatchKeyOperationBuilder:
        """Read the estimated cardinality of the union with other sketches."""
        self._key_op._operations.append(
            HllOperation.get_union_count(self._bin_name, list(hll_list)),
        )
        return self._key_op

    def hll_get_intersect_count(self, hll_list: Sequence[Any]) -> BatchKeyOperationBuilder:
        """Read the estimated intersection cardinality with other sketches."""
        self._key_op._operations.append(
            HllOperation.get_intersect_count(self._bin_name, list(hll_list)),
        )
        return self._key_op

    def hll_get_similarity(self, hll_list: Sequence[Any]) -> BatchKeyOperationBuilder:
        """Read Jaccard similarity between this sketch and other sketches."""
        self._key_op._operations.append(
            HllOperation.get_similarity(self._bin_name, list(hll_list)),
        )
        return self._key_op


class _BatchKeyOperationBuilderBase:
    """State + chaining shared by async and sync BatchKeyOperationBuilder.

    Methods migrate from :class:`BatchKeyOperationBuilder` during Phase 4 collapse.
    """
    def __init__(
        self,
        batch: BatchOperationBuilder,
        key: Key,
        op_type: BatchOpType,
    ) -> None:
        self._batch = batch
        self._key = key
        self._op_type = op_type
        self._bins: Dict[str, Any] = {}
        self._operations: List[Union[Operation, ExpOperation]] = []

    def bin(self, bin_name: str) -> BatchBinBuilder:
        """
        Start a bin operation chain.
        
        Args:
            bin_name: The name of the bin.
        
        Returns:
            A BatchBinBuilder for chaining bin operations.
        
        Example:
            batch.insert(key).bin("name").set_to("Alice").bin("age").set_to(25)
        """
        return BatchBinBuilder(self, bin_name)

    def put(self, bins: Dict[str, Any]) -> BatchKeyOperationBuilder:
        """
        Set multiple bins at once.
        
        Args:
            bins: Dictionary of bin name to value mappings.
        
        Returns:
            self for method chaining.
        
        Example:
            batch.insert(key).put({"name": "Alice", "age": 25})
        """
        self._bins.update(bins)
        for bin_name, value in bins.items():
            self._operations.append(Operation.put(bin_name, value))
        return self

    def insert(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an insert operation for another key."""
        return self._batch.insert(key)

    def update(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an update operation for another key."""
        return self._batch.update(key)

    def upsert(self, key: Key) -> BatchKeyOperationBuilder:
        """Add an upsert operation for another key."""
        return self._batch.upsert(key)

    def replace(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a replace operation for another key."""
        return self._batch.replace(key)

    def replace_if_exists(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a replace-if-exists operation for another key."""
        return self._batch.replace_if_exists(key)

    def delete(self, key: Key) -> BatchKeyOperationBuilder:
        """Add a delete operation for another key."""
        return self._batch.delete(key)

    def execute_blocking(self) -> list:
        """Sync execute; returns the raw PAC ``BatchRecord`` list."""
        return self._batch.execute_blocking()



class BatchKeyOperationBuilder(_BatchKeyOperationBuilderBase):
    """
    Builder for a single key's operation within a batch.
    
    This class allows chaining bin operations and then continuing
    to add more keys to the batch.
    
    Example:
        batch.insert(key1).bin("name").set_to("Alice") \\
             .update(key2).bin("counter").add(1)
    """
    
    
    
    
    # Methods to continue chaining to more keys (delegate to batch)
    
    
    
    
    
    
    
    async def execute(self, on_error: Optional[ErrorHandler] = None) -> RecordStream:
        """Execute all batch operations.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                Failed per-key results are dispatched to the handler and
                excluded from the returned stream.
        """
        return await self._batch.execute(on_error=on_error)

    async def execute_stream(
        self, on_error: Optional[ErrorHandler] = None,
    ) -> RecordStream:
        """Lazy streaming batch execute. See :meth:`BatchOperationBuilder.execute_stream`."""
        return await self._batch.execute_stream(on_error=on_error)



class _BatchOperationBuilderBase:
    """Source of truth for batch-builder state — ``_key_operations``,
    ``_behavior``, ``_txn``, ``_namespace_mode_resolver(_blocking)``,
    ``_client`` — that fluent chaining (``.insert(k).bin(...).set_to(...)``)
    mutates. Both the async dispatch path
    (:meth:`BatchOperationBuilder.execute` / :meth:`execute_stream`) and the
    sync wrappers in :mod:`aerospike_sdk.sync.operations.batch` (which reach
    in via ``self._inner.*``) read this state.

    The sync dispatch never enters an event loop — it calls PAC's
    ``*_blocking`` entries directly. Pure-Python op-construction helpers
    (:func:`_build_pac_batch_ops`, :func:`_write_policy_for_op_type`) live
    in :mod:`aerospike_sdk.operations_shared` and are shared by both surfaces;
    they aren't "async" code, just neutral utilities.

    :meth:`execute_blocking` on this base is the buffered sync sibling of
    :meth:`BatchOperationBuilder.execute` — no asyncio, returns the raw PAC
    ``BatchRecord`` list for the caller to wrap (the sync wrappers do).
    """
    def __init__(
        self,
        client: Client,
        behavior: Optional[Behavior] = None,
        txn: Optional[Txn] = None,
        namespace_mode_resolver: NamespaceModeResolver = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
    ) -> None:
        """
        Initialize a BatchOperationBuilder.

        Args:
            client: The underlying async client.
            behavior: Optional Behavior for deriving policies.
            txn: Optional active :class:`~aerospike_async.Txn` captured from
                a transactional session; stamped on the outer batch policy
                at execute. ``None`` means no transaction participation.
            namespace_mode_resolver: Optional async callable ``namespace -> Mode``
                used to apply AP vs SC behavior scopes before policies are built.
            namespace_mode_resolver_blocking: Optional sync callable for the
                same purpose, used by :meth:`execute_blocking`.
        """
        self._client = client
        self._behavior = behavior
        self._key_operations: List[BatchKeyOperationBuilder] = []
        self._txn: Optional[Txn] = txn
        self._namespace_mode_resolver = namespace_mode_resolver
        self._namespace_mode_resolver_blocking = namespace_mode_resolver_blocking

    def _apply_txn(self, policy: Any) -> Any:
        """Stamp this builder's captured txn on the outer batch policy."""
        if self._txn is not None and policy is not None:
            policy.txn = self._txn
        return policy

    def with_txn(self, txn: Optional[Txn]) -> "BatchOperationBuilder":
        """Opt this batch into (or out of) a specific transaction.

        See :meth:`aerospike_sdk.aio.operations.query.QueryBuilder.with_txn`
        for semantics.

        Args:
            txn: The :class:`~aerospike_async.Txn` to participate in, or
                ``None`` to opt out.

        Returns:
            This builder for chaining.
        """
        self._txn = txn
        return self

    def insert(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an insert (create only) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.insert(key).bin("name").set_to("Alice")
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.INSERT)
        self._key_operations.append(op)
        return op

    def update(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an update (update only) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.update(key).bin("counter").add(1)
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.UPDATE)
        self._key_operations.append(op)
        return op

    def upsert(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add an upsert (create or update) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.upsert(key).bin("name").set_to("Bob")
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.UPSERT)
        self._key_operations.append(op)
        return op

    def replace(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a replace (create or replace) operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.replace(key).put({"name": "Charlie", "age": 35})
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.REPLACE)
        self._key_operations.append(op)
        return op

    def replace_if_exists(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a replace-if-exists operation for a key.
        
        This operation will fail if the record does not exist.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for chaining bin operations.
        
        Example:
            batch.replace_if_exists(key).put({"name": "Updated", "status": "active"})
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.REPLACE_IF_EXISTS)
        self._key_operations.append(op)
        return op

    def delete(self, key: Key) -> BatchKeyOperationBuilder:
        """
        Add a delete operation for a key.
        
        Args:
            key: The key for the record.
        
        Returns:
            A BatchKeyOperationBuilder for continuing the chain.
        
        Example:
            batch.delete(key1).delete(key2).execute()
        """
        op = BatchKeyOperationBuilder(self, key, BatchOpType.DELETE)
        self._key_operations.append(op)
        return op

    def _resolved_mode_for_keys_blocking(self, keys: List[Key]) -> Mode:
        """Sync counterpart of :meth:`_resolved_mode_for_keys`."""
        if self._namespace_mode_resolver_blocking is None:
            return Mode.AP
        seen: set[str] = set()
        for key in keys:
            namespace = key.namespace
            if namespace in seen:
                continue
            seen.add(namespace)
            if self._namespace_mode_resolver_blocking(namespace) == Mode.SC:
                return Mode.SC
        return Mode.AP

    def execute_blocking(self) -> list:
        """Synchronously execute all batch operations; returns raw PAC ``BatchRecord`` list.

        Caller wraps in :class:`SyncRecordStream` (or any other sync
        iterator). Mirrors :meth:`execute` semantics: a single mixed
        ``batch_blocking`` call over pre-wrapped ops; per-key write
        policies enforce verb existence semantics on the wire. No
        asyncio loop is involved.
        """
        if not self._key_operations:
            raise ValueError("No operations to execute. Add operations with insert(), update(), etc.")

        all_keys = [key_op._key for key_op in self._key_operations]
        batch_mode = self._resolved_mode_for_keys_blocking(all_keys)

        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, batch_mode))
        if self._txn is not None and batch_policy is None:
            from aerospike_async import BatchPolicy
            batch_policy = BatchPolicy()
        self._apply_txn(batch_policy)

        delete_policy: Optional[BatchDeletePolicy] = None
        has_delete = any(k._op_type == BatchOpType.DELETE for k in self._key_operations)
        if has_delete and self._behavior is not None:
            delete_keys = [k._key for k in self._key_operations
                           if k._op_type == BatchOpType.DELETE]
            bs = self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE,
                OpShape.BATCH,
                self._resolved_mode_for_keys_blocking(delete_keys),
            )
            if resolve_durable_delete(bs.durable_delete, None, None):
                delete_policy = BatchDeletePolicy()
                delete_policy.durable_delete = True

        ops = _build_pac_batch_ops(self._key_operations, delete_policy)

        try:
            return self._client.batch_blocking(ops, batch_policy=batch_policy)
        except Exception as e:
            raise _convert_pac_exception(e) from e


class BatchOperationBuilder(_BatchOperationBuilderBase):
    """
    Builder for chaining operations across multiple keys.
    
    This class enables method chaining of operations on different keys,
    which are then executed as a single batch operation.
    
    Example::

            results = await session.batch() \\
                .insert(key1).bin("name").set_to("Alice").bin("age").set_to(25) \\
                .update(key2).bin("counter").add(1) \\
                .delete(key3) \\
                .execute()
    
    The operations are collected and executed together using the async
    client's batch_operate method for optimal performance.
    """
    



    async def _resolved_mode_for_keys(self, keys: List[Key]) -> Mode:
        """Return SC when any key belongs to an SC namespace."""
        if self._namespace_mode_resolver is None:
            return Mode.AP
        seen: set[str] = set()
        for key in keys:
            namespace = key.namespace
            if namespace in seen:
                continue
            seen.add(namespace)
            if await self._namespace_mode_resolver(namespace) == Mode.SC:
                return Mode.SC
        return Mode.AP
    
    
    
    
    
    
    
    async def execute(
        self, on_error: Optional[ErrorHandler] = None,
    ) -> RecordStream:
        """Execute all batch operations as a buffered call.

        Awaits all per-key results before returning a :class:`RecordStream`
        backed by a fully-materialized list. Writes are guaranteed to have
        completed server-side by the time this method returns; subsequent
        reads will observe the new state without races. Callers that don't
        iterate the returned stream are safe — the "fire-and-forget" shape
        works as expected.

        For true per-record streaming (records arrive at first-RTT, peak
        memory bounded), see :meth:`execute_stream` — note its different
        semantics (completion-order yields, no writes-complete-on-return
        guarantee, peak memory bounded).

        Internally dispatches as a single mixed PAC ``batch`` call over
        pre-wrapped :class:`BatchWriteOp` / :class:`BatchReadOp` /
        :class:`BatchDeleteOp` entries — each per-key write carries its
        own :class:`BatchWritePolicy` so the verb's existence semantics
        (``update`` requires existing, ``insert`` requires absent, …) are
        enforced on the wire. Results land in input order.

        Example::

            stream = await (
                session.batch()
                .insert(key1)
                .bin("name").set_to("Ada")
                .upsert(key2)
                .bin("n").set_to(1)
                .execute()
            )
            rows = await stream.collect()

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, failed per-key results are dispatched to the
                callback and excluded from the returned stream — the stream
                contains only successes. Cluster-level errors still raise.

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items,
            backed by a materialized list (positional via :attr:`RecordResult.index`).

        Raises:
            ValueError: If no operations have been added.
        """
        if not self._key_operations:
            raise ValueError("No operations to execute. Add operations with insert(), update(), etc.")

        all_keys = [key_op._key for key_op in self._key_operations]
        batch_mode = await self._resolved_mode_for_keys(all_keys)

        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, batch_mode))
        if self._txn is not None and batch_policy is None:
            from aerospike_async import BatchPolicy
            batch_policy = BatchPolicy()
        self._apply_txn(batch_policy)

        delete_policy: Optional[BatchDeletePolicy] = None
        has_delete = any(k._op_type == BatchOpType.DELETE for k in self._key_operations)
        if has_delete and self._behavior is not None:
            delete_keys = [k._key for k in self._key_operations
                           if k._op_type == BatchOpType.DELETE]
            bs = self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE,
                OpShape.BATCH,
                await self._resolved_mode_for_keys(delete_keys),
            )
            if resolve_durable_delete(bs.durable_delete, None, None):
                delete_policy = BatchDeletePolicy()
                delete_policy.durable_delete = True

        ops = _build_pac_batch_ops(self._key_operations, delete_policy)

        try:
            raw_results = await self._client.batch(ops, batch_policy=batch_policy)
        except Exception as e:
            raise _convert_pac_exception(e) from e

        if on_error is not None:
            return RecordStream.from_list(
                _filter_records_with_handler(
                    batch_records_to_results(raw_results), on_error,
                ),
            )
        return RecordStream.from_batch_records(raw_results)

    async def execute_stream(
        self, on_error: Optional[ErrorHandler] = None,
    ) -> RecordStream:
        """Lazy streaming batch execute — yields records in completion order.

        Builds a single mixed PAC ``batch_stream`` call covering all ops
        (reads, writes, deletes) and returns a :class:`RecordStream` whose
        ``__anext__`` pulls ``(idx, BatchRecord)`` tuples from the PAC
        stream one at a time. First record arrives at first-RTT, not after
        all keys complete; peak memory is bounded.

        **Caveats** — differ from :meth:`execute`:

        - **Yields completion order, not input order.** Each
          :class:`RecordResult` carries its originating op's input
          position in :attr:`RecordResult.index`; sort after collecting if
          positional results are needed.
        - **Per-key errors inline** on :class:`RecordResult` (when
          ``on_error`` is unset); cluster-level errors raise from
          ``__anext__``.
        - **No writes-complete-on-return guarantee.** Per-node tasks
          dispatch in the background; if the caller awaits this method
          but never iterates the returned stream, server-side writes may
          still be in-flight when subsequent code runs. Callers that need
          writes-complete-on-return — or use the "fire-and-forget" shape
          (``await session.batch()...execute_stream()`` without draining)
          — must use :meth:`execute` instead.

        Per-key op composition is inspected via
        :func:`aerospike_async.has_any_write_op` to choose the right PAC
        op wrapper: read-only op lists (e.g. AEL ``select_from``
        expressions) land as :class:`BatchReadOp`, write/mutation op
        lists as :class:`BatchWriteOp`. Matches the wire-dispatch
        behavior of the buffered :meth:`execute` path.

        Args:
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, per-key failures are dispatched to the handler
                as records arrive and excluded from the returned stream;
                cluster-level errors still raise from ``__anext__``.

        Returns:
            A lazy :class:`RecordStream`. ``async for`` it to drive
            PAC's per-record yield.

        Raises:
            ValueError: If no operations have been added.
        """
        if not self._key_operations:
            raise ValueError(
                "No operations to execute. Add operations with insert(), update(), etc.")

        all_keys = [key_op._key for key_op in self._key_operations]
        batch_mode = await self._resolved_mode_for_keys(all_keys)

        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, batch_mode))
        if self._txn is not None and batch_policy is None:
            from aerospike_async import BatchPolicy
            batch_policy = BatchPolicy()
        self._apply_txn(batch_policy)

        delete_policy: Optional[BatchDeletePolicy] = None
        has_delete = any(k._op_type == BatchOpType.DELETE for k in self._key_operations)
        if has_delete and self._behavior is not None:
            delete_keys = [k._key for k in self._key_operations
                           if k._op_type == BatchOpType.DELETE]
            bs = self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE,
                OpShape.BATCH,
                await self._resolved_mode_for_keys(delete_keys),
            )
            if resolve_durable_delete(bs.durable_delete, None, None):
                delete_policy = BatchDeletePolicy()
                delete_policy.durable_delete = True

        ops = _build_pac_batch_ops(self._key_operations, delete_policy)

        try:
            pac_stream = await self._client.batch_stream(
                ops, batch_policy=batch_policy,
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e

        return RecordStream.from_pac_batch_stream(pac_stream, on_error=on_error)


