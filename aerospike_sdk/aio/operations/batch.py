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

from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
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
    Key,
    Operation,
    Txn,
)

from aerospike_sdk.aio.operations.query import _build_exp_write_flags
from aerospike_sdk.ael.parser import parse_ael
from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import resolve_durable_delete, to_batch_policy
from aerospike_sdk.record_stream import RecordStream

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.policy.behavior import Behavior

NamespaceModeResolver = Optional[Callable[[str], Awaitable[Mode]]]


class BatchOpType(Enum):
    """Type of batch operation."""
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"
    REPLACE = "replace"
    REPLACE_IF_EXISTS = "replace_if_exists"
    DELETE = "delete"


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


class BatchKeyOperationBuilder:
    """
    Builder for a single key's operation within a batch.
    
    This class allows chaining bin operations and then continuing
    to add more keys to the batch.
    
    Example:
        batch.insert(key1).bin("name").set_to("Alice") \\
             .update(key2).bin("counter").add(1)
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
    
    # Methods to continue chaining to more keys (delegate to batch)
    
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
    
    async def execute(self) -> RecordStream:
        """Execute all batch operations."""
        return await self._batch.execute()


class BatchOperationBuilder:
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
    
    def __init__(
        self,
        client: Client,
        behavior: Optional[Behavior] = None,
        txn: Optional[Txn] = None,
        namespace_mode_resolver: NamespaceModeResolver = None,
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
        """
        self._client = client
        self._behavior = behavior
        self._key_operations: List[BatchKeyOperationBuilder] = []
        self._txn: Optional[Txn] = txn
        self._namespace_mode_resolver = namespace_mode_resolver

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
    
    async def execute(self) -> RecordStream:
        """Execute all batch operations.

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

        Returns:
            A :class:`RecordStream` of per-key :class:`RecordResult` items.

        Raises:
            ValueError: If no operations have been added.
        """
        if not self._key_operations:
            raise ValueError("No operations to execute. Add operations with insert(), update(), etc.")

        # Separate delete operations from others (they use batch_delete)
        delete_keys: List[Key] = []
        operate_keys: List[Key] = []
        operate_ops: List[List[Union[Operation, ExpOperation]]] = []

        for key_op in self._key_operations:
            if key_op._op_type == BatchOpType.DELETE:
                delete_keys.append(key_op._key)
            else:
                operate_keys.append(key_op._key)
                # Build operations list for this key
                ops = key_op._operations.copy()

                # If no operations but we have bins, convert to put operations
                if not ops and key_op._bins:
                    for bin_name, value in key_op._bins.items():
                        ops.append(Operation.put(bin_name, value))

                # If still no operations, add a touch to make it valid
                if not ops:
                    ops.append(Operation.touch())

                operate_ops.append(ops)

        raw_results: list = []

        all_keys = [key_op._key for key_op in self._key_operations]
        batch_mode = await self._resolved_mode_for_keys(all_keys)

        batch_policy = None
        if self._behavior is not None:
            batch_policy = to_batch_policy(
                self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, batch_mode))
        # Under MRT the PAC rejects a null BatchPolicy, so materialize one
        # just to carry the txn reference when the behavior path didn't.
        if self._txn is not None and batch_policy is None:
            from aerospike_async import BatchPolicy
            batch_policy = BatchPolicy()
        self._apply_txn(batch_policy)

        try:
            delete_policy: Optional[BatchDeletePolicy] = None
            if delete_keys and self._behavior is not None:
                bs = self._behavior.get_settings(
                    OpKind.WRITE_NON_RETRYABLE,
                    OpShape.BATCH,
                    await self._resolved_mode_for_keys(delete_keys),
                )
                if resolve_durable_delete(bs.durable_delete, None, None):
                    delete_policy = BatchDeletePolicy()
                    delete_policy.durable_delete = True

            if delete_keys:
                delete_results = await self._client.batch_delete(
                    batch_policy,
                    delete_policy,
                    delete_keys,
                )
                raw_results.extend(delete_results)

            if operate_keys:
                operate_results = await self._client.batch_operate(
                    batch_policy,
                    None,
                    operate_keys,
                    operate_ops,
                )
                raw_results.extend(operate_results)
        except Exception as e:
            raise _convert_pac_exception(e) from e

        return RecordStream.from_batch_records(raw_results)
