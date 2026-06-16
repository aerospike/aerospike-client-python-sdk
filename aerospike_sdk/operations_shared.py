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

"""Neutral helpers for write operations — shared by async + sync, single-key + batch.

No asyncio anywhere. Holds the verb model (:class:`BatchOpType` enum,
:data:`_OP_TYPE_TO_REA` mapping) used by both the single-key write path
(:mod:`aerospike_sdk.aio.operations.query`) and the batch path
(:mod:`aerospike_sdk.aio.operations.batch` /
:mod:`aerospike_sdk.sync.operations.batch`), plus the PAC op-list builder
used by the batch dispatchers.
"""

from __future__ import annotations

from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ClassVar,
    List,
    Optional,
    Union,
    overload,
)

from typing_extensions import Self

from aerospike_async import (
    BatchDeleteOp,
    BatchDeletePolicy,
    BatchReadOp,
    BatchWriteOp,
    BatchWritePolicy,
    Client,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    Expiration,
    FilterExpression,
    Key,
    Operation,
    ReadPolicy,
    RecordExistsAction,
    Txn,
    WritePolicy,
    has_any_write_op,
)
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.ael.parser import parse_ael
from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import to_write_policy
from aerospike_sdk.record_stream import RecordStream

if TYPE_CHECKING:  # Forward-reference only; the concrete classes live in aio.operations.query.
    from aerospike_sdk.aio.operations.query import (
        QueryBuilder,
        WriteBinBuilder,
        WriteSegmentBuilder,
    )
    from aerospike_sdk.error_strategy import OnError
    from aerospike_sdk.record_result import RecordResult


NamespaceModeResolver = Optional[Callable[[str], Awaitable[Mode]]]


class BatchOpType(Enum):
    """Type of batch operation."""
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"
    REPLACE = "replace"
    REPLACE_IF_EXISTS = "replace_if_exists"
    DELETE = "delete"


# Verb → record_exists_action enforcement on the wire. Same mapping is used
# by the single-key write path (string verbs from ``Session.insert/update/...``)
# and the batch path (:attr:`BatchOpType` values are the string verbs).
# `UPSERT` is the server default (no enforcement) and `DELETE` uses a
# different policy type, hence both are absent.
_OP_TYPE_TO_REA: dict[str, RecordExistsAction] = {
    "insert": RecordExistsAction.CREATE_ONLY,
    "update": RecordExistsAction.UPDATE_ONLY,
    "replace": RecordExistsAction.REPLACE,
    "replace_if_exists": RecordExistsAction.REPLACE_ONLY,
}


def _write_policy_for_op_type(op_type: BatchOpType) -> Optional[BatchWritePolicy]:
    """Build a per-key :class:`BatchWritePolicy` enforcing the verb's existence
    semantics, or ``None`` for ``UPSERT`` / ``DELETE`` (server default already
    matches / delete uses a different policy).
    """
    rea = _OP_TYPE_TO_REA.get(op_type.value)
    if rea is None:
        return None
    wp = BatchWritePolicy()
    wp.record_exists_action = rea
    return wp


# TTL sentinels — match the signed-int convention the server uses on the wire.
_TTL_NEVER_EXPIRE = -1
_TTL_DONT_UPDATE = -2
_TTL_SERVER_DEFAULT = 0


def _to_expiration(ttl: int) -> Expiration:
    """Convert an integer TTL value to an ``Expiration`` object."""
    if ttl == _TTL_NEVER_EXPIRE:
        return Expiration.NEVER_EXPIRE
    if ttl == _TTL_DONT_UPDATE:
        return Expiration.DONT_UPDATE
    if ttl == _TTL_SERVER_DEFAULT:
        return Expiration.NAMESPACE_DEFAULT
    return Expiration.seconds(ttl)


class _WriteVerbs:
    """Mixin exposing write verbs that open a :class:`WriteSegmentBuilder`.

    Implemented on :class:`QueryBuilder` (chain from a read query) and
    :class:`WriteBinBuilder` (chain from a bin-scoped write). Each method
    finalizes the prior segment when applicable and targets new key(s).
    """

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        raise NotImplementedError

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open a create-or-update segment for the given key(s).

        Example::
            await session.upsert(key).put({"name": "Bob"}).execute()

        Returns:
            :class:`WriteSegmentBuilder` for ``put`` / ``bin`` / ``execute``.

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.upsert`: Session entry point.
        """
        return self._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open a create-only segment (fails if the record exists).

        Example::
            await session.insert(key).put({"name": "Ada"}).execute()

        Returns:
            :class:`WriteSegmentBuilder` for further bins and :meth:`execute`.
        """
        return self._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open an update-only segment (fails if the record is missing).

        Example::
            await session.update(key).bin("count").add(1).execute()

        Returns:
            :class:`WriteSegmentBuilder` for further bins and :meth:`execute`.
        """
        return self._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open a full replace segment (removes bins not written in this segment).

        Example::
            await session.replace(key).put({"a": 1}).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open replace-if-exists semantics (like replace, but only if the record exists).

        Example::
            await session.replace_if_exists(key).put({"a": 1}).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open a delete segment.

        Example::
            await session.delete(key).execute()

        Returns:
            :class:`WriteSegmentBuilder` (often followed immediately by :meth:`execute`).
        """
        return self._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open a touch segment (TTL refresh).

        Example::
            await session.touch(key).execute()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Open an exists-check segment.

        Example::
            stream = await session.exists(key).execute()
            found = (await stream.first_or_raise()).as_bool()

        Returns:
            :class:`WriteSegmentBuilder`.
        """
        return self._start_write_verb("exists", arg1, *more_keys)


def _build_pac_batch_ops(
    key_operations: List[Any],
    delete_policy: Optional[BatchDeletePolicy],
) -> List[Any]:
    """Translate the builder's accumulated per-key ops into PAC's pre-wrapped
    op list (:class:`BatchWriteOp` / :class:`BatchReadOp` /
    :class:`BatchDeleteOp`), one entry per key in input order.

    Each write carries a per-key :class:`BatchWritePolicy` derived from its
    verb (:func:`_write_policy_for_op_type`). Read-only op lists land as
    :class:`BatchReadOp` so PAC routes them via the read wire path —
    wrapping a read-only op list in :class:`BatchWriteOp` would force write
    semantics on the wire and the per-node batch group would error out.
    """
    ops: List[Any] = []
    for key_op in key_operations:
        if key_op._op_type == BatchOpType.DELETE:
            ops.append(BatchDeleteOp(key_op._key, policy=delete_policy))
            continue
        key_ops = key_op._operations.copy()
        if not key_ops and key_op._bins:
            for bin_name, value in key_op._bins.items():
                key_ops.append(Operation.put(bin_name, value))
        if not key_ops:
            key_ops.append(Operation.touch())
        if has_any_write_op(key_ops):
            ops.append(BatchWriteOp(
                key_op._key, key_ops,
                policy=_write_policy_for_op_type(key_op._op_type),
            ))
        else:
            ops.append(BatchReadOp(key_op._key, operations=key_ops))
    return ops


def _build_exp_write_flags(
    base: int,
    ignore_op_failure: bool,
    ignore_eval_failure: bool,
    delete_if_null: bool,
) -> int:
    """OR together ExpWriteFlags bitmask from boolean kwargs."""
    flags = base
    if ignore_op_failure:
        flags |= ExpWriteFlags.POLICY_NO_FAIL
    if ignore_eval_failure:
        flags |= ExpWriteFlags.EVAL_NO_FAIL
    if delete_if_null:
        flags |= ExpWriteFlags.ALLOW_DELETE
    return flags


class _WriteSegmentBuilderBase:
    """State + chaining shared by async and sync write-segment builders.

    Holds the wrapped query-builder reference (``_qb``) and the chaining
    methods that mutate state on it. Concrete subclasses
    (:class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder` for
    async, :class:`~aerospike_sdk.sync.operations.query.SyncWriteSegmentBuilder`
    for sync) add their respective ``execute()`` paths.

    Subclasses inject their tier-appropriate :class:`WriteBinBuilder` class
    via :attr:`_bin_builder_cls` (set at module load, after the concrete
    class is defined). The base's :meth:`bin` reads through that hook so it
    doesn't need a hard reference to a tier-specific module.
    """

    # Subclasses set this after class definition (concrete WriteBinBuilder
    # is tier-neutral but lives in aio.operations.query, so we avoid the
    # cross-tier reverse import).
    _bin_builder_cls: ClassVar[type] = None  # type: ignore[assignment]

    def __init__(self, qb: "QueryBuilder") -> None:
        self._qb = qb

    def with_txn(self, txn: Optional[Txn]) -> Self:
        """Opt this write into (or out of) a specific transaction.

        Delegates to the underlying query builder.

        Args:
            txn: The :class:`~aerospike_async.Txn` to participate in, or
                ``None`` to run without a transaction.

        Returns:
            This segment for chaining.
        """
        self._qb.with_txn(txn)
        return self

    @overload
    def where(self, expression: str) -> "WriteSegmentBuilder": ...

    @overload
    def where(self, expression: FilterExpression) -> "WriteSegmentBuilder": ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> Self:
        """Set a filter expression on the current write segment.

        Args:
            expression: AEL string or pre-built FilterExpression.

        Returns:
            self for method chaining.
        """
        if isinstance(expression, str):
            self._qb._filter_expression = parse_ael(expression)
        else:
            self._qb._filter_expression = expression
        return self

    def expire_record_after_seconds(self, seconds: int) -> Self:
        """Set the TTL on the current write segment.

        Args:
            seconds: Time-to-live in seconds (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If seconds is <= 0.
        """
        if seconds <= 0:
            raise ValueError("seconds must be greater than 0")
        self._qb._ttl_seconds = seconds
        return self

    def never_expire(self) -> Self:
        """Set this record to never expire (TTL = -1)."""
        self._qb._ttl_seconds = _TTL_NEVER_EXPIRE
        return self

    def with_no_change_in_expiration(self) -> Self:
        """Preserve the record's existing TTL (TTL = -2)."""
        self._qb._ttl_seconds = _TTL_DONT_UPDATE
        return self

    def expiry_from_server_default(self) -> Self:
        """Use the namespace's default TTL for this record (TTL = 0)."""
        self._qb._ttl_seconds = _TTL_SERVER_DEFAULT
        return self

    def ensure_generation_is(self, generation: int) -> Self:
        """Set expected generation for optimistic locking on the current segment.

        Args:
            generation: The expected generation number (must be > 0).

        Returns:
            self for method chaining.

        Raises:
            ValueError: If generation is <= 0.
        """
        if generation <= 0:
            raise ValueError("Generation must be greater than 0")
        self._qb._generation = generation
        return self

    def with_durable_delete(self) -> Self:
        """Enable durable delete on the current segment."""
        self._qb._durable_delete = True
        return self

    def default_with_durable_delete(self) -> Self:
        """Prefer durable deletes for this segment when resolving behavior defaults."""
        self._qb._durable_delete_command_default = True
        return self

    def default_without_durable_delete(self) -> Self:
        """Prefer non-durable deletes for this segment when resolving behavior defaults."""
        self._qb._durable_delete_command_default = False
        return self

    def without_durable_delete(self) -> Self:
        """Force a non-durable delete for this segment (may be rejected on SC)."""
        self._qb._durable_delete = False
        return self

    def respond_all_keys(self) -> Self:
        """Include results for missing keys in the stream."""
        self._qb._respond_all_keys = True
        return self

    def fail_on_filtered_out(self) -> Self:
        """Mark filtered-out records with ``FILTERED_OUT`` result code."""
        self._qb._fail_on_filtered_out = True
        return self

    def replace_only(self) -> Self:
        """Change the current segment to replace-if-exists semantics.

        The record must already exist; the operation fails with
        ``KEY_NOT_FOUND_ERROR`` if it does not. All existing bins are
        removed and only the bins specified in this segment are written.
        """
        self._qb._op_type = "replace_if_exists"
        return self

    def execute_blocking_fast_path(
        self,
        on_error: Optional[Any] = None,
    ) -> Optional[List[Any]]:
        """Try the blocking single-key fast path via the parent query builder.

        Mirrors :meth:`execute` but uses PAC ``_blocking``. Returns a list
        of :class:`~aerospike_sdk.record_result.RecordResult` on success,
        ``None`` when the spec shape isn't yet handled by the blocking
        dispatch. Raises a converted PAC exception on failure.
        """
        return self._qb.execute_blocking_fast_path(on_error)

    def bin(self, bin_name: str) -> "WriteBinBuilder":
        """Start a bin-level write operation.

        Args:
            bin_name: The bin to operate on.

        Returns:
            A WriteBinBuilder for method chaining.
        """
        return type(self)._bin_builder_cls(self, bin_name)

    def put(self, bins: dict) -> Self:
        """Apply ``Operation.put`` for each bin in the mapping.

        Args:
            bins: Map of bin name to value.

        Example::
            await session.upsert(key).put({"email": "a@b.com", "age": 30}).execute()

        See Also:
            :meth:`bin`: Per-bin CDT or scalar follow-ups.
        """
        for bin_name, value in bins.items():
            self._qb._operations.append(Operation.put(bin_name, value))
        return self

    def set_bins(self, bins: dict) -> Self:
        """Alias for :meth:`put`."""
        return self.put(bins)

    def _add_op(self, op: Any) -> Self:
        self._qb._operations.append(op)
        return self

    def add_operation(self, op: Any) -> Self:
        """Append an operation. Returns ``self`` so calls can chain."""
        self._qb._operations.append(op)
        return self

    def set_to(self, bin_name: str, value: Any) -> Self:
        """Set a bin to *value*."""
        return self._add_op(Operation.put(bin_name, value))

    def add(self, bin_name: str, value: Any) -> Self:
        """Add a numeric *value* to a bin."""
        return self._add_op(Operation.add(bin_name, value))

    def increment_by(self, bin_name: str, value: Any) -> Self:
        """Alias for :meth:`add`."""
        return self.add(bin_name, value)

    def get(self, bin_name: str) -> Self:
        """Read a bin value back within a write operate."""
        return self._add_op(Operation.get_bin(bin_name))

    def append(self, bin_name: str, value: str) -> Self:
        """Append a string to a bin."""
        return self._add_op(Operation.append(bin_name, value))

    def prepend(self, bin_name: str, value: str) -> Self:
        """Prepend a string to a bin."""
        return self._add_op(Operation.prepend(bin_name, value))

    def remove_bin(self, bin_name: str) -> Self:
        """Delete a bin from the record."""
        return self._add_op(Operation.put(bin_name, None))

    def delete_record(self) -> Self:
        """Add a record-level delete to the current operate call.

        Unlike :meth:`~_WriteVerbs.delete` which targets a different key,
        this deletes the record being operated on as part of the same
        atomic operation.

        Example::

            stream = await (
                session.upsert(key)
                    .bin("name").get()
                    .delete_record()
                    .execute()
            )

        See Also:
            :meth:`~_WriteVerbs.delete`: Start a new delete segment for a key.
        """
        self._qb._record_delete_in_operations = True
        return self._add_op(Operation.delete())

    def touch_record(self) -> Self:
        """Add a record-level touch to the current operate call.

        Resets the record's TTL as part of an atomic multi-operation call.
        Combine with :meth:`expire_record_after_seconds` to set a new TTL.

        Example::

            stream = await (
                session.upsert(key)
                    .bin("score").get()
                    .touch_record()
                    .expire_record_after_seconds(120)
                    .execute()
            )

        See Also:
            :meth:`~_WriteVerbs.touch`: Start a new touch segment for a key.
        """
        return self._add_op(Operation.touch())

    def select_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> Self:
        """Read a computed value into a bin using an AEL expression."""
        flags = ExpReadFlags.EVAL_NO_FAIL if ignore_eval_failure else ExpReadFlags.DEFAULT
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.read(bin_name, expr, flags))

    def insert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> Self:
        """Write expression result only if bin does not already exist."""
        flags = _build_exp_write_flags(
            ExpWriteFlags.CREATE_ONLY, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    def update_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> Self:
        """Write expression result only if bin already exists."""
        flags = _build_exp_write_flags(
            ExpWriteFlags.UPDATE_ONLY, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    def upsert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> Self:
        """Write expression result, creating or overwriting the bin."""
        flags = _build_exp_write_flags(
            ExpWriteFlags.DEFAULT, ignore_op_failure,
            ignore_eval_failure, delete_if_null,
        )
        expr = parse_ael(expression) if isinstance(expression, str) else expression
        return self._add_op(ExpOperation.write(bin_name, expr, flags))

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> "QueryBuilder":
        """Finalize current write segment and start a read segment.

        Args:
            arg1: A single Key or List[Key].
            *more_keys: Additional keys (varargs).

        Returns:
            The parent QueryBuilder for method chaining.
        """
        self._qb._finalize_current_spec()
        self._qb._op_type = None
        self._qb._set_current_keys(arg1, *more_keys)
        return self._qb

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> Self:
        self._qb._finalize_current_spec()
        self._qb._op_type = op_type
        self._qb._set_current_keys(arg1, *more_keys)
        return self


# Single-key write verbs whose "missing record" outcome is a real error
# (not a silent KEY_NOT_FOUND no-op). Consulted by the fast-path error
# converter in :class:`_SingleKeyWriteSegmentBase._handle_fast_error`.
_FAST_WRITES_REQUIRING_KEY = frozenset({"update", "replace_if_exists"})


class _SingleKeyWriteSegmentBase(_WriteSegmentBuilderBase):
    """Shared fast-path state + promote-delegate logic for single-key segments.

    Holds the fast-path slot fields (``_client_fast``, ``_key``,
    ``_op_type_fast``, ``_ops``, durable-delete overrides, MRT plumbing) and
    the methods that toggle between fast-path mutation and promoted
    delegation. The concrete ``_promote()`` implementation differs per
    subclass: the async :class:`_SingleKeyWriteSegment` constructs a
    :class:`QueryBuilder`; ``SyncSingleKeyWriteSegment`` constructs a
    ``SyncQueryBuilder``.

    Subclasses:
        - :class:`~aerospike_sdk.aio.operations.query._SingleKeyWriteSegment`:
          ``_promote()`` constructs a :class:`QueryBuilder`; async ``execute()``.
        - :class:`~aerospike_sdk.sync.operations.query.SyncSingleKeyWriteSegment`:
          ``_promote()`` constructs a ``SyncQueryBuilder``; sync ``execute()``.

    Private class — never seen by end users; constructed by the session
    when a single-key write verb is invoked.
    """

    # Class-level defaults for fields that are always None/False at
    # construction time. Reads fall through to these until a chained
    # method (`.with_durable_delete()`, `.delete_record()`, etc.) assigns
    # an instance attribute that shadows the class default. Eliminates 3
    # attribute writes per `_SingleKeyWriteSegment(...)` construction
    # call — pure savings on the bench upsert/insert/etc hot path.
    _dd_command_default: Optional[bool] = None
    _dd_override: Optional[bool] = None
    _record_delete_in_fast_ops: bool = False

    def _promote(self) -> None:
        """Lift the fast-path state into a full builder. Subclasses override.

        The base implementation raises — concrete subclasses
        (:class:`~aerospike_sdk.aio.operations.query._SingleKeyWriteSegment`
        and ``SyncSingleKeyWriteSegment``) override.
        """
        raise NotImplementedError("subclass must implement _promote")

    def __init__(
        self,
        client: Client,
        key: Key,
        op_type: str,
        behavior: Any,
        write_policy: Optional[WritePolicy],
        read_policy: Optional[ReadPolicy] = None,
        txn: Optional[Txn] = None,
        namespace_mode_resolver: NamespaceModeResolver = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], Mode]] = None,
        write_policy_sc: Optional[WritePolicy] = None,
        read_policy_sc: Optional[ReadPolicy] = None,
    ) -> None:
        self._qb = None  # type: ignore[assignment]
        self._client_fast = client
        self._key = key
        self._op_type_fast = op_type
        self._ops: list[Any] = []
        # Under MRT we can't reuse the session's cached write/read policies
        # (they were built without a txn), so null them here and force the
        # fast path to derive fresh policies from behavior on each execute.
        if txn is None:
            self._write_policy = write_policy
            self._read_policy = read_policy
            self._write_policy_sc = write_policy_sc
            self._read_policy_sc = read_policy_sc
        else:
            self._write_policy = None
            self._read_policy = None
            self._write_policy_sc = None
            self._read_policy_sc = None
        self._behavior_fast = behavior
        self._txn: Optional[Txn] = txn
        self._namespace_mode_resolver = namespace_mode_resolver
        self._namespace_mode_resolver_blocking = namespace_mode_resolver_blocking
        # _dd_command_default, _dd_override, _record_delete_in_fast_ops
        # are class-level defaults; reads fall through, chained-method
        # writes shadow.

    def _apply_txn(self, policy: Any) -> Any:
        """Stamp this segment's captured txn on an outer policy in place."""
        if self._txn is not None and policy is not None:
            policy.txn = self._txn
        return policy

    def with_txn(self, txn: Optional[Txn]) -> Self:
        """Opt this write into (or out of) a specific transaction."""
        self._txn = txn
        self._write_policy = None
        self._read_policy = None
        if self._qb is not None:
            self._qb.with_txn(txn)
        return self

    def put(self, bins: dict) -> Self:
        if self._qb is not None:
            return super().put(bins)
        ops = self._ops
        for bin_name, value in bins.items():
            ops.append(Operation.put(bin_name, value))
        return self

    def _add_op(self, op: Any) -> Self:
        if self._qb is not None:
            self._qb._operations.append(op)
        else:
            self._ops.append(op)
        return self

    def add_operation(self, op: Any) -> Self:
        """Append an operation. Returns ``self`` so calls can chain."""
        if self._qb is not None:
            self._qb._operations.append(op)
        else:
            self._ops.append(op)
        return self

    def replace_only(self) -> Self:
        if self._qb is not None:
            return super().replace_only()
        self._op_type_fast = "replace_if_exists"
        return self

    def delete_record(self) -> Self:
        if self._qb is not None:
            return super().delete_record()
        self._record_delete_in_fast_ops = True
        return self._add_op(Operation.delete())

    def default_with_durable_delete(self) -> Self:
        if self._qb is not None:
            return super().default_with_durable_delete()
        self._dd_command_default = True
        return self

    def default_without_durable_delete(self) -> Self:
        if self._qb is not None:
            return super().default_without_durable_delete()
        self._dd_command_default = False
        return self

    def without_durable_delete(self) -> Self:
        if self._qb is not None:
            return super().without_durable_delete()
        self._dd_override = False
        return self

    def with_durable_delete(self) -> Self:
        if self._qb is not None:
            return super().with_durable_delete()
        self._dd_override = True
        return self

    def where(self, expression):
        self._promote()
        return super().where(expression)

    def expire_record_after_seconds(self, seconds):
        self._promote()
        return super().expire_record_after_seconds(seconds)

    def never_expire(self):
        self._promote()
        return super().never_expire()

    def with_no_change_in_expiration(self):
        self._promote()
        return super().with_no_change_in_expiration()

    def expiry_from_server_default(self):
        self._promote()
        return super().expiry_from_server_default()

    def ensure_generation_is(self, generation):
        self._promote()
        return super().ensure_generation_is(generation)

    def respond_all_keys(self):
        self._promote()
        return super().respond_all_keys()

    def fail_on_filtered_out(self):
        self._promote()
        return super().fail_on_filtered_out()

    def query(self, arg1, *more_keys):
        self._promote()
        return super().query(arg1, *more_keys)

    def _start_write_verb(self, op_type, arg1, *more_keys):
        self._promote()
        return super()._start_write_verb(op_type, arg1, *more_keys)

    @staticmethod
    def _handle_fast_error(
        exc: Exception, op_type: str,
    ) -> RecordStream:
        pfc_exc = _convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        if rc == ResultCode.KEY_NOT_FOUND_ERROR:
            if op_type in _FAST_WRITES_REQUIRING_KEY:
                raise pfc_exc from exc
        elif rc != ResultCode.FILTERED_OUT:
            raise pfc_exc from exc
        return RecordStream.from_list([])

    def _get_write_policy(self) -> WritePolicy:
        wp = self._write_policy
        if wp is None and self._behavior_fast is not None:
            wp = self._apply_txn(to_write_policy(
                self._behavior_fast.get_settings(
                    OpKind.WRITE_NON_RETRYABLE, OpShape.POINT)))
            self._write_policy = wp
        return self._apply_txn(wp or WritePolicy())

    def execute_blocking_fast_path(
        self,
        on_error: "Optional[OnError]" = None,
    ) -> "Optional[List[RecordResult]]":
        """Blocking fast path. Promotes to a full builder first, then defers
        to the inherited :meth:`QueryBuilder.execute_blocking_fast_path`.

        Returns a list of :class:`RecordResult` on success; ``None`` when
        the spec shape isn't eligible (caller falls back to runner-driven).
        """
        if on_error is not None:
            # Fast path requires THROW disposition; bail.
            return None
        self._promote()
        return self._qb.execute_blocking_fast_path(on_error)  # type: ignore[union-attr]
