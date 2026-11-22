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

"""Selectors: the operations an ``on`` entry of a Behavior applies to."""

from __future__ import annotations

import dataclasses
from typing import Callable, FrozenSet, List, Optional, Tuple

from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Scope, Settings

_WRITE_KINDS: FrozenSet[OpKind] = frozenset({OpKind.WRITE_RETRYABLE, OpKind.WRITE_NON_RETRYABLE})
_OP_KEYS: Tuple[Tuple[OpKind, OpShape, Mode], ...] = tuple(
    (kind, shape, mode) for kind in OpKind for shape in OpShape for mode in Mode
)
_TXN_SCOPES: Tuple[Scope, Scope] = (Scope.SYSTEM_TXN_VERIFY, Scope.SYSTEM_TXN_ROLL)


class Selector:
    """A set of operations, named by kind, shape and namespace mode.

    Paired with a :class:`~aerospike_sdk.policy.behavior_settings.Settings`
    in the ``on`` argument of
    :meth:`~aerospike_sdk.policy.behavior.Behavior.derive_with_changes`, a
    selector says which operations those settings apply to. Start from one
    of the class methods, then narrow: each narrowing method returns a new
    selector, and the order of the calls does not matter.

    Selectors are immutable and compare equal when they select the same
    operations. They are built only through the class methods here.

    Example::

        from aerospike_sdk import Behavior
        from aerospike_sdk.policy import CommitLevel, Selector, Settings

        critical_writes = Behavior.DEFAULT.derive_with_changes(
            "critical-writes",
            on=[
                (Selector.writes().retryable().point().ap(),
                 Settings(max_retries=4, commit_level=CommitLevel.COMMIT_ALL)),
            ],
        )

    See Also:
        :meth:`~aerospike_sdk.policy.behavior.Behavior.derive_with_changes`:
            Where selectors are used, and the order entries apply in.
    """

    __slots__ = ("_kinds", "_shape", "_mode", "_txn")

    def __init__(
        self,
        _kinds: Optional[FrozenSet[OpKind]],
        _shape: Optional[OpShape],
        _mode: Optional[Mode],
        _txn: Optional[Scope],
    ) -> None:
        object.__setattr__(self, "_kinds", _kinds)
        object.__setattr__(self, "_shape", _shape)
        object.__setattr__(self, "_mode", _mode)
        object.__setattr__(self, "_txn", _txn)

    @classmethod
    def all(cls) -> Selector:
        """Select every operation, and both transaction phases.

        Returns:
            A selector that takes no narrowing.

        Example::

            on=[(Selector.all(), Settings(max_retries=1))]
        """
        return Selector(None, None, None, None)

    @classmethod
    def reads(cls) -> ReadSelector:
        """Select reads: point reads, batch reads and queries.

        Returns:
            A :class:`ReadSelector`, which narrows by shape and mode.

        Example::

            on=[(Selector.reads().batch().ap(), Settings(max_retries=2))]
        """
        return ReadSelector(frozenset({OpKind.READ}), None, None, None)

    @classmethod
    def writes(cls) -> WriteSelector:
        """Select writes, retryable or not.

        Returns:
            A :class:`WriteSelector`, which narrows by retryability, shape
            and mode.

        Example::

            on=[(Selector.writes().retryable().point().ap(), Settings(max_retries=4))]
        """
        return WriteSelector(_WRITE_KINDS, None, None, None)

    @classmethod
    def txn_verify(cls) -> Selector:
        """Select the transaction verify phase.

        Returns:
            A selector that takes no narrowing.

        Example::

            on=[(Selector.txn_verify(), Settings(total_timeout=timedelta(seconds=30)))]

        See Also:
            :meth:`txn_roll`: The roll-forward and roll-back phase.
        """
        return Selector(frozenset(), None, None, Scope.SYSTEM_TXN_VERIFY)

    @classmethod
    def txn_roll(cls) -> Selector:
        """Select the transaction roll-forward and roll-back phase.

        Returns:
            A selector that takes no narrowing.

        Example::

            on=[(Selector.txn_roll(), Settings(max_retries=5))]

        See Also:
            :meth:`txn_verify`: The verify phase.
        """
        return Selector(frozenset(), None, None, Scope.SYSTEM_TXN_ROLL)

    def _matches(self, kind: OpKind, shape: OpShape, mode: Mode) -> bool:
        return (
            (self._kinds is None or kind in self._kinds)
            and (self._shape is None or shape is self._shape)
            and (self._mode is None or mode is self._mode)
        )

    def _matches_txn(self, txn_scope: Scope) -> bool:
        return self._kinds is None or self._txn is txn_scope

    def _narrowed(self, method: str, **axes: object) -> Selector:
        for axis in axes:
            if axis == "_kinds":
                taken = self._kinds != _WRITE_KINDS
                what = "retryability"
            else:
                taken = getattr(self, axis) is not None
                what = axis[1:]
            if taken:
                raise ValueError(
                    f"{self} already selects a {what}; it cannot be narrowed with {method}()"
                )
        state = {name: getattr(self, name) for name in Selector.__slots__}
        state.update(axes)
        return type(self)(**state)

    def _key(self) -> tuple:
        return (self._kinds, self._shape, self._mode, self._txn)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Selector) and self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("Selector is immutable")

    def __str__(self) -> str:
        if self._txn is Scope.SYSTEM_TXN_VERIFY:
            return "txn_verify()"
        if self._txn is Scope.SYSTEM_TXN_ROLL:
            return "txn_roll()"
        if self._kinds is None:
            return "all()"
        if self._kinds == frozenset({OpKind.READ}):
            parts = ["reads()"]
        else:
            parts = ["writes()"]
            if self._kinds == frozenset({OpKind.WRITE_RETRYABLE}):
                parts.append("retryable()")
            elif self._kinds == frozenset({OpKind.WRITE_NON_RETRYABLE}):
                parts.append("non_retryable()")
        if self._shape is not None:
            parts.append(f"{self._shape.value}()")
        if self._mode is not None:
            parts.append(f"{self._mode.value}()")
        return ".".join(parts)

    def __repr__(self) -> str:
        return f"Selector.{self}"


class ReadSelector(Selector):
    """A selector over reads, returned by :meth:`Selector.reads`.

    Narrow it by shape (:meth:`point`, :meth:`batch`, :meth:`query`) and by
    namespace mode (:meth:`ap`, :meth:`sc`), each at most once.

    Example::

        fast_batch_reads = Behavior.DEFAULT.derive_with_changes(
            "fast-batch-reads",
            on=[(Selector.reads().batch().ap(), Settings(max_concurrent_nodes=8))],
        )

    See Also:
        :class:`WriteSelector`: The selector over writes.
    """

    __slots__ = ()

    def point(self) -> ReadSelector:
        """Narrow to single-key reads.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("point", _shape=OpShape.POINT)  # type: ignore[return-value]

    def batch(self) -> ReadSelector:
        """Narrow to batch reads.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("batch", _shape=OpShape.BATCH)  # type: ignore[return-value]

    def query(self) -> ReadSelector:
        """Narrow to queries.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("query", _shape=OpShape.QUERY)  # type: ignore[return-value]

    def ap(self) -> ReadSelector:
        """Narrow to AP (availability) namespaces.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a mode is already selected.
        """
        return self._narrowed("ap", _mode=Mode.AP)  # type: ignore[return-value]

    def sc(self) -> ReadSelector:
        """Narrow to SC (strong-consistency) namespaces.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a mode is already selected.
        """
        return self._narrowed("sc", _mode=Mode.SC)  # type: ignore[return-value]


class WriteSelector(Selector):
    """A selector over writes, returned by :meth:`Selector.writes`.

    Narrow it by retryability (:meth:`retryable`, :meth:`non_retryable`), by
    shape (:meth:`point`, :meth:`batch`, :meth:`query`) and by namespace mode
    (:meth:`ap`, :meth:`sc`), each at most once.

    Example::

        patient_point_writes = Behavior.DEFAULT.derive_with_changes(
            "patient-point-writes",
            on=[(Selector.writes().retryable().point(), Settings(max_retries=5))],
        )

    See Also:
        :class:`ReadSelector`: The selector over reads.
    """

    __slots__ = ()

    def retryable(self) -> WriteSelector:
        """Narrow to retryable writes: those whose every operation is idempotent.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a retryability is already selected.
        """
        return self._narrowed(  # type: ignore[return-value]
            "retryable", _kinds=frozenset({OpKind.WRITE_RETRYABLE}),
        )

    def non_retryable(self) -> WriteSelector:
        """Narrow to non-retryable writes.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a retryability is already selected.
        """
        return self._narrowed(  # type: ignore[return-value]
            "non_retryable", _kinds=frozenset({OpKind.WRITE_NON_RETRYABLE}),
        )

    def point(self) -> WriteSelector:
        """Narrow to single-key writes.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("point", _shape=OpShape.POINT)  # type: ignore[return-value]

    def batch(self) -> WriteSelector:
        """Narrow to batch writes.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("batch", _shape=OpShape.BATCH)  # type: ignore[return-value]

    def query(self) -> WriteSelector:
        """Narrow to background query writes.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a shape is already selected.
        """
        return self._narrowed("query", _shape=OpShape.QUERY)  # type: ignore[return-value]

    def ap(self) -> WriteSelector:
        """Narrow to AP (availability) namespaces.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a mode is already selected.
        """
        return self._narrowed("ap", _mode=Mode.AP)  # type: ignore[return-value]

    def sc(self) -> WriteSelector:
        """Narrow to SC (strong-consistency) namespaces.

        Returns:
            The narrowed selector; this one is unchanged.

        Raises:
            ValueError: If a mode is already selected.
        """
        return self._narrowed("sc", _mode=Mode.SC)  # type: ignore[return-value]


# -- Field applicability ------------------------------------------------------

_CellTest = Callable[[OpKind, OpShape, Mode], bool]


def _is_write(kind: OpKind) -> bool:
    return kind in _WRITE_KINDS


def _reads_or_batch_writes(kind: OpKind, shape: OpShape) -> bool:
    # A batch that carries writes resolves as a write, yet its parent batch
    # policy carries the routing fields for every row.
    return kind is OpKind.READ or shape is OpShape.BATCH


# (fields, where they apply, matrix cells, transaction phases). A field absent
# from here applies to every cell. Each row follows which policy builders read
# the field; the applicability unit test holds the table to those builders.
_FIELD_DOMAINS: List[Tuple[Tuple[str, ...], str, _CellTest, FrozenSet[Scope]]] = [
    (
        ("timeout_delay", "use_compression", "compression_threshold", "error_detail_verbosity"),
        "every operation, but not the transaction phases",
        lambda kind, shape, mode: True,
        frozenset(),
    ),
    (
        ("commit_level",),
        "writes on AP namespaces",
        lambda kind, shape, mode: _is_write(kind) and mode is Mode.AP,
        frozenset(),
    ),
    (
        ("durable_delete", "send_key"),
        "writes",
        lambda kind, shape, mode: _is_write(kind),
        frozenset(),
    ),
    (
        ("read_mode_ap",),
        "reads and batch writes on AP namespaces",
        lambda kind, shape, mode: _reads_or_batch_writes(kind, shape) and mode is Mode.AP,
        frozenset(),
    ),
    (
        ("read_mode_sc",),
        "reads and batch writes on SC namespaces, and the transaction verify phase",
        lambda kind, shape, mode: _reads_or_batch_writes(kind, shape) and mode is Mode.SC,
        frozenset({Scope.SYSTEM_TXN_VERIFY}),
    ),
    (
        ("read_touch_ttl_percent",),
        "point and batch reads",
        lambda kind, shape, mode: kind is OpKind.READ and shape is not OpShape.QUERY,
        frozenset(),
    ),
    (
        ("replica",),
        "reads, batch writes, and the transaction phases",
        lambda kind, shape, mode: _reads_or_batch_writes(kind, shape),
        frozenset(_TXN_SCOPES),
    ),
    (
        ("allow_inline", "allow_inline_ssd"),
        "batch reads and writes",
        lambda kind, shape, mode: shape is OpShape.BATCH,
        frozenset(),
    ),
    (
        ("max_concurrent_nodes",),
        "batch reads and writes, and query reads",
        lambda kind, shape, mode: (
            shape is OpShape.BATCH or (kind is OpKind.READ and shape is OpShape.QUERY)
        ),
        frozenset(),
    ),
    (
        ("record_queue_size", "allow_scans_with_where"),
        "query reads",
        lambda kind, shape, mode: kind is OpKind.READ and shape is OpShape.QUERY,
        frozenset(),
    ),
]

_DOMAIN_OF = {
    field: (where, applies, txn_phases)
    for fields, where, applies, txn_phases in _FIELD_DOMAINS
    for field in fields
}


def _check_applicable(selector: Selector, settings: Settings) -> None:
    """Raise when *settings* sets a field that reaches none of *selector*'s operations."""
    for field in dataclasses.fields(settings):
        domain = _DOMAIN_OF.get(field.name)
        if domain is None or getattr(settings, field.name) is None:
            continue
        where, applies, txn_phases = domain
        covered = any(
            selector._matches(*key) and applies(*key) for key in _OP_KEYS
        ) or any(selector._matches_txn(scope) for scope in txn_phases)
        if not covered:
            raise ValueError(
                f"{field.name} does not apply to {selector}: it applies to {where}"
            )
