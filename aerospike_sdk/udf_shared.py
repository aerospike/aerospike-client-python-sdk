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

"""Shared UDF helpers and runtime-agnostic foreground-UDF builder bases.

Holds the ``udf-list`` info-response parser used by both admin surfaces,
plus the chain state and chaining methods shared by the async and sync
foreground UDF builders — no I/O. Terminal ``execute()`` dispatchers are
runtime-bound and live on the leaves:
:mod:`aerospike_sdk.aio.operations.udf` (async) and
:mod:`aerospike_sdk.sync.operations.udf` (blocking).
"""

from __future__ import annotations

from typing import Any, ClassVar, Generic, List, TYPE_CHECKING, TypeVar, Union, overload

from typing import Self

from aerospike_async import FilterExpression, Key

from aerospike_sdk.operations_shared import _ExpirationVerbs
from aerospike_sdk.server_filter import bind_ael_params

if TYPE_CHECKING:  # Forward-reference only; the concrete classes live in aio.
    from aerospike_sdk.aio.operations.query import WriteSegmentBuilder
    from aerospike_sdk.aio.operations.udf import UdfBuilder, UdfFunctionBuilder
    from aerospike_sdk.query_shared import _QueryBuilderBase

# Each UDF-builder leaf binds this to its tree's concrete ``QueryBuilder`` so the
# wrapped ``_qb`` keeps its runtime-appropriate type instead of a hard-coded tree's.
_QB = TypeVar("_QB", bound="_QueryBuilderBase")


def parse_udf_list(raw: str) -> list[dict[str, str]]:
    """Parse a server ``udf-list`` info response into module descriptors.

    The server returns modules as ``filename=<name>,hash=<sha>,type=<lang>``
    entries joined by ``;`` (empty when nothing is registered). Each entry
    becomes a dict with ``name`` / ``hash`` / ``type`` keys.
    """
    modules: list[dict[str, str]] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        fields: dict[str, str] = {}
        for kv in entry.split(","):
            key, sep, value = kv.partition("=")
            if sep:
                fields[key.strip()] = value.strip()
        modules.append(
            {
                # server uses `filename`; `module` kept as a defensive fallback
                "name": fields.get("filename") or fields.get("module", ""),
                "hash": fields.get("hash", ""),
                "type": fields.get("type", ""),
            }
        )
    return modules


class _UdfFunctionBuilderBase(Generic[_QB]):
    """State + chaining shared by the async and sync UdfFunctionBuilder.

    Subclasses inject their tier-appropriate ``UdfBuilder`` class via
    :attr:`_udf_builder_cls` (set at module load, after the concrete class
    is defined) so :meth:`function` stays runtime-agnostic.
    """

    __slots__ = ("_qb",)

    # Leaf modules bind this after their concrete UdfBuilder is defined.
    _udf_builder_cls: ClassVar[type]

    def __init__(self, qb: _QB) -> None:
        self._qb: _QB = qb

    def function(self, package: str, function_name: str) -> UdfBuilder:
        """Select the registered module and function to invoke.

        Args:
            package: Server-side module name (no ``.lua`` suffix).
            function_name: Lua function symbol exported by the module.

        Returns:
            A ``UdfBuilder`` for arguments and execution.

        Raises:
            ValueError: If ``package`` or ``function_name`` is empty.
        """
        if not package:
            raise ValueError("package must be a non-empty string")
        if not function_name:
            raise ValueError("function_name must be a non-empty string")
        self._qb._udf_package = package
        self._qb._udf_function = function_name
        self._qb._udf_args = None
        self._qb._op_type = "udf"
        return type(self)._udf_builder_cls(self._qb)


class _UdfBuilderBase(_ExpirationVerbs[_QB]):
    """State + chaining shared by the async and sync UdfBuilder.

    Subclasses inject their tier-appropriate ``UdfFunctionBuilder`` class
    via :attr:`_udf_function_builder_cls` so :meth:`execute_udf` stays
    runtime-agnostic. Write-verb and ``query`` transitions delegate to the
    wrapped query builder, whose overrides already return the right tier's
    segment/builder types.

    Inherits the record-expiration verbs (:meth:`expire_record_after_seconds`
    and siblings) so a UDF apply can set the record TTL, which is carried into
    the batch/point apply policy's ``expiration``.
    """

    __slots__ = ("_qb",)

    # Leaf modules bind this after their concrete UdfFunctionBuilder is defined.
    _udf_function_builder_cls: ClassVar[type]

    def __init__(self, qb: _QB) -> None:
        self._qb: _QB = qb

    def passing(self, *args: Any) -> Self:
        """Set positional arguments forwarded to the Lua function.

        The Aerospike server automatically passes the record as the first
        argument to the UDF; values provided here follow it.

        Args:
            *args: Values serialized by the async client (scalars, lists, maps, bytes).

        Returns:
            This builder for chaining.

        Example::
            builder.passing("binName", 42)
        """
        self._qb._udf_args = list(args)
        return self

    @overload
    def where(self, expression: str, *params: Any) -> Self: ...

    @overload
    def where(self, expression: FilterExpression) -> Self: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> Self:
        """Apply a filter expression so the UDF runs only when the predicate matches.

        Args:
            expression: AEL string or ``FilterExpression``.
            *params: Values for printf placeholders in an AEL template. See
                :meth:`QueryBuilder.where` for the interpolation contract.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`QueryBuilder.where`: Same AEL for reads.
        """
        expression = bind_ael_params(expression, params)
        if isinstance(expression, str):
            self._qb._filter_expression = self._qb._filter_expression_from_ael(expression)
        else:
            self._qb._filter_expression = expression
        return self

    def default_with_durable_delete(self) -> Self:
        """Prefer durable deletes when resolving policy defaults."""
        self._qb._durable_delete_command_default = True
        return self

    def default_without_durable_delete(self) -> Self:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._qb._durable_delete_command_default = False
        return self

    def with_durable_delete(self) -> Self:
        """Force durable delete for this UDF invocation."""
        self._qb._durable_delete = True
        return self

    def without_durable_delete(self) -> Self:
        """Force non-durable delete for this UDF invocation."""
        self._qb._durable_delete = False
        return self

    def include_missing_keys(self) -> Self:
        """For batch UDF, emit a row per requested key (including not-found).

        Returns:
            This builder for chaining.

        See Also:
            :meth:`QueryBuilder.include_missing_keys`: Same flag for reads.
            :meth:`respond_all_keys`: Alias using the underlying client's name.
        """
        self._qb._respond_all_keys = True
        return self

    def respond_all_keys(self) -> Self:
        """Alias for :meth:`include_missing_keys` (underlying client's ``respondAllKeys`` name)."""
        return self.include_missing_keys()

    def execute_udf(self, *keys: Key) -> UdfFunctionBuilder:
        """Finalize this UDF operation and start another on *keys*.

        Args:
            *keys: One or more keys for the next UDF segment.

        Returns:
            A new ``UdfFunctionBuilder`` to call ``function`` again.

        Raises:
            ValueError: If no keys are provided.
        """
        if not keys:
            raise ValueError("At least one key is required")
        self._qb._finalize_udf_spec()
        self._qb._set_current_keys_from_varargs(keys)
        return type(self)._udf_function_builder_cls(self._qb)

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> _QB:
        """Close the UDF operation and begin a read query segment.

        Args:
            arg1: One key or a list of keys.
            *more_keys: Additional keys when ``arg1`` is a single key.

        Returns:
            The wrapped query builder for chaining.
        """
        self._qb._finalize_udf_spec()
        self._qb._op_type = None
        self._qb._set_current_keys(arg1, *more_keys)
        return self._qb

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an upsert write segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an insert-only write segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an update-only write segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a replace write segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a replace-if-exists segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a delete segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a touch segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an exists-check segment.

        Returns:
            A write-segment builder for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("exists", arg1, *more_keys)
