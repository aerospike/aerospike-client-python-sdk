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
# License for the specific language governing permissions and limitations
# under the License.

"""Foreground UDF execution builders (single-key, batch, and chained operations)."""

from __future__ import annotations

from typing import Any, List, Union, overload

from aerospike_async import FilterExpression, Key

from aerospike_sdk.aio.operations.query import QueryBuilder, WriteSegmentBuilder
from aerospike_sdk.ael.parser import parse_ael
from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.record_stream import RecordStream


class UdfFunctionBuilder:
    """First step of foreground UDF chaining: choose package and Lua function name.

    Produced by :meth:`~aerospike_sdk.aio.session.Session.execute_udf` or
    :meth:`UdfBuilder.execute_udf`. Call :meth:`function` before :meth:`UdfBuilder.passing`
    or :meth:`execute`.

    Example::

        stream = await (
            session.execute_udf(key)
                .function("my_module", "my_func")
                .execute()
        )

    """

    __slots__ = ("_qb",)

    def __init__(self, qb: QueryBuilder) -> None:
        self._qb = qb

    def function(self, package: str, function_name: str) -> UdfBuilder:
        """Select the registered module and function to invoke.

        Args:
            package: Server-side module name (no ``.lua`` suffix).
            function_name: Lua function symbol exported by the module.

        Returns:
            :class:`UdfBuilder` for arguments and execution.

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
        return UdfBuilder(self._qb)


class _UdfBuilderBase:
    """State + chaining shared by async and sync UdfBuilder.

    Methods migrate from :class:`UdfBuilder` during Phase 4 collapse.
    """
    def __init__(self, qb: QueryBuilder) -> None:
        self._qb = qb

    def passing(self, *args: Any) -> UdfBuilder:
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
    def where(self, expression: str) -> UdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> UdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> UdfBuilder:
        """Apply a filter expression so the UDF runs only when the predicate matches.

        Args:
            expression: AEL string or ``FilterExpression``.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`QueryBuilder.where`: Same AEL for reads.
        """
        if isinstance(expression, str):
            self._qb._filter_expression = parse_ael(expression)
        else:
            self._qb._filter_expression = expression
        return self

    def default_with_durable_delete(self) -> UdfBuilder:
        """Prefer durable deletes when resolving policy defaults."""
        self._qb._durable_delete_command_default = True
        return self

    def default_without_durable_delete(self) -> UdfBuilder:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._qb._durable_delete_command_default = False
        return self

    def with_durable_delete(self) -> UdfBuilder:
        """Force durable delete for this UDF invocation."""
        self._qb._durable_delete = True
        return self

    def without_durable_delete(self) -> UdfBuilder:
        """Force non-durable delete for this UDF invocation."""
        self._qb._durable_delete = False
        return self

    def respond_all_keys(self) -> UdfBuilder:
        """For batch UDF, emit a row per requested key (including not-found).

        Returns:
            This builder for chaining.

        See Also:
            :meth:`QueryBuilder.respond_all_keys`: Same flag for reads.
        """
        self._qb._respond_all_keys = True
        return self

    def execute_udf(self, *keys: Key) -> UdfFunctionBuilder:
        """Finalize this UDF operation and start another on *keys*.

        Args:
            *keys: One or more keys for the next UDF segment.

        Returns:
            A new :class:`UdfFunctionBuilder` to call :meth:`function` again.

        Raises:
            ValueError: If no keys are provided.
        """
        if not keys:
            raise ValueError("At least one key is required")
        self._qb._finalize_udf_spec()
        self._qb._set_current_keys_from_varargs(keys)
        return UdfFunctionBuilder(self._qb)

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> QueryBuilder:
        """Close the UDF operation and begin a read :class:`QueryBuilder` segment.

        Args:
            arg1: One key or a list of keys.
            *more_keys: Additional keys when ``arg1`` is a single key.

        Returns:
            :class:`QueryBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        self._qb._op_type = None
        self._qb._set_current_keys(arg1, *more_keys)
        return self._qb

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an upsert :class:`WriteSegmentBuilder`.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an insert-only write segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an update-only write segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a replace write segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a replace-if-exists segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a delete segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start a touch segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        """Finalize the UDF operation and start an exists-check segment.

        Returns:
            :class:`WriteSegmentBuilder` for chaining.
        """
        self._qb._finalize_udf_spec()
        return self._qb._start_write_verb("exists", arg1, *more_keys)



class UdfBuilder(_UdfBuilderBase):
    """Supply UDF arguments, optional filter, then execute or chain another operation.

    After :meth:`UdfFunctionBuilder.function`, call :meth:`passing` with values
    passed to Lua (after the implicit record argument). Use :meth:`execute_udf`
    to append another UDF segment, or :meth:`query` / write verbs to switch
    operation type. Await :meth:`execute` to run the accumulated chain.

    Example::

        stream = await (
            session.execute_udf(key)
                .function("my_pkg", "my_func")
                .passing(1, "x")
                .execute()
        )

    See Also:
        :meth:`~aerospike_sdk.aio.session.Session.execute_udf`: Entry point.
    """

    __slots__ = ("_qb",)





















    async def execute(self, on_error: OnError | None = None) -> RecordStream:
        """Run the current builder state and return a :class:`~aerospike_sdk.record_stream.RecordStream`.

        Requires :meth:`function` to have been called for the pending UDF operation.

        Args:
            on_error: Same as :meth:`QueryBuilder.execute`.

        Returns:
            Stream of per-key results and optional ``udf_result`` fields.

        Example::

            stream = await (
                session.execute_udf(k1, k2)
                    .function("pkg", "fn")
                    .execute()
            )

        Raises:
            ValueError: If no UDF function was selected before execute.
        """
        if self._qb._udf_function is None:
            raise ValueError(
                "function(package, name) must be called before execute()",
            )
        self._qb._finalize_udf_spec()
        return await self._qb.execute(on_error)
