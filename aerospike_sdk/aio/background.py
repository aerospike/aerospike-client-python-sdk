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

"""Chainable builders for server-side background operations on datasets."""

from __future__ import annotations

import enum
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, List, Optional, Union, overload


from aerospike_native import (
    Client,
    ExecuteTask,
    Filter,
    FilterExpression,
    Operation,
    RecordExistsAction,
)


from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.background_shared import (
    dataset_statement,
    make_background_write_policy,
)
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.query_shared import _BACKGROUND_IN_TXN_ERROR, _BinWriteSteps, _W
from aerospike_sdk.server_filter import bind_ael_params, filter_expression_from_ael_string
from aerospike_sdk.exceptions import _convert_pnc_exception
from aerospike_sdk.metrics import usage
from aerospike_sdk.operations_shared import (
    _TTL_DONT_UPDATE,
    _TTL_NEVER_EXPIRE,
    _TTL_SERVER_DEFAULT,
    _seconds_from_timedelta,
    _seconds_until,
)

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.aio.session import Session

log = logging.getLogger(SdkLoggers.BACKGROUND)


class _OpType(enum.Enum):
    UPDATE = enum.auto()
    DELETE = enum.auto()
    TOUCH = enum.auto()


_BG_UNSUPPORTED = (
    "fail_on_filtered_out and include_missing_keys apply to foreground reads; "
    "they are not supported for background tasks."
)


class BackgroundTaskSession:
    """Choose a dataset-wide background job (update, delete, touch, or UDF).

    From :meth:`~aerospike_sdk.aio.session.Session.background_task`. Each
    method returns a builder to add filters, bin operations or UDF arguments,
    then ``await ...execute()`` for a server :class:`~aerospike_native.ExecuteTask`.

    Example::

        Background update with a filter::

            task = await (
                session.background_task()
                .update(users)
                .where("$.active == true")
                .bin("score").add(1)
                .execute()
            )

    Background tasks cannot join a transaction: the server applies them outside
    any transaction, so their writes would escape its commit and abort.

    See Also:
        :meth:`~aerospike_sdk.aio.session.Session.execute_udf`: Foreground UDF on keys.
    """

    def __init__(self, session: Session) -> None:
        """Bind to *session*; prefer :meth:`Session.background_task`.

        Raises:
            RuntimeError: If *session* has an active transaction.
        """
        if session.current_transaction is not None:
            raise RuntimeError(_BACKGROUND_IN_TXN_ERROR)
        self._session = session

    def update(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Start a ``query_operate`` update over records in *dataset*.

        Args:
            dataset: Namespace/set scope for the scan.

        Returns:
            :class:`BackgroundOperationBuilder` — add ``where``, ``bin``, then
            :meth:`BackgroundOperationBuilder.execute`.

        Raises:
            ValueError: On execute if no bin operations were added.
        """
        return BackgroundOperationBuilder(self._session, dataset, _OpType.UPDATE)

    def delete(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Start a background delete of all records matching optional filters.

        Args:
            dataset: Namespace/set to scan.

        Returns:
            :class:`BackgroundOperationBuilder` (no bin ops required for delete).
        """
        return BackgroundOperationBuilder(self._session, dataset, _OpType.DELETE)

    def touch(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Start a background touch (TTL refresh) for matching records.

        Args:
            dataset: Namespace/set to scan.

        Returns:
            :class:`BackgroundOperationBuilder` — optional ``expire_record_after_seconds``.
        """
        return BackgroundOperationBuilder(self._session, dataset, _OpType.TOUCH)

    def execute_udf(self, dataset: DataSet) -> BackgroundUdfFunctionBuilder:
        """Start a background UDF executed via ``query_execute_udf``.

        Args:
            dataset: Namespace/set scope.

        Returns:
            :class:`BackgroundUdfFunctionBuilder` — call :meth:`BackgroundUdfFunctionBuilder.function`
            then :meth:`BackgroundUdfBuilder.passing` and :meth:`BackgroundUdfBuilder.execute`.
        """
        return BackgroundUdfFunctionBuilder(self._session, dataset)


class BackgroundWriteBinBuilder(_BinWriteSteps[_W]):
    """Per-bin write steps for a background update job.

    Obtained from :meth:`BackgroundOperationBuilder.bin` (or its sync
    counterpart). Offers the same steps as
    :class:`~aerospike_sdk.aio.operations.query.WriteBinBuilder`: scalar and
    expression writes, and list, map, bit, HyperLogLog and string operations,
    including nested ``on_map_*`` / ``on_list_*`` navigation. Each step
    returns the parent builder for further chaining.

    The server applies the operations to every record the job matches. It
    rejects read operations in a background job, so read steps such as
    :meth:`get` fail when the job is submitted.

    Example::

        Remove every segment whose value list sorts before a cutoff::

            task = await (
                session.background_task()
                    .update(DataSet.of("test", "profiles"))
                    .bin("segments").on_map_value_range(None, [1704067200]).remove()
                    .execute()
            )
            await task.wait_till_complete()

    See Also:
        :meth:`BackgroundOperationBuilder.add_operation`: Append a prebuilt operation.
    """

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: _W, bin_name: str) -> None:
        """Capture the bin name; prefer :meth:`BackgroundOperationBuilder.bin`."""
        self._parent = parent
        self._bin = bin_name


class _BackgroundOperationBuilderBase:
    """State + chaining shared by async and sync BackgroundOperationBuilder.

    Methods migrate from :class:`BackgroundOperationBuilder` during Phase 4 collapse.
    """
    def __init__(
        self,
        session: Session,
        dataset: DataSet,
        op_type: _OpType,
    ) -> None:
        self._session = session
        self._dataset = dataset
        self._op_type = op_type
        self._operations: List[Any] = []
        self._filter_expression: Optional[FilterExpression] = None
        self._filter: Optional[Filter] = None
        self._ttl_seconds: Optional[int] = None
        self._records_per_second: Optional[int] = None
        self._durable_delete_command_default: Optional[bool] = None
        self._durable_delete_override: Optional[bool] = None
        self._supports_server_compiled_ael = bool(
            session._client.supports_server_compiled_ael,
        )

    def default_with_durable_delete(self) -> BackgroundOperationBuilder:
        """Prefer durable deletes when resolving policy defaults (SC namespaces)."""
        self._durable_delete_command_default = True
        return self

    def default_without_durable_delete(self) -> BackgroundOperationBuilder:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._durable_delete_command_default = False
        return self

    def with_durable_delete(self) -> BackgroundOperationBuilder:
        """Force durable delete on this background job."""
        self._durable_delete_override = True
        return self

    def without_durable_delete(self) -> BackgroundOperationBuilder:
        """Force non-durable deletes (may be rejected on SC)."""
        self._durable_delete_override = False
        return self

    @overload
    def where(self, expression: str, *params: Any) -> BackgroundOperationBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundOperationBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> BackgroundOperationBuilder:
        """Restrict the job with an AEL or ``FilterExpression`` predicate.

        The predicate travels as the write policy's filter expression and decides
        which records the job writes. It combines with :meth:`filter`, which
        selects the candidates through a secondary index; without one, the server
        reaches the records by scanning the set.

        Args:
            expression: AEL string or ``FilterExpression``.
            *params: Values for printf placeholders in an AEL template. See
                :meth:`~aerospike_sdk.query_shared._QueryBuilderBase.where`
                for the interpolation contract.

        Returns:
            This builder for chaining.

        Raises:
            ValueError: If ``where`` has already been called on this builder.

        Example::

            await session.background_task().delete(users).where(
                "$.status == '%s'", status,
            ).execute()
        """
        if self._filter_expression is not None:
            raise ValueError("where() can only be called once per background task")
        expression = bind_ael_params(expression, params)
        if isinstance(expression, str):
            self._filter_expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=self._supports_server_compiled_ael,
            )
        else:
            self._filter_expression = expression
        return self

    def filter(self, filter_obj: Filter) -> BackgroundOperationBuilder:
        """Restrict the job with a secondary-index :class:`~aerospike_native.Filter`.

        The filter attaches to the query ``Statement``, so the server reaches the
        records through the index instead of scanning the set. It combines with
        :meth:`where`, which travels separately as the write policy's filter
        expression: the index selects the candidates and the predicate decides
        which of them the job writes. Use both when the index covers part of the
        condition and an expression covers the rest. A job carries at most one
        filter, because the server accepts one index range per query.

        Args:
            filter_obj: The secondary-index filter (for example ``Filter.range``).

        Returns:
            This builder for chaining.

        Raises:
            TypeError: If ``filter_obj`` is ``None``.
            ValueError: If ``filter`` has already been called on this builder.

        Example::

            task = await (
                session.background_task()
                    .update(DataSet.of("test", "donor"))
                    .filter(Filter.range("age", 30, 65))
                    .where("not($.update_pass.exists()) or $.update_pass < 5")
                    .bin("campaign1").add(50)
                    .execute()
            )

        See Also:
            :meth:`where`
        """
        if filter_obj is None:
            raise TypeError("filter() requires a Filter, got None")
        if self._filter is not None:
            raise ValueError("filter() can only be called once per background job")
        self._filter = filter_obj
        return self

    def bin(self, name: str) -> BackgroundWriteBinBuilder[BackgroundOperationBuilder]:
        """Start a write on bin *name* (update jobs only).

        Example::

            builder.bin("score").add(10)
            builder.bin("prefs").on_map_key("tags").list_append_items(["sports"])

        Args:
            name: The bin to write.

        Returns:
            A :class:`BackgroundWriteBinBuilder` whose steps return this builder.

        See Also:
            :meth:`add_operation`: Append a prebuilt operation instead.
        """
        return BackgroundWriteBinBuilder(self, name)

    def add_operation(self, op: Any) -> BackgroundOperationBuilder:
        """Append a prebuilt write operation to apply to each matching record.

        Accepts any write operation from ``aerospike_native``: ``Operation``,
        ``ExpOperation``, ``ListOperation``, ``MapOperation``,
        ``BitOperation``, ``HllOperation`` or ``StringOperation``, including
        ones built with a nested CDT context. The server rejects read
        operations in a background job.

        Example::

            task = await (
                session.background_task()
                    .update(DataSet.of("test", "profiles"))
                    .add_operation(MapOperation.remove_by_value_range(
                        "segments", None, [1704067200], MapReturnType.NONE,
                    ))
                    .execute()
            )

        Args:
            op: The operation to append.

        Returns:
            This builder, for chaining.

        See Also:
            :meth:`bin`: Build the operation with chained steps.
        """
        self._operations.append(op)
        return self

    _add_op = add_operation

    def _expression_from_ael_string_for_ops(
        self, expression: Union[str, FilterExpression],
    ) -> FilterExpression:
        if isinstance(expression, str):
            return filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=self._supports_server_compiled_ael,
            )
        return expression

    def expire_record_after_seconds(self, seconds: int) -> BackgroundOperationBuilder:
        """Set record TTL in seconds for touches/updates when supported by policy."""
        self._ttl_seconds = seconds
        return self

    def expire_record_after(self, duration: timedelta) -> BackgroundOperationBuilder:
        """Set record TTL using a :class:`datetime.timedelta` (-1/-2/0 select sentinels)."""
        self._ttl_seconds = _seconds_from_timedelta(duration)
        return self

    def expire_record_at(self, when: datetime) -> BackgroundOperationBuilder:
        """Set record TTL so records expire at an absolute point in time.

        A naive ``when`` is interpreted in local time; pass a timezone-aware
        ``datetime`` for explicit UTC or other zones. Raises ``ValueError``
        if ``when`` is not strictly in the future.
        """
        self._ttl_seconds = _seconds_until(when)
        return self

    def never_expire(self) -> BackgroundOperationBuilder:
        """Make every record the job writes never expire (TTL = -1)."""
        self._ttl_seconds = _TTL_NEVER_EXPIRE
        return self

    def with_no_change_in_expiration(self) -> BackgroundOperationBuilder:
        """Keep each record's existing TTL (TTL = -2)."""
        self._ttl_seconds = _TTL_DONT_UPDATE
        return self

    def expiry_from_server_default(self) -> BackgroundOperationBuilder:
        """Give each record the namespace's default TTL (TTL = 0)."""
        self._ttl_seconds = _TTL_SERVER_DEFAULT
        return self

    def records_per_second(self, rps: int) -> BackgroundOperationBuilder:
        """Throttle the background job to *rps* records per second.

        Args:
            rps: Records per second. ``0`` means unthrottled.

        Returns:
            This builder, for chaining.

        Example::

            await session.update(users).bin("tier").set_to("gold") \\
                .records_per_second(500).execute()
        """
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundOperationBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def include_missing_keys(self) -> BackgroundOperationBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def _pnc_client(self) -> Client:
        fc = self._session._client
        if fc._client is None:
            raise RuntimeError("Client is not connected")
        return fc._client

    def _final_operations(self) -> List[Any]:
        ops = list(self._operations)
        if self._op_type is _OpType.DELETE:
            if not ops:
                ops = [Operation.delete()]
        elif self._op_type is _OpType.TOUCH:
            if not ops:
                ops = [Operation.touch()]
        elif self._op_type is _OpType.UPDATE:
            if not ops:
                raise ValueError(
                    "Background update requires at least one bin operation; "
                    "use .bin(name) steps or .add_operation(...).",
                )
        return ops

    def _record_background_usage(self) -> None:
        extra = []
        if self._filter is not None:
            extra.append(usage.FILTER_SECONDARY_INDEX)
        if self._durable_delete_override or self._durable_delete_command_default:
            extra.append(usage.WRITE_DURABLE_DELETE)
        usage.record_background(
            self._session._client, usage.BACKGROUND_OPERATE, extra,
        )

    def _record_exists_action(self) -> Optional[RecordExistsAction]:
        if self._op_type is _OpType.UPDATE:
            return RecordExistsAction.UPDATE_ONLY
        if self._op_type is _OpType.TOUCH:
            return RecordExistsAction.UPDATE_ONLY
        return None

    def _execute_blocking(self) -> ExecuteTask:
        """Blocking counterpart of :meth:`execute` — uses PNC ``query_operate_blocking``.

        Internal: the sync tree's background builder is a wrapper over this
        class, and this is the terminal it drives. Async callers use
        :meth:`execute`.
        """
        ops = self._final_operations()
        self._record_background_usage()
        mode = self._session._resolve_namespace_mode_blocking(self._dataset.namespace)
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            self._ttl_seconds,
            self._record_exists_action(),
            namespace_mode=mode,
            durable_delete_command_default=self._durable_delete_command_default,
            durable_delete_override=self._durable_delete_override,
            records_per_second=self._records_per_second,
        )
        if self._op_type is not _OpType.DELETE:
            wp.durable_delete = False
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        if self._filter is not None:
            statement.filters = [self._filter]
        client = self._pnc_client()
        try:
            return client.query_operate_blocking(statement, ops, write_policy=wp)
        except Exception as e:
            raise _convert_pnc_exception(e) from e



class BackgroundOperationBuilder(_BackgroundOperationBuilderBase):
    """Configure filters, TTL, and operations for ``query_operate``.

    See Also:
        :meth:`BackgroundTaskSession.update`: Typical construction path.
    """

    __slots__ = (
        "_session",
        "_dataset",
        "_op_type",
        "_operations",
        "_filter_expression",
        "_filter",
        "_ttl_seconds",
        "_records_per_second",
        "_durable_delete_command_default",
        "_durable_delete_override",
    )

    async def execute(self) -> ExecuteTask:
        """Start the server job and return an :class:`~aerospike_native.ExecuteTask`.

        Raises:
            ValueError: For update without bin operations.
            RuntimeError: If the SDK client is not connected.
            AerospikeError: On PNC errors (converted).

        Example::

            task = await (
                session.background_task()
                    .update(users)
                    .bin("visits").add(1)
                    .execute()
            )
            await task.wait_till_complete()

        """
        ops = self._final_operations()
        self._record_background_usage()
        log.debug(
            "background %s: %s.%s ops=%d",
            self._op_type.name if self._op_type else "WRITE",
            self._dataset.namespace, self._dataset.set_name, len(ops),
        )
        mode = await self._session._resolve_namespace_mode(self._dataset.namespace)
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            self._ttl_seconds,
            self._record_exists_action(),
            namespace_mode=mode,
            durable_delete_command_default=self._durable_delete_command_default,
            durable_delete_override=self._durable_delete_override,
            records_per_second=self._records_per_second,
        )
        if self._op_type is not _OpType.DELETE:
            wp.durable_delete = False
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        if self._filter is not None:
            statement.filters = [self._filter]
        client = self._pnc_client()
        try:
            return await client.query_operate(statement, ops, write_policy=wp)
        except Exception as e:
            raise _convert_pnc_exception(e) from e



class BackgroundUdfFunctionBuilder:
    """Pick module and function for a dataset background UDF."""

    __slots__ = ("_session", "_dataset")

    def __init__(self, session: Session, dataset: DataSet) -> None:
        self._session = session
        self._dataset = dataset

    def function(
        self,
        package_name: str,
        function_name: str,
    ) -> BackgroundUdfBuilder:
        """Select the registered package and Lua entrypoint.

        Args:
            package_name: Server module name (no ``.lua`` suffix).
            function_name: Lua function to invoke.

        Returns:
            :class:`BackgroundUdfBuilder` for arguments and execution.

        Raises:
            ValueError: If either string is empty.
        """
        if not package_name:
            raise ValueError("package_name must be a non-empty string")
        if not function_name:
            raise ValueError("function_name must be a non-empty string")
        return BackgroundUdfBuilder(
            self._session,
            self._dataset,
            package_name,
            function_name,
        )


class _BackgroundUdfBuilderBase:
    """State + chaining shared by async and sync BackgroundUdfBuilder.

    Methods migrate from :class:`BackgroundUdfBuilder` during Phase 4 collapse.
    """
    def __init__(
        self,
        session: Session,
        dataset: DataSet,
        package_name: str,
        function_name: str,
    ) -> None:
        self._session = session
        self._dataset = dataset
        self._package_name = package_name
        self._function_name = function_name
        self._args: Optional[List[Any]] = None
        self._filter: Optional[Filter] = None
        self._filter_expression: Optional[FilterExpression] = None
        self._records_per_second: Optional[int] = None
        self._durable_delete_command_default: Optional[bool] = None
        self._durable_delete_override: Optional[bool] = None
        self._supports_server_compiled_ael = bool(
            session._client.supports_server_compiled_ael,
        )

    def default_with_durable_delete(self) -> BackgroundUdfBuilder:
        """Prefer durable deletes when resolving policy defaults (SC namespaces)."""
        self._durable_delete_command_default = True
        return self

    def default_without_durable_delete(self) -> BackgroundUdfBuilder:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._durable_delete_command_default = False
        return self

    def with_durable_delete(self) -> BackgroundUdfBuilder:
        """Force durable delete on this background UDF job."""
        self._durable_delete_override = True
        return self

    def without_durable_delete(self) -> BackgroundUdfBuilder:
        """Force non-durable deletes (may be rejected on SC)."""
        self._durable_delete_override = False
        return self

    def passing(self, *args: Any) -> BackgroundUdfBuilder:
        """Set Lua arguments after the implicit record parameter.

        Returns:
            This builder for chaining.

        Example::
            builder.passing("arg1", 42)
        """
        self._args = list(args)
        return self

    @overload
    def where(self, expression: str, *params: Any) -> BackgroundUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
        *params: Any,
    ) -> BackgroundUdfBuilder:
        """Optional predicate limiting which records invoke the UDF.

        Raises:
            ValueError: If ``where`` has already been called on this builder.
        """
        if self._filter_expression is not None:
            raise ValueError("where() can only be called once per background task")
        expression = bind_ael_params(expression, params)
        if isinstance(expression, str):
            self._filter_expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=self._supports_server_compiled_ael,
            )
        else:
            self._filter_expression = expression
        return self

    def filter(self, filter_obj: Filter) -> BackgroundUdfBuilder:
        """Restrict the UDF job with a secondary-index :class:`~aerospike_native.Filter`.

        The filter attaches to the query ``Statement``, so the server reaches the
        records through the index instead of scanning the set, and invokes the UDF
        only on records in the index range. It combines with :meth:`where`: the
        index selects the candidates and the predicate decides which of them the
        UDF runs on. A job carries at most one filter, because the server accepts
        one index range per query.

        Args:
            filter_obj: The secondary-index filter (for example ``Filter.range``).

        Returns:
            This builder for chaining.

        Raises:
            TypeError: If ``filter_obj`` is ``None``.
            ValueError: If ``filter`` has already been called on this builder.

        Example::

            task = await (
                session.background_task()
                    .execute_udf(DataSet.of("test", "donor"))
                    .function("donor_udfs", "apply_bonus")
                    .passing(50)
                    .filter(Filter.range("age", 30, 65))
                    .execute()
            )

        See Also:
            :meth:`where`
            :meth:`BackgroundOperationBuilder.filter`
        """
        if filter_obj is None:
            raise TypeError("filter() requires a Filter, got None")
        if self._filter is not None:
            raise ValueError("filter() can only be called once per background job")
        self._filter = filter_obj
        return self

    def records_per_second(self, rps: int) -> BackgroundUdfBuilder:
        """Throttle the background job to *rps* records per second.

        Args:
            rps: Records per second. ``0`` means unthrottled.

        Returns:
            This builder, for chaining.

        Example::

            await session.update(users).bin("tier").set_to("gold") \\
                .records_per_second(500).execute()
        """
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundUdfBuilder:
        """Unsupported (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def include_missing_keys(self) -> BackgroundUdfBuilder:
        """Unsupported (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def _pnc_client(self) -> Client:
        fc = self._session._client
        if fc._client is None:
            raise RuntimeError("Client is not connected")
        return fc._client

    def _record_background_usage(self) -> None:
        extra = []
        if self._durable_delete_override or self._durable_delete_command_default:
            extra.append(usage.WRITE_DURABLE_DELETE)
        usage.record_background(
            self._session._client, usage.BACKGROUND_UDF, extra,
        )

    def _execute_blocking(self) -> ExecuteTask:
        """Blocking counterpart of :meth:`execute` — uses PNC ``query_execute_udf_blocking``.

        Internal: the sync tree's background UDF builder is a wrapper over
        this class, and this is the terminal it drives. Async callers use
        :meth:`execute`.
        """
        self._record_background_usage()
        mode = self._session._resolve_namespace_mode_blocking(self._dataset.namespace)
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            None,
            None,
            namespace_mode=mode,
            durable_delete_command_default=self._durable_delete_command_default,
            durable_delete_override=self._durable_delete_override,
            records_per_second=self._records_per_second,
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        if self._filter is not None:
            statement.filters = [self._filter]
        client = self._pnc_client()
        py_args: Optional[List[Any]] = list(self._args) if self._args is not None else None
        try:
            return client.query_execute_udf_blocking(
                statement,
                self._package_name,
                self._function_name,
                py_args,
                write_policy=wp,
            )
        except Exception as e:
            raise _convert_pnc_exception(e) from e



class BackgroundUdfBuilder(_BackgroundUdfBuilderBase):
    """Arguments, optional filter, and execution for ``query_execute_udf``."""

    __slots__ = (
        "_session",
        "_dataset",
        "_package_name",
        "_function_name",
        "_args",
        "_filter",
        "_filter_expression",
        "_records_per_second",
        "_durable_delete_command_default",
        "_durable_delete_override",
    )














    async def execute(self) -> ExecuteTask:
        """Start the background UDF job.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On PNC errors (converted).

        Example::

            task = await (
                session.background_task()
                    .execute_udf(users)
                    .function("mypkg", "expire_old")
                    .passing(30)
                    .execute()
            )
            await task.wait_till_complete()

        """
        self._record_background_usage()
        log.debug(
            "background UDF: %s.%s %s.%s",
            self._dataset.namespace, self._dataset.set_name,
            self._package_name, self._function_name,
        )
        mode = await self._session._resolve_namespace_mode(self._dataset.namespace)
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            None,
            None,
            namespace_mode=mode,
            durable_delete_command_default=self._durable_delete_command_default,
            durable_delete_override=self._durable_delete_override,
            records_per_second=self._records_per_second,
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        if self._filter is not None:
            statement.filters = [self._filter]
        client = self._pnc_client()
        py_args: Optional[List[Any]] = list(self._args) if self._args is not None else None
        try:
            return await client.query_execute_udf(
                statement,
                self._package_name,
                self._function_name,
                py_args,
                write_policy=wp,
            )
        except Exception as e:
            raise _convert_pnc_exception(e) from e

