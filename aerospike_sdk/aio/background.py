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
from typing import TYPE_CHECKING, Any, List, Optional, Union, overload

log = logging.getLogger("aerospike_sdk.background")

from aerospike_async import (
    Client,
    ExecuteTask,
    FilterExpression,
    Operation,
    RecordExistsAction,
)

from aerospike_sdk.background_shared import (
    dataset_statement,
    make_background_write_policy,
    reject_unsupported_background_write_ops,
)
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.pac_sdk_client_attr import PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL
from aerospike_sdk.ael.server_filter import filter_expression_from_ael_string
from aerospike_sdk.exceptions import _convert_pac_exception

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.aio.session import Session


class _OpType(enum.Enum):
    UPDATE = enum.auto()
    DELETE = enum.auto()
    TOUCH = enum.auto()


_BG_UNSUPPORTED = (
    "fail_on_filtered_out and respond_all_keys apply to foreground reads; "
    "they are not supported for background tasks."
)


class BackgroundTaskSession:
    """Choose a dataset-wide background job (update, delete, touch, or UDF).

    From :meth:`~aerospike_sdk.aio.session.Session.background_task`. Each
    method returns a builder to add filters, bin operations or UDF arguments,
    then ``await ...execute()`` for a server :class:`~aerospike_async.ExecuteTask`.

    Example:
        Background update with a filter::

            task = await (
                session.background_task()
                .update(users)
                .where("$.active == true")
                .bin("score").add(1)
                .execute()
            )

    See Also:
        :meth:`~aerospike_sdk.aio.session.Session.execute_udf`: Foreground UDF on keys.
    """

    def __init__(self, session: Session) -> None:
        """Bind to *session*; prefer :meth:`Session.background_task`."""
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


class BackgroundWriteBinBuilder:
    """Per-bin write helper for background updates (``put`` / ``add`` only).

    Obtained from :meth:`BackgroundOperationBuilder.bin`. Call :meth:`set_to`
    or :meth:`add`, which return the parent builder for further chaining.

    Example::

        builder.bin("score").add(10)
    """

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: BackgroundOperationBuilder, bin_name: str) -> None:
        """Capture the bin name; prefer :meth:`BackgroundOperationBuilder.bin`."""
        self._parent = parent
        self._bin = bin_name

    def set_to(self, value: Any) -> BackgroundOperationBuilder:
        """Set the bin to *value* (``Operation.put``).

        Args:
            value: The value to write.

        Returns:
            The parent :class:`BackgroundOperationBuilder`.
        """
        self._parent._operations.append(Operation.put(self._bin, value))
        return self._parent

    def add(self, value: Any) -> BackgroundOperationBuilder:
        """Add a numeric *value* to the bin (``Operation.add``).

        Args:
            value: Numeric amount to add (may be negative).

        Returns:
            The parent :class:`BackgroundOperationBuilder`.
        """
        self._parent._operations.append(Operation.add(self._bin, value))
        return self._parent


class BackgroundOperationBuilder:
    """Configure filters, TTL, and operations for ``query_operate``.

    Not all query-policy knobs are wired through to PAC for background jobs;
    ``records_per_second`` is stored for API parity but may not affect the
    underlying call.

    See Also:
        :meth:`BackgroundTaskSession.update`: Typical construction path.
    """

    __slots__ = (
        "_session",
        "_dataset",
        "_op_type",
        "_operations",
        "_filter_expression",
        "_ttl_seconds",
        "_records_per_second",
    )

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
        self._ttl_seconds: Optional[int] = None
        self._records_per_second: Optional[int] = None

    @overload
    def where(self, expression: str) -> BackgroundOperationBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundOperationBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> BackgroundOperationBuilder:
        """Restrict the scan with an AEL or ``FilterExpression`` predicate.

        Returns:
            This builder for chaining.

        Example::
            builder.where("$.status == 'inactive'")
        """
        if isinstance(expression, str):
            self._filter_expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=bool(
                    getattr(
                        self._pac_client(),
                        PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL,
                        False,
                    )
                ),
            )
        else:
            self._filter_expression = expression
        return self

    def bin(self, name: str) -> BackgroundWriteBinBuilder:
        """Start a scalar write on *name* (update jobs only).

        Example::
            builder.bin("score").add(10)
        """
        return BackgroundWriteBinBuilder(self, name)

    def expire_record_after_seconds(self, seconds: int) -> BackgroundOperationBuilder:
        """Set record TTL in seconds for touches/updates when supported by policy."""
        self._ttl_seconds = seconds
        return self

    def records_per_second(self, rps: int) -> BackgroundOperationBuilder:
        """Store a throttle hint (may be unused depending on PAC background API)."""
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundOperationBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def respond_all_keys(self) -> BackgroundOperationBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def _pac_client(self) -> Client:
        fc = self._session.client
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
                    "use .bin(name).set_to(...) or .add(...).",
                )
        return ops

    def _record_exists_action(self) -> Optional[RecordExistsAction]:
        if self._op_type is _OpType.UPDATE:
            return RecordExistsAction.UPDATE_ONLY
        if self._op_type is _OpType.TOUCH:
            return RecordExistsAction.UPDATE_ONLY
        return None

    async def execute(self) -> ExecuteTask:
        """Start the server job and return an :class:`~aerospike_async.ExecuteTask`.

        Raises:
            ValueError: For update without bin operations.
            RuntimeError: If the SDK client is not connected.
            AerospikeError: On PAC errors (converted).

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
        reject_unsupported_background_write_ops(ops)
        log.debug(
            "background %s: %s.%s ops=%d",
            self._op_type.name if self._op_type else "WRITE",
            self._dataset.namespace, self._dataset.set_name, len(ops),
        )
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            self._ttl_seconds,
            self._record_exists_action(),
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        client = self._pac_client()
        try:
            return await client.query_operate(wp, statement, ops)
        except Exception as e:
            raise _convert_pac_exception(e) from e


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


class BackgroundUdfBuilder:
    """Arguments, optional filter, and execution for ``query_execute_udf``."""

    __slots__ = (
        "_session",
        "_dataset",
        "_package_name",
        "_function_name",
        "_args",
        "_filter_expression",
        "_records_per_second",
    )

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
        self._filter_expression: Optional[FilterExpression] = None
        self._records_per_second: Optional[int] = None

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
    def where(self, expression: str) -> BackgroundUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> BackgroundUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> BackgroundUdfBuilder:
        """Optional predicate limiting which records invoke the UDF."""
        if isinstance(expression, str):
            self._filter_expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=bool(
                    getattr(
                        self._pac_client(),
                        PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL,
                        False,
                    )
                ),
            )
        else:
            self._filter_expression = expression
        return self

    def records_per_second(self, rps: int) -> BackgroundUdfBuilder:
        """Throttle hint stored for API parity (may not affect PAC)."""
        self._records_per_second = rps
        return self

    def fail_on_filtered_out(self) -> BackgroundUdfBuilder:
        """Unsupported (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def respond_all_keys(self) -> BackgroundUdfBuilder:
        """Unsupported (raises ``TypeError``)."""
        raise TypeError(_BG_UNSUPPORTED)

    def _pac_client(self) -> Client:
        fc = self._session.client
        if fc._client is None:
            raise RuntimeError("Client is not connected")
        return fc._client

    async def execute(self) -> ExecuteTask:
        """Start the background UDF job.

        Raises:
            RuntimeError: If the client is not connected.
            AerospikeError: On PAC errors (converted).

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
        log.debug(
            "background UDF: %s.%s %s.%s",
            self._dataset.namespace, self._dataset.set_name,
            self._package_name, self._function_name,
        )
        wp = make_background_write_policy(
            self._session.behavior,
            self._filter_expression,
            None,
            None,
        )
        statement = dataset_statement(
            self._dataset.namespace,
            self._dataset.set_name,
        )
        client = self._pac_client()
        py_args: Optional[List[Any]] = (
            list(self._args) if self._args is not None else None
        )
        try:
            return await client.query_execute_udf(
                wp,
                statement,
                self._package_name,
                self._function_name,
                py_args,
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e
