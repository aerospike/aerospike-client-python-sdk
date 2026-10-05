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

"""Synchronous background dataset task builders (delegate to ``aio.background``)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Union, overload


from aerospike_async import ExecuteTask, Filter, FilterExpression

from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder as AsyncBackgroundOperationBuilder,
    BackgroundTaskSession as AsyncBackgroundTaskSession,
    BackgroundUdfBuilder as AsyncBackgroundUdfBuilder,
    BackgroundUdfFunctionBuilder as AsyncBackgroundUdfFunctionBuilder,
    BackgroundWriteBinBuilder,
)
from aerospike_sdk.dataset import DataSet


class BackgroundOperationBuilder:
    """Configure a background update/delete/touch job (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundOperationBuilder`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBackgroundOperationBuilder) -> None:
        self._inner = inner

    def default_with_durable_delete(self) -> BackgroundOperationBuilder:
        """Prefer durable deletes when resolving policy defaults (SC namespaces)."""
        self._inner.default_with_durable_delete()
        return self

    def default_without_durable_delete(self) -> BackgroundOperationBuilder:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._inner.default_without_durable_delete()
        return self

    def with_durable_delete(self) -> BackgroundOperationBuilder:
        """Force durable delete on this background job."""
        self._inner.with_durable_delete()
        return self

    def without_durable_delete(self) -> BackgroundOperationBuilder:
        """Force non-durable deletes (may be rejected on SC)."""
        self._inner.without_durable_delete()
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
        """Restrict the job with an AEL or filter predicate.

        Combines with :meth:`filter`; see
        :meth:`aerospike_sdk.aio.background.BackgroundOperationBuilder.where`
        for the full contract.
        """
        self._inner.where(expression, *params)
        return self

    def filter(self, filter_obj: Filter) -> BackgroundOperationBuilder:
        """Restrict the job with a secondary-index ``Filter`` (sync).

        Combines with :meth:`where`; see
        :meth:`aerospike_sdk.aio.background.BackgroundOperationBuilder.filter`
        for the full contract.

        Raises:
            TypeError: If ``filter_obj`` is ``None``.
            ValueError: If ``filter`` has already been called on this builder.
        """
        self._inner.filter(filter_obj)
        return self

    def bin(self, name: str) -> BackgroundWriteBinBuilder[BackgroundOperationBuilder]:
        """Start a write on bin *name* (update jobs only).

        Example::

            builder.bin("prefs").on_map_key("tags").list_append_items(["sports"])

        See Also:
            :meth:`aerospike_sdk.aio.background.BackgroundOperationBuilder.bin`
        """
        return BackgroundWriteBinBuilder(self, name)

    def add_operation(self, op: Any) -> BackgroundOperationBuilder:
        """Append a prebuilt write operation to apply to each matching record.

        See Also:
            :meth:`aerospike_sdk.aio.background.BackgroundOperationBuilder.add_operation`
        """
        self._inner.add_operation(op)
        return self

    _add_op = add_operation

    def _expression_from_ael_string_for_ops(
        self, expression: Union[str, FilterExpression],
    ) -> FilterExpression:
        return self._inner._expression_from_ael_string_for_ops(expression)

    def expire_record_after_seconds(self, seconds: int) -> BackgroundOperationBuilder:
        """Set record TTL for the background job."""
        self._inner.expire_record_after_seconds(seconds)
        return self

    def expire_record_after(self, duration: timedelta) -> BackgroundOperationBuilder:
        """Set record TTL using a :class:`datetime.timedelta` (-1/-2/0 select sentinels)."""
        self._inner.expire_record_after(duration)
        return self

    def expire_record_at(self, when: datetime) -> BackgroundOperationBuilder:
        """Set record TTL so records expire at an absolute point in time.

        A naive ``when`` is interpreted in local time; pass a timezone-aware
        ``datetime`` for explicit UTC or other zones. Raises ``ValueError``
        if ``when`` is not strictly in the future.
        """
        self._inner.expire_record_at(when)
        return self

    def never_expire(self) -> BackgroundOperationBuilder:
        """Make every record the job writes never expire (TTL = -1)."""
        self._inner.never_expire()
        return self

    def with_no_change_in_expiration(self) -> BackgroundOperationBuilder:
        """Keep each record's existing TTL (TTL = -2)."""
        self._inner.with_no_change_in_expiration()
        return self

    def expiry_from_server_default(self) -> BackgroundOperationBuilder:
        """Give each record the namespace's default TTL (TTL = 0)."""
        self._inner.expiry_from_server_default()
        return self

    def records_per_second(self, rps: int) -> BackgroundOperationBuilder:
        """Throttle the background job to *rps* records per second.

        Args:
            rps: Records per second. ``0`` means unthrottled.

        Returns:
            This builder, for chaining.

        Example::

            session.update(users).bin("tier").set_to("gold") \\
                .records_per_second(500).execute()
        """
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> BackgroundOperationBuilder:
        self._inner.fail_on_filtered_out()
        return self

    def include_missing_keys(self) -> BackgroundOperationBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        self._inner.include_missing_keys()
        return self

    def execute(self) -> ExecuteTask:
        """Submit the job and return a task handle (blocks until accepted).

        See Also:
            :meth:`~aerospike_sdk.aio.background.BackgroundOperationBuilder.execute`
        """
        return self._inner._execute_blocking()


class BackgroundUdfFunctionBuilder:
    """Select UDF package/function for a background dataset run (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundUdfFunctionBuilder`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBackgroundUdfFunctionBuilder) -> None:
        self._inner = inner

    def function(
        self,
        package_name: str,
        function_name: str,
    ) -> BackgroundUdfBuilder:
        """Select the UDF package and Lua function."""
        async_udf_builder = self._inner.function(package_name, function_name)
        return BackgroundUdfBuilder(async_udf_builder)


class BackgroundUdfBuilder:
    """Arguments, filters, and throttle for background UDF execution (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundUdfBuilder`
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBackgroundUdfBuilder) -> None:
        self._inner = inner

    def default_with_durable_delete(self) -> BackgroundUdfBuilder:
        """Prefer durable deletes when resolving policy defaults (SC namespaces)."""
        self._inner.default_with_durable_delete()
        return self

    def default_without_durable_delete(self) -> BackgroundUdfBuilder:
        """Prefer non-durable deletes when resolving policy defaults."""
        self._inner.default_without_durable_delete()
        return self

    def with_durable_delete(self) -> BackgroundUdfBuilder:
        """Force durable delete on this background job."""
        self._inner.with_durable_delete()
        return self

    def without_durable_delete(self) -> BackgroundUdfBuilder:
        """Force non-durable deletes (may be rejected on SC)."""
        self._inner.without_durable_delete()
        return self

    def passing(self, *args: Any) -> BackgroundUdfBuilder:
        self._inner.passing(*args)
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
        """Optional predicate limiting which records invoke the UDF."""
        self._inner.where(expression, *params)
        return self

    def records_per_second(self, rps: int) -> BackgroundUdfBuilder:
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> BackgroundUdfBuilder:
        """Unsupported for background tasks."""
        self._inner.fail_on_filtered_out()
        return self

    def include_missing_keys(self) -> BackgroundUdfBuilder:
        """Unsupported for background tasks (raises ``TypeError``)."""
        self._inner.include_missing_keys()
        return self

    def execute(self) -> ExecuteTask:
        """Submit the background UDF job (blocks until accepted).

        See Also:
            :meth:`~aerospike_sdk.aio.background.BackgroundUdfBuilder.execute`
        """
        return self._inner._execute_blocking()


class BackgroundTaskSession:
    """Sync entry for server-side dataset background operations.

    Obtained from :meth:`~aerospike_sdk.sync.session.Session.background_task`.
    Each method returns a sync builder that mirrors
    :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`.

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`

    Examples:
        session.background_task().update(dataset).bin("x").set_to(1).execute()
        session.background_task().execute_udf(dataset).function("pkg", "fn").execute()
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncBackgroundTaskSession) -> None:
        self._inner = inner

    def update(self, dataset: DataSet) -> BackgroundOperationBuilder:
        async_op_builder = self._inner.update(dataset)
        return BackgroundOperationBuilder(async_op_builder)

    def delete(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Start a background delete over *dataset*."""
        async_op_builder = self._inner.delete(dataset)
        return BackgroundOperationBuilder(async_op_builder)

    def touch(self, dataset: DataSet) -> BackgroundOperationBuilder:
        """Start a background touch (TTL refresh) over *dataset*."""
        async_op_builder = self._inner.touch(dataset)
        return BackgroundOperationBuilder(async_op_builder)

    def execute_udf(self, dataset: DataSet) -> BackgroundUdfFunctionBuilder:
        """Start a background UDF over *dataset*."""
        async_udf_function_builder = self._inner.execute_udf(dataset)
        return BackgroundUdfFunctionBuilder(async_udf_function_builder)
