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

from typing import Any, Union, overload

from aerospike_async import ExecuteTask, FilterExpression

from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder as AsyncBackgroundOperationBuilder,
    BackgroundTaskSession as AsyncBackgroundTaskSession,
    BackgroundUdfBuilder as AsyncBackgroundUdfBuilder,
    BackgroundUdfFunctionBuilder as AsyncBackgroundUdfFunctionBuilder,
    BackgroundWriteBinBuilder as AsyncBackgroundWriteBinBuilder,
)
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.sync.client import _EventLoopManager


class SyncBackgroundWriteBinBuilder:
    """Per-bin scalar write inside a background operation (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundWriteBinBuilder`
    """

    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundWriteBinBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def set_to(self, value: Any) -> SyncBackgroundOperationBuilder:
        """Set the bin to *value* (sync wrapper)."""
        self._inner.set_to(value)
        return SyncBackgroundOperationBuilder(
            self._inner._parent, self._loop_manager)

    def add(self, value: Any) -> SyncBackgroundOperationBuilder:
        """Numeric increment (sync wrapper)."""
        self._inner.add(value)
        return SyncBackgroundOperationBuilder(
            self._inner._parent, self._loop_manager)


class SyncBackgroundOperationBuilder:
    """Configure a background update/delete/touch job (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundOperationBuilder`
    """

    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundOperationBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    @overload
    def where(self, expression: str) -> SyncBackgroundOperationBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncBackgroundOperationBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncBackgroundOperationBuilder:
        """Restrict the scan with an AEL or filter predicate."""
        self._inner.where(expression)
        return self

    def index_filters(self, *filters: Any) -> SyncBackgroundOperationBuilder:
        """Restrict the job using secondary-index ``Filter`` objects (sync)."""
        self._inner.index_filters(*filters)
        return self

    def bin(self, name: str) -> SyncBackgroundWriteBinBuilder:
        return SyncBackgroundWriteBinBuilder(
            self._inner.bin(name), self._loop_manager)

    def expire_record_after_seconds(self, seconds: int) -> SyncBackgroundOperationBuilder:
        """Set record TTL for the background job."""
        self._inner.expire_record_after_seconds(seconds)
        return self

    def records_per_second(self, rps: int) -> SyncBackgroundOperationBuilder:
        """Store a throttle hint."""
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> SyncBackgroundOperationBuilder:
        self._inner.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncBackgroundOperationBuilder:
        """Unsupported for background tasks."""
        self._inner.respond_all_keys()
        return self

    def execute(self) -> ExecuteTask:
        """Submit the job and return a task handle (blocks until accepted).

        See Also:
            :meth:`~aerospike_sdk.aio.background.BackgroundOperationBuilder.execute`
        """
        inner = self._inner

        async def _run() -> ExecuteTask:
            return await inner.execute()

        return self._loop_manager.run_async(_run())


class SyncBackgroundUdfFunctionBuilder:
    """Select UDF package/function for a background dataset run (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundUdfFunctionBuilder`
    """

    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundUdfFunctionBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def function(
        self,
        package_name: str,
        function_name: str,
    ) -> SyncBackgroundUdfBuilder:
        """Select the UDF package and Lua function."""
        b = self._inner.function(package_name, function_name)
        return SyncBackgroundUdfBuilder(b, self._loop_manager)


class SyncBackgroundUdfBuilder:
    """Arguments, filters, and throttle for background UDF execution (sync).

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundUdfBuilder`
    """

    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundUdfBuilder,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def passing(self, *args: Any) -> SyncBackgroundUdfBuilder:
        self._inner.passing(*args)
        return self

    @overload
    def where(self, expression: str) -> SyncBackgroundUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncBackgroundUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncBackgroundUdfBuilder:
        """Optional predicate limiting which records invoke the UDF."""
        self._inner.where(expression)
        return self

    def records_per_second(self, rps: int) -> SyncBackgroundUdfBuilder:
        self._inner.records_per_second(rps)
        return self

    def fail_on_filtered_out(self) -> SyncBackgroundUdfBuilder:
        """Unsupported for background tasks."""
        self._inner.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncBackgroundUdfBuilder:
        """Unsupported for background tasks."""
        self._inner.respond_all_keys()
        return self

    def execute(self) -> ExecuteTask:
        """Submit the background UDF job (blocks until accepted).

        See Also:
            :meth:`~aerospike_sdk.aio.background.BackgroundUdfBuilder.execute`
        """
        inner = self._inner

        async def _run() -> ExecuteTask:
            return await inner.execute()

        return self._loop_manager.run_async(_run())


class SyncBackgroundTaskSession:
    """Sync entry for server-side dataset background operations.

    Obtained from :meth:`~aerospike_sdk.sync.session.SyncSession.background_task`.
    Each method returns a sync builder that mirrors
    :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`.

    See Also:
        :class:`~aerospike_sdk.aio.background.BackgroundTaskSession`

    Examples:
        session.background_task().update(dataset).bin("x").set_to(1).execute()
        session.background_task().execute_udf(dataset).function("pkg", "fn").execute()
    """

    __slots__ = ("_inner", "_loop_manager")

    def __init__(
        self,
        inner: AsyncBackgroundTaskSession,
        loop_manager: _EventLoopManager,
    ) -> None:
        self._inner = inner
        self._loop_manager = loop_manager

    def update(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        b = self._inner.update(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def delete(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        """Start a background delete over *dataset*."""
        b = self._inner.delete(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def touch(self, dataset: DataSet) -> SyncBackgroundOperationBuilder:
        """Start a background touch (TTL refresh) over *dataset*."""
        b = self._inner.touch(dataset)
        return SyncBackgroundOperationBuilder(b, self._loop_manager)

    def execute_udf(self, dataset: DataSet) -> SyncBackgroundUdfFunctionBuilder:
        """Start a background UDF over *dataset*."""
        b = self._inner.execute_udf(dataset)
        return SyncBackgroundUdfFunctionBuilder(b, self._loop_manager)
