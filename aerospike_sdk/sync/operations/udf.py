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

"""Synchronous foreground UDF builders delegating to ``aio.operations.udf``."""

from __future__ import annotations

from typing import Any, List, Union, overload

from aerospike_async import FilterExpression, Key

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.operations.udf import (
    UdfBuilder as AsyncUdfBuilder,
    UdfFunctionBuilder as AsyncUdfFunctionBuilder,
)
from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.sync.operations.query import SyncQueryBuilder, SyncWriteSegmentBuilder
from aerospike_sdk.sync.record_stream import SyncRecordStream


class SyncUdfFunctionBuilder:
    """First step after ``execute_udf``: select package and function name.

    See Also:
        :class:`~aerospike_sdk.aio.operations.udf.UdfFunctionBuilder`

    Examples:
        session.execute_udf(key).function("pkg", "fn")
    """

    __slots__ = ("_inner", "_sdk_client")

    def __init__(
        self,
        inner: AsyncUdfFunctionBuilder,
        sdk_client: Client,
    ) -> None:
        self._inner = inner
        self._sdk_client = sdk_client

    def function(self, package: str, function_name: str) -> SyncUdfBuilder:
        """Select the UDF package and Lua function."""
        async_udf_builder = self._inner.function(package, function_name)
        return SyncUdfBuilder(async_udf_builder, self._sdk_client)


class SyncUdfBuilder:
    """Chain UDF arguments, optional filter, and execution (sync).

    See Also:
        :class:`~aerospike_sdk.aio.operations.udf.UdfBuilder`

    Examples:
        session.execute_udf(key).function("pkg", "fn").passing(1, 2).execute()
        session.execute_udf(key).function("pkg", "fn").query(key).where("true").execute()
    """

    __slots__ = ("_inner", "_sdk_client")

    def __init__(
        self,
        inner: AsyncUdfBuilder,
        sdk_client: Client,
    ) -> None:
        self._inner = inner
        self._sdk_client = sdk_client

    def passing(self, *args: Any) -> SyncUdfBuilder:
        """Forward arguments to the server UDF (chainable)."""
        self._inner.passing(*args)
        return self

    @overload
    def where(self, expression: str) -> SyncUdfBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncUdfBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncUdfBuilder:
        """Restrict rows with an AEL string or :class:`~aerospike_async.FilterExpression`."""
        self._inner.where(expression)
        return self

    def respond_all_keys(self) -> SyncUdfBuilder:
        """Include results for missing keys in the stream.

        Returns:
            self for method chaining.
        """
        self._inner.respond_all_keys()
        return self

    def execute_udf(self, *keys: Key) -> SyncUdfFunctionBuilder:
        """Finalize this UDF spec and start another on *keys*."""
        async_function_builder = self._inner.execute_udf(*keys)
        return SyncUdfFunctionBuilder(async_function_builder, self._sdk_client)

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> SyncQueryBuilder:
        # The inner UdfBuilder's _qb is a SyncQueryBuilder (set up by
        # :meth:`SyncSession.execute_udf`); the inner ``query`` transition
        # returns it directly.
        qb = self._inner.query(arg1, *more_keys)
        assert isinstance(qb, SyncQueryBuilder)
        return qb

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start an upsert write segment."""
        wsb = self._inner.upsert(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start an insert-only write segment."""
        wsb = self._inner.insert(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        wsb = self._inner.update(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start a replace write segment."""
        wsb = self._inner.replace(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        wsb = self._inner.replace_if_exists(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start a delete segment."""
        wsb = self._inner.delete(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start a touch segment."""
        wsb = self._inner.touch(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize the UDF spec and start an exists-check segment."""
        wsb = self._inner.exists(arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb)

    def execute(self, on_error: OnError | None = None) -> SyncRecordStream:
        """Run the UDF and return a :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

        Args:
            on_error: Same semantics as query/write
                :meth:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder.execute`.

        See Also:
            :meth:`~aerospike_sdk.aio.operations.udf.UdfBuilder.execute`
        """
        inner = self._inner
        if inner._qb._udf_function is None:
            raise ValueError(
                "function(package, name) must be called before execute()",
            )
        inner._qb._finalize_udf_spec()
        qb = inner._qb

        # Tier 1: list-returning blocking dispatch (single + multi-key UDF
        # land here via "udf" op_type → execute_udf_blocking / batch_apply_blocking).
        fast = qb.execute_blocking_fast_path(on_error)
        if fast is not None:
            return SyncRecordStream.from_list(fast)

        # Tier 1b: multi-spec blocking dispatch.
        multispec = qb.execute_multispec_blocking(on_error)
        if multispec is not None:
            return SyncRecordStream.from_list(multispec)

        # Every reachable shape is handled by Tier 1 or 1b. If we land here
        # a new code path slipped through without a blocking dispatcher —
        # raise loudly so the gap is identifiable.
        specs = getattr(qb, "_specs", [])
        shape = (
            f"specs={len(specs)}: " + ", ".join(
                f"spec{i}(op_type={s.op_type!r} keys={len(s.keys)} "
                f"ops={len(s.operations)})"
                for i, s in enumerate(specs)
            )
        ) if specs else f"keyless ns={qb._namespace!r} set={qb._set_name!r}"
        raise NotImplementedError(
            f"sync UDF builder shape not yet covered by a blocking dispatcher: "
            f"{shape}")
