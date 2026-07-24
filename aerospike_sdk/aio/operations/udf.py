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

"""Foreground UDF execution builders (single-key, batch, and chained operations).

Chain state and chaining methods live on the shared, runtime-agnostic bases
in :mod:`aerospike_sdk.udf_shared`; this module adds the async ``execute()``
terminal.
"""

from __future__ import annotations

from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.record_stream import RecordStream
from aerospike_sdk.udf_shared import _UdfBuilderBase, _UdfFunctionBuilderBase

# Bases re-exported for callers that historically imported them from this
# module (the shared definitions now live in udf_shared).
__all__ = [
    "UdfBuilder",
    "UdfFunctionBuilder",
    "_UdfBuilderBase",
    "_UdfFunctionBuilderBase",
]


class UdfFunctionBuilder(_UdfFunctionBuilderBase[QueryBuilder]):
    """First step of foreground UDF chaining: choose package and Lua function name.

    Produced by :meth:`~aerospike_sdk.aio.session.Session.execute_udf` or
    :meth:`UdfBuilder.execute_udf`. Call :meth:`function` before :meth:`UdfBuilder.passing`
    or :meth:`UdfBuilder.execute`.

    Example::

        stream = await (
            session.execute_udf(key)
                .function("my_module", "my_func")
                .execute()
        )

    """

    __slots__ = ()


class UdfBuilder(_UdfBuilderBase[QueryBuilder]):
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

    __slots__ = ()

    async def execute(self, on_error: OnError | None = None) -> RecordStream:
        """Run the current builder state and return a :class:`~aerospike_sdk.record_stream.RecordStream`.

        Requires :meth:`UdfFunctionBuilder.function` to have been called for
        the pending UDF operation.

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


# Bind the tier-appropriate leaf classes into the shared bases' factory
# hooks (see udf_shared for why these are class attributes, not imports).
UdfFunctionBuilder._udf_builder_cls = UdfBuilder
UdfBuilder._udf_function_builder_cls = UdfFunctionBuilder
# Chain transition hook: lets query/write chains open a UDF segment
# (`QueryBuilder.execute_udf`) without the shared base importing this module.
QueryBuilder._udf_function_builder_cls = UdfFunctionBuilder
