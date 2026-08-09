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

"""Synchronous foreground UDF builders (blocking terminals).

Chain state and chaining methods live on the shared, runtime-agnostic bases
in :mod:`aerospike_sdk.udf_shared`; this module adds the blocking
``execute()`` terminal. The wrapped query builder is the sync
:class:`~aerospike_sdk.sync.operations.query.QueryBuilder` (constructed by
:meth:`~aerospike_sdk.sync.session.Session.execute_udf`), so verb transitions inherited from the
base already return sync segment types.
"""

from __future__ import annotations

from typing import List, Union

from aerospike_async import Key

from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.sync.operations.query import QueryBuilder
from aerospike_sdk.sync.record_stream import RecordStream
from aerospike_sdk.udf_shared import _UdfBuilderBase, _UdfFunctionBuilderBase

__all__ = [
    "UdfBuilder",
    "UdfFunctionBuilder",
]


class UdfFunctionBuilder(_UdfFunctionBuilderBase[QueryBuilder]):
    """First step after ``execute_udf``: select package and function name.

    See Also:
        :class:`~aerospike_sdk.aio.operations.udf.UdfFunctionBuilder`: Async equivalent.

    Examples:
        session.execute_udf(key).function("pkg", "fn")
    """

    __slots__ = ()


class UdfBuilder(_UdfBuilderBase[QueryBuilder]):
    """Chain UDF arguments, optional filter, and execution (sync).

    See Also:
        :class:`~aerospike_sdk.aio.operations.udf.UdfBuilder`: Async equivalent.

    Examples:
        session.execute_udf(key).function("pkg", "fn").passing(1, 2).execute()
        session.execute_udf(key).function("pkg", "fn").query(key).where("true").execute()
    """

    __slots__ = ()

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> QueryBuilder:
        """Close the UDF operation and begin a sync read query segment."""
        qb = super().query(arg1, *more_keys)
        # The wrapped query builder is a sync QueryBuilder per
        # Session.execute_udf's construction contract; assert so the
        # sync type flows to callers.
        assert isinstance(qb, QueryBuilder)
        return qb

    def execute(self, on_error: OnError | None = None) -> RecordStream:
        """Run the UDF and return a :class:`~aerospike_sdk.sync.record_stream.RecordStream`.

        Args:
            on_error: Same semantics as query/write
                :meth:`~aerospike_sdk.sync.operations.query.QueryBuilder.execute`.

        See Also:
            :meth:`~aerospike_sdk.aio.operations.udf.UdfBuilder.execute`
        """
        qb = self._qb
        if qb._udf_function is None:
            raise ValueError(
                "function(package, name) must be called before execute()",
            )
        qb._finalize_udf_spec()

        # Tier 1: list-returning blocking dispatch (single + multi-key UDF
        # land here via "udf" op_type → execute_udf_blocking / batch_apply_blocking).
        fast = qb._execute_blocking_fast_path(on_error)
        if fast is not None:
            return RecordStream._from_list(fast)

        # Tier 1b: multi-spec blocking dispatch.
        multispec = qb._execute_multispec_blocking(on_error)
        if multispec is not None:
            return RecordStream._from_list(multispec)

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


# Bind the tier-appropriate leaf classes into the shared bases' factory
# hooks (see udf_shared for why these are class attributes, not imports).
UdfFunctionBuilder._udf_builder_cls = UdfBuilder
UdfBuilder._udf_function_builder_cls = UdfFunctionBuilder
# Chain transition hook: lets sync query/write chains open a UDF segment
# (`QueryBuilder.execute_udf`) and get the sync builder back.
QueryBuilder._udf_function_builder_cls = UdfFunctionBuilder

# Deprecated aliases, kept importable for one release cycle.
SyncUdfFunctionBuilder = UdfFunctionBuilder
SyncUdfBuilder = UdfBuilder
