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

"""Synchronous query and write-segment builders.

Each sync class inherits state, chaining methods, and the blocking-IO
dispatchers from the corresponding ``_*Base`` in
:mod:`aerospike_sdk.aio.operations.query`. Concrete sync subclasses add
sync ``execute()`` (Tier 1 / 1b / 2 dispatch) and override factory
overrides (``_start_write_verb``, ``_promote``) so chained types stay in
the sync namespace.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

from aerospike_async import ExecuteTask, Key

from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    WriteBinBuilder,
    _QueryBuilderBase,
    _SingleKeyWriteSegmentBase,
    _WriteSegmentBuilderBase,
    _WriteVerbs,
)
from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.sync.record_stream import SyncRecordStream

# Bin builders are parent-generic; the same class serves both async write
# segments (:class:`WriteSegmentBuilder`) and :class:`SyncWriteSegmentBuilder`.
# Aliases preserve the import path callers used during the wrapper era.
SyncQueryBinBuilder = QueryBinBuilder
SyncWriteBinBuilder = WriteBinBuilder


def _describe_specs(qb) -> str:
    """One-line summary of a query builder's specs for diagnostic errors."""
    specs = getattr(qb, "_specs", None)
    if specs is None:
        return "qb=None"
    if not specs:
        return (
            f"keyless ns={qb._namespace!r} set={qb._set_name!r} "
            f"ops={len(getattr(qb, '_operations', []))} "
            f"where_ael={getattr(qb, '_where_ael', None) is not None} "
            f"filter_records={bool(getattr(qb, '_filter_records', None))}"
        )
    parts = []
    for i, s in enumerate(specs):
        parts.append(
            f"spec{i}(op_type={s.op_type!r} keys={len(s.keys)} "
            f"ops={len(s.operations)} "
            f"filter_expression={s.filter_expression is not None} "
            f"gen={s.generation} ttl={s.ttl_seconds})")
    return f"specs={len(specs)}: " + ", ".join(parts)


class SyncQueryBuilder(_QueryBuilderBase, _WriteVerbs):
    """Synchronous query builder.

    Inherits state + chaining + blocking-IO dispatchers from
    :class:`_QueryBuilderBase`. Provides sync ``execute()`` that routes
    through Tier 1 (fast path / multi-key list dispatch), Tier 1b
    (multi-spec blocking dispatch), or Tier 2 (dataset / SI / scan
    streaming) using PAC ``_blocking`` entries. No asyncio loop involved.
    """

    # -- Bin / op entry points (inherited base mutates ``self`` directly) -----

    def bin(self, bin_name: str) -> QueryBinBuilder[SyncQueryBuilder]:
        """Open a per-bin read builder targeting this query builder."""
        return QueryBinBuilder(self, bin_name)

    # -- Write transitions ----------------------------------------------------

    def _start_write_verb(  # type: ignore[override]
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Open a sync write segment after a write verb on this query."""
        # Promote the current query into a write segment by recording the
        # op_type and target keys on this builder, then wrap in
        # :class:`SyncWriteSegmentBuilder`.
        if isinstance(arg1, Key):
            keys = [arg1, *more_keys]
        elif isinstance(arg1, list):
            keys = list(arg1)
            keys.extend(more_keys)
        else:
            raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        self._op_type = op_type
        if len(keys) == 1:
            self._single_key = keys[0]
        else:
            self._keys = keys
        return SyncWriteSegmentBuilder(self)

    # -- Execute --------------------------------------------------------------

    def execute_background_task(self) -> ExecuteTask:
        """Run a background write for this dataset query (synchronous)."""
        return self.execute_background_task_blocking()

    def execute_udf_background_task(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Run a background UDF for this dataset query (synchronous)."""
        return self.execute_udf_background_task_blocking(
            package_name, function_name, args,
        )

    def execute(
        self, on_error: Optional[OnError] = None,
    ) -> SyncRecordStream:
        """Run the configured query/write chain synchronously.

        Tier 1: single-key + multi-key + all op-types (returns list).
        Tier 1b: multi-spec sequential dispatch via PAC ``batch_blocking``.
        Tier 2: dataset / SI / scan streams (returns Recordset; lazy).
        """
        fast = self.execute_blocking_fast_path(on_error)
        if fast is not None:
            return SyncRecordStream.from_list(fast)

        multispec = self.execute_multispec_blocking(on_error)
        if multispec is not None:
            return SyncRecordStream.from_list(multispec)

        stream_kind = self.execute_blocking_stream(on_error)
        if stream_kind is not None:
            kind, payload = stream_kind
            if kind == "recordset":
                return SyncRecordStream.from_pac_recordset(payload)
            if kind == "chunked":
                recordset, reexecute = payload
                return SyncRecordStream.from_chunked_pac_recordset(
                    recordset, reexecute, limit=0,
                )

        raise NotImplementedError(
            f"sync builder shape not yet covered by a blocking dispatcher: "
            f"{_describe_specs(self)}",
        )


class SyncWriteSegmentBuilder(_WriteSegmentBuilderBase, _WriteVerbs):
    """Synchronous write-segment builder.

    Inherits state + chaining + ``execute_blocking_fast_path`` from
    :class:`_WriteSegmentBuilderBase`. Provides sync ``execute()`` and
    overrides ``_start_write_verb`` so chained writes return
    :class:`SyncWriteSegmentBuilder`.
    """

    # -- Bin entry point ------------------------------------------------------

    def bin(self, bin_name: str) -> WriteBinBuilder:
        """Open a per-bin write builder targeting this segment."""
        return WriteBinBuilder(self, bin_name)

    # -- Write transition (chained writes) ------------------------------------

    def _start_write_verb(  # type: ignore[override]
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Finalize this segment and open a fresh sync write segment."""
        # Finalize current segment into a spec on the inner QB.
        self._qb._finalize_current_spec()
        # Open a new segment targeting the new key(s) on the same QB.
        if isinstance(arg1, Key):
            keys = [arg1, *more_keys]
        elif isinstance(arg1, list):
            keys = list(arg1)
            keys.extend(more_keys)
        else:
            raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        self._qb._op_type = op_type
        if len(keys) == 1:
            self._qb._single_key = keys[0]
            self._qb._keys = None
        else:
            self._qb._keys = keys
            self._qb._single_key = None
        return self

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Finalize this segment and open a fresh sync read query on new keys."""
        self._qb._finalize_current_spec()
        if isinstance(arg1, Key):
            keys = [arg1, *more_keys]
        elif isinstance(arg1, list):
            keys = list(arg1)
            keys.extend(more_keys)
        else:
            raise TypeError(f"Expected Key or List[Key], got {type(arg1)}")
        self._qb._op_type = None
        if len(keys) == 1:
            self._qb._single_key = keys[0]
            self._qb._keys = None
        else:
            self._qb._keys = keys
            self._qb._single_key = None
        # The QueryBuilder we wrap is a SyncQueryBuilder per our construction
        # contract; assert and return it as the sync type.
        assert isinstance(self._qb, SyncQueryBuilder)
        return self._qb

    # -- Execute --------------------------------------------------------------

    def execute(
        self, on_error: Optional[OnError] = None,
    ) -> SyncRecordStream:
        """Run the configured write segment synchronously.

        Tries the inherited blocking fast path first; otherwise delegates
        to the wrapped query builder's full dispatch.
        """
        fast = self.execute_blocking_fast_path(on_error)
        if fast is not None:
            return SyncRecordStream.from_list(fast)
        # Fall back to the QB's full sync dispatch (Tier 1b / 2).
        assert isinstance(self._qb, SyncQueryBuilder)
        return self._qb.execute(on_error)


class SyncSingleKeyWriteSegment(_SingleKeyWriteSegmentBase, SyncWriteSegmentBuilder):
    """Synchronous single-key write fast-path segment.

    Inherits fast-path slot state from :class:`_SingleKeyWriteSegmentBase`
    and overrides ``_promote()`` to construct a :class:`SyncQueryBuilder`
    when escalating to the full query path.
    """

    __slots__ = ()

    def _promote(self) -> None:  # type: ignore[override]
        """Populate ``self._qb`` with a :class:`SyncQueryBuilder` (not aio)."""
        if self._qb is not None:
            return
        qb = SyncQueryBuilder(
            client=self._client_fast,
            namespace=self._key.namespace,
            set_name=self._key.set_name,
            behavior=self._behavior_fast,
            cached_write_policy=self._write_policy,
            cached_read_policy=self._read_policy,
            txn=self._txn,
            namespace_mode_resolver=self._namespace_mode_resolver,
            namespace_mode_resolver_blocking=self._namespace_mode_resolver_blocking,
        )
        qb._op_type = self._op_type_fast
        qb._single_key = self._key
        qb._operations = self._ops
        qb._durable_delete_command_default = self._dd_command_default
        qb._durable_delete = self._dd_override
        qb._record_delete_in_operations = self._record_delete_in_fast_ops
        self._qb = qb

    def execute(  # type: ignore[override]
        self, on_error: Optional[OnError] = None,
    ) -> SyncRecordStream:
        """Run the single-key fast path synchronously."""
        # Promote then defer to the SyncQueryBuilder's blocking fast path.
        self._promote()
        return SyncWriteSegmentBuilder.execute(self, on_error)
