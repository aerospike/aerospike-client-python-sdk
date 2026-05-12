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

"""Synchronous query and write-verb builders delegating to ``aio.operations.query``."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union, overload

from aerospike_async import (
    BasePolicy,
    BitOperation,
    CTX,
    ExecuteTask,
    Filter,
    FilterExpression,
    GeoJSON,
    HllOperation,
    Key,
    ListOperation,
    ListOrderType,
    ListReturnType,
    ListSortFlags,
    MapOperation,
    MapOrder,
    MapPolicy,
    MapReturnType,
    MapWriteFlags,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
)

from aerospike_sdk.aio.operations.cdt_read import _map_item_pairs
from aerospike_sdk.aio.operations.cdt_write import (
    CdtWriteBuilder,
    CdtWriteInvertableBuilder,
    _UNORDERED_LIST_POLICY,
    _resolve_list_policy,
    _resolve_map_policy,
)
from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    QueryHint,
    WriteSegmentBuilder,
    _bit_policy_or_default,
    _bitwise_and,
    _bitwise_not,
    _bitwise_or,
    _resize_flags_or_default,
    _resolve_hll_flags,
)
from aerospike_sdk.ael.filter_gen import IndexContext
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.error_strategy import OnError
from aerospike_sdk.sync.client import _EventLoopManager
from aerospike_sdk.sync.record_stream import SyncRecordStream


class _SyncWriteVerbs:
    """Mixin mirroring async write-verb entry points on :class:`QueryBuilder`.

    Subclasses implement ``_start_write_verb`` to open a
    :class:`SyncWriteSegmentBuilder`. Semantics match
    :class:`~aerospike_sdk.aio.operations.query.QueryBuilder` /
    :class:`~aerospike_sdk.aio.session.Session`.
    """

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        raise NotImplementedError

    def upsert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an upsert write segment."""
        return self._start_write_verb("upsert", arg1, *more_keys)

    def insert(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an insert (create-only) segment."""
        return self._start_write_verb("insert", arg1, *more_keys)

    def update(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an update (update-only) segment."""
        return self._start_write_verb("update", arg1, *more_keys)

    def replace(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a replace segment."""
        return self._start_write_verb("replace", arg1, *more_keys)

    def replace_if_exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a replace-if-exists segment."""
        return self._start_write_verb("replace_if_exists", arg1, *more_keys)

    def delete(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a delete segment."""
        return self._start_write_verb("delete", arg1, *more_keys)

    def touch(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start a touch segment (reset TTL)."""
        return self._start_write_verb("touch", arg1, *more_keys)

    def exists(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        """Start an exists-check segment."""
        return self._start_write_verb("exists", arg1, *more_keys)


class SyncQueryBuilder(_SyncWriteVerbs):
    """Configure and run reads, queries, and write segments synchronously.

    Every chain method forwards to :class:`~aerospike_sdk.aio.operations.query.QueryBuilder`.
    :meth:`execute` blocks on the owning loop manager and returns
    :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`. Detailed
    parameter semantics (filters, policies, CDT, ``on_error``) are documented
    on the async builder.

    See Also:
        :class:`~aerospike_sdk.aio.operations.query.QueryBuilder`
    """

    def __init__(
        self,
        async_client: Any,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
        query_builder: Optional[QueryBuilder] = None,
    ) -> None:
        """Attach or create an async :class:`QueryBuilder` and the sync loop."""
        self._loop_manager = loop_manager
        self._qb: QueryBuilder = query_builder if query_builder is not None else QueryBuilder(
            client=async_client, namespace=namespace, set_name=set_name,
        )

    # -- Bin projection / selection -------------------------------------------

    def bins(self, bin_names: List[str]) -> SyncQueryBuilder:
        """Specify which bins to retrieve."""
        self._qb.bins(bin_names)
        return self

    def bin(self, bin_name: str) -> QueryBinBuilder[SyncQueryBuilder]:
        """Start a bin-level read operation."""
        return QueryBinBuilder(self, bin_name)

    def add_operation(self, op: Any) -> None:
        """Append a read operation produced by a bin or CDT builder."""
        self._qb.add_operation(op)

    def with_write_operations(
        self, operations: Sequence[Any],
    ) -> SyncQueryBuilder:
        """Attach scalar write operations for a background dataset task."""
        self._qb.with_write_operations(operations)
        return self

    def with_no_bins(self) -> SyncQueryBuilder:
        """Specify that no bins should be read (header-only query)."""
        self._qb.with_no_bins()
        return self

    def with_op_projection(self, *ops: Any) -> SyncQueryBuilder:
        """Project query results through one or more read operations.

        Forwards to
        :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.with_op_projection`.
        """
        self._qb.with_op_projection(*ops)
        return self

    # -- Filtering ------------------------------------------------------------

    def filter(self, filter_obj: Filter) -> SyncQueryBuilder:
        """Add a filter to the query."""
        self._qb.filter(filter_obj)
        return self

    def filter_expression(self, expression: FilterExpression) -> SyncQueryBuilder:
        """Set a FilterExpression for server-side filtering."""
        self._qb.filter_expression(expression)
        return self

    @overload
    def where(self, expression: str) -> SyncQueryBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncQueryBuilder:
        """Set the query filter from an AEL string or FilterExpression."""
        self._qb.where(expression)
        return self

    # -- Policy / options -----------------------------------------------------

    def with_policy(self, policy: QueryPolicy) -> SyncQueryBuilder:
        """Set the query policy."""
        self._qb.with_policy(policy)
        return self

    def with_read_policy(self, policy: ReadPolicy) -> SyncQueryBuilder:
        """Set the read policy (for single key or batch key queries)."""
        self._qb.with_read_policy(policy)
        return self

    def partition(self, partition_filter: PartitionFilter) -> SyncQueryBuilder:
        """Set the partition filter."""
        self._qb.partition(partition_filter)
        return self

    def on_partitions(self, *partition_ids: int) -> SyncQueryBuilder:
        """Set partitions to query by partition IDs."""
        self._qb.on_partitions(*partition_ids)
        return self

    def on_partition(self, part_id: int) -> SyncQueryBuilder:
        """Target a specific partition for the query."""
        self._qb.on_partition(part_id)
        return self

    def on_partition_range(self, start_incl: int, end_excl: int) -> SyncQueryBuilder:
        """Target a range of partitions for the query."""
        self._qb.on_partition_range(start_incl, end_excl)
        return self

    def chunk_size(self, chunk_size: int) -> SyncQueryBuilder:
        """Set the chunk size for server-side streaming."""
        self._qb.chunk_size(chunk_size)
        return self

    def records_per_second(self, rps: int) -> SyncQueryBuilder:
        """Set the maximum records per second for the query."""
        self._qb.records_per_second(rps)
        return self

    def max_records(self, max_records: int) -> SyncQueryBuilder:
        """Set the maximum number of records to return."""
        self._qb.max_records(max_records)
        return self

    def limit(self, limit: int) -> SyncQueryBuilder:
        """Set the maximum number of records to return (alias for max_records)."""
        self._qb.limit(limit)
        return self

    def expected_duration(self, duration: QueryDuration) -> SyncQueryBuilder:
        """Set the expected duration of the query."""
        self._qb.expected_duration(duration)
        return self

    def with_hint(self, hint: QueryHint) -> SyncQueryBuilder:
        """Attach a query hint for secondary index selection or scheduling.

        Forwards to :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.with_hint`.

        Args:
            hint: A :class:`QueryHint` instance.

        Returns:
            This builder for method chaining.

        See Also:
            :class:`~aerospike_sdk.aio.operations.query.QueryHint`
        """
        self._qb.with_hint(hint)
        return self

    def with_index_context(self, index_context: IndexContext) -> SyncQueryBuilder:
        """Explicitly override the secondary index metadata used for filter generation.

        Most applications do **not** need this method. The client automatically
        discovers and caches secondary index metadata in the background.

        Args:
            index_context: Index metadata for the query's namespace.

        Returns:
            This builder for method chaining.
        """
        self._qb.with_index_context(index_context)
        return self

    def replica(self, replica: Replica) -> SyncQueryBuilder:
        """Set the replica preference for the query."""
        self._qb.replica(replica)
        return self

    def base_policy(self, base_policy: BasePolicy) -> SyncQueryBuilder:
        """Set the base policy for the query."""
        self._qb.base_policy(base_policy)
        return self

    def fail_on_filtered_out(self) -> SyncQueryBuilder:
        """Include filtered-out records in the stream with FILTERED_OUT code."""
        self._qb.fail_on_filtered_out()
        return self

    def respond_all_keys(self) -> SyncQueryBuilder:
        """Return null for missing keys instead of omitting them."""
        self._qb.respond_all_keys()
        return self

    # -- Chain-level defaults -------------------------------------------------

    @overload
    def default_where(self, expression: str) -> SyncQueryBuilder: ...

    @overload
    def default_where(self, expression: FilterExpression) -> SyncQueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncQueryBuilder:
        """Set a default filter for all chained operations that lack their own."""
        self._qb.default_where(expression)
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> SyncQueryBuilder:
        """Set a default TTL for all chained operations that lack their own."""
        self._qb.default_expire_record_after_seconds(seconds)
        return self

    def default_never_expire(self) -> SyncQueryBuilder:
        """Set the default TTL to never expire (TTL = -1)."""
        self._qb.default_never_expire()
        return self

    def default_with_no_change_in_expiration(self) -> SyncQueryBuilder:
        """Set the default to preserve each record's existing TTL (TTL = -2)."""
        self._qb.default_with_no_change_in_expiration()
        return self

    def default_expiry_from_server_default(self) -> SyncQueryBuilder:
        """Set the default TTL to the namespace's server default (TTL = 0)."""
        self._qb.default_expiry_from_server_default()
        return self

    # -- Query stacking -------------------------------------------------------

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Chain another query with new key(s) for batch/point stacking."""
        self._qb.query(arg1, *more_keys)
        return self

    # -- Write transitions ----------------------------------------------------

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        wsb = self._qb._start_write_verb(op_type, arg1, *more_keys)
        return SyncWriteSegmentBuilder(wsb, self._loop_manager)

    # -- Execute --------------------------------------------------------------

    def execute_background_task(self) -> ExecuteTask:
        """Run a background write for this dataset query (see ``QueryBuilder``)."""
        qb = self._qb

        async def _run():
            return await qb.execute_background_task()

        return self._loop_manager.run_async(_run())

    def execute_udf_background_task(
        self,
        package_name: str,
        function_name: str,
        args: Optional[Sequence[Any]] = None,
    ) -> ExecuteTask:
        """Run a background UDF for this dataset query (see ``QueryBuilder``)."""
        qb = self._qb

        async def _run():
            return await qb.execute_udf_background_task(
                package_name, function_name, args)

        return self._loop_manager.run_async(_run())

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Run the configured query or write chain and block until the stream is ready.

        Args:
            on_error: Same as :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.execute`
                (:class:`~aerospike_sdk.error_strategy.ErrorStrategy` or callback).

        Returns:
            :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

        See Also:
            :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.execute`
        """
        qb = self._qb

        async def _run():
            return await qb.execute(on_error)

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteSegmentBuilder(_SyncWriteVerbs):
    """Synchronous multi-key write segment (mirrors :class:`WriteSegmentBuilder`).

    Bin scalars, CDT, expressions, and policies delegate to the embedded async
    segment; :meth:`execute` returns :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

    See Also:
        :class:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder`
    """

    __slots__ = ("_wsb", "_loop_manager")

    def __init__(
        self, wsb: WriteSegmentBuilder, loop_manager: _EventLoopManager,
    ) -> None:
        self._wsb = wsb
        self._loop_manager = loop_manager

    # -- Bin operations -------------------------------------------------------

    def bin(self, bin_name: str) -> SyncWriteBinBuilder:
        """Start a bin-level write operation."""
        return SyncWriteBinBuilder(self, bin_name)

    def add_operation(self, op: Any) -> None:
        """Append an operation (used by CDT action builders)."""
        self._wsb.add_operation(op)

    def put(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Set multiple bins at once."""
        self._wsb.put(bins)
        return self

    def set_bins(self, bins: dict) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`put`."""
        return self.put(bins)

    # -- Scalar bin operations (direct on segment) ----------------------------

    def set_to(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Set a bin to *value*."""
        self._wsb.set_to(bin_name, value)
        return self

    def add(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Add a numeric *value* to a bin."""
        self._wsb.add(bin_name, value)
        return self

    def increment_by(self, bin_name: str, value: Any) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`add`."""
        return self.add(bin_name, value)

    def get(self, bin_name: str) -> SyncWriteSegmentBuilder:
        """Read a bin value back within a write operate."""
        self._wsb.get(bin_name)
        return self

    def append(self, bin_name: str, value: str) -> SyncWriteSegmentBuilder:
        """Append a string to a bin."""
        self._wsb.append(bin_name, value)
        return self

    def prepend(self, bin_name: str, value: str) -> SyncWriteSegmentBuilder:
        """Prepend a string to a bin."""
        self._wsb.prepend(bin_name, value)
        return self

    def remove_bin(self, bin_name: str) -> SyncWriteSegmentBuilder:
        """Delete a bin from the record."""
        self._wsb.remove_bin(bin_name)
        return self

    # -- Record-level operations ----------------------------------------------

    def delete_record(self) -> SyncWriteSegmentBuilder:
        """Add a record-level delete to the current operate call.

        Unlike :meth:`~SyncWriteVerbs.delete` which targets a different key,
        this deletes the record being operated on as part of the same
        atomic operation.

        Example::

            stream = (
                session.upsert(key)
                    .bin("name").get()
                    .delete_record()
                    .execute()
            )

        Returns:
            This segment for chaining.

        See Also:
            :meth:`~SyncWriteVerbs.delete`: Start a new delete segment for a key.
        """
        self._wsb.delete_record()
        return self

    def touch_record(self) -> SyncWriteSegmentBuilder:
        """Add a record-level touch to the current operate call.

        Resets the record's TTL as part of an atomic multi-operation call.
        Combine with :meth:`expire_record_after_seconds` to set a new TTL.

        Example::

            stream = (
                session.upsert(key)
                    .bin("score").get()
                    .touch_record()
                    .expire_record_after_seconds(120)
                    .execute()
            )

        Returns:
            This segment for chaining.

        See Also:
            :meth:`~SyncWriteVerbs.touch`: Start a new touch segment for a key.
        """
        self._wsb.touch_record()
        return self

    # -- Expression operations (direct on segment) ----------------------------

    def select_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Read a computed value into a bin using an AEL expression."""
        self._wsb.select_from(bin_name, expression, ignore_eval_failure=ignore_eval_failure)
        return self

    def insert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        self._wsb.insert_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    def update_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        self._wsb.update_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    def upsert_from(
        self,
        bin_name: str,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        self._wsb.upsert_from(
            bin_name, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )
        return self

    # -- Transition methods ---------------------------------------------------

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Finalize current write segment and start a read segment."""
        qb = self._wsb.query(arg1, *more_keys)
        return SyncQueryBuilder(
            None, "", "", self._loop_manager, query_builder=qb,
        )

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        self._wsb._start_write_verb(op_type, arg1, *more_keys)
        return self

    # -- Per-operation settings ------------------------------------------------

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> SyncWriteSegmentBuilder:
        """Set a filter expression on the current write segment."""
        self._wsb.where(expression)
        return self

    def expire_record_after_seconds(self, seconds: int) -> SyncWriteSegmentBuilder:
        """Set the TTL on the current write segment."""
        self._wsb.expire_record_after_seconds(seconds)
        return self

    def never_expire(self) -> SyncWriteSegmentBuilder:
        """Set this record to never expire (TTL = -1)."""
        self._wsb.never_expire()
        return self

    def with_no_change_in_expiration(self) -> SyncWriteSegmentBuilder:
        """Preserve the record's existing TTL (TTL = -2)."""
        self._wsb.with_no_change_in_expiration()
        return self

    def expiry_from_server_default(self) -> SyncWriteSegmentBuilder:
        """Use the namespace's default TTL for this record (TTL = 0)."""
        self._wsb.expiry_from_server_default()
        return self

    def ensure_generation_is(self, generation: int) -> SyncWriteSegmentBuilder:
        """Set expected generation for optimistic locking."""
        self._wsb.ensure_generation_is(generation)
        return self

    def with_durable_delete(self) -> SyncWriteSegmentBuilder:
        """Force durable delete on the current operation (override)."""
        self._wsb.with_durable_delete()
        return self

    def without_durable_delete(self) -> SyncWriteSegmentBuilder:
        """Force a non-durable delete on the current operation (override)."""
        self._wsb.without_durable_delete()
        return self

    def default_with_durable_delete(self) -> SyncWriteSegmentBuilder:
        """Prefer durable deletes when resolving behavior defaults (SC namespaces)."""
        self._wsb.default_with_durable_delete()
        return self

    def default_without_durable_delete(self) -> SyncWriteSegmentBuilder:
        """Prefer non-durable deletes when resolving behavior defaults."""
        self._wsb.default_without_durable_delete()
        return self

    def respond_all_keys(self) -> SyncWriteSegmentBuilder:
        """Include results for missing keys in the stream."""
        self._wsb.respond_all_keys()
        return self

    def fail_on_filtered_out(self) -> SyncWriteSegmentBuilder:
        """Mark filtered-out records with ``FILTERED_OUT`` result code."""
        self._wsb.fail_on_filtered_out()
        return self

    def replace_only(self) -> SyncWriteSegmentBuilder:
        """Change the current segment to replace-if-exists semantics."""
        self._wsb.replace_only()
        return self

    # -- Execution ------------------------------------------------------------

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Flush accumulated write operations and return a synchronous result stream.

        Args:
            on_error: Same as :meth:`SyncQueryBuilder.execute`.

        Returns:
            :class:`~aerospike_sdk.sync.record_stream.SyncRecordStream`.

        See Also:
            :meth:`~aerospike_sdk.aio.operations.query.WriteSegmentBuilder.execute`
        """
        wsb = self._wsb

        async def _run():
            return await wsb.execute(on_error)

        stream = self._loop_manager.run_async(_run())
        return SyncRecordStream(stream, self._loop_manager)


class SyncWriteBinBuilder(_SyncWriteVerbs):
    """Synchronous wrapper for bin-level write operations.

    Per-bin write builder that captures a bin name and delegates
    all operations to the parent ``SyncWriteSegmentBuilder``.
    HyperLogLog and blob bit operations use ``hll_*`` and ``bit_*`` methods,
    matching :class:`~aerospike_sdk.aio.operations.query.WriteBinBuilder`.
    """

    __slots__ = ("_sync_segment", "_bin")

    def __init__(
        self, sync_segment: SyncWriteSegmentBuilder, bin_name: str,
    ) -> None:
        self._sync_segment = sync_segment
        self._bin = bin_name

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> SyncWriteSegmentBuilder:
        """Set the bin to *value*.

        Args:
            value: New value to store.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.set_to(self._bin, value)

    def set_to_geo_json(self, geo_json: str) -> SyncWriteSegmentBuilder:
        """Set the bin to a GeoJSON value from its string form.

        The bin's server-side particle type is GEOJSON, not STRING. Equivalent
        to ``set_to(GeoJSON(geo_json))`` but reads naturally for spatial data.

        Args:
            geo_json: A GeoJSON string (e.g. a Point, Polygon, or AeroCircle).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.set_to(self._bin, GeoJSON(geo_json))

    def add(self, value: Any) -> SyncWriteSegmentBuilder:
        """Add a numeric *value* to the bin.

        Args:
            value: Numeric value to add.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.add(self._bin, value)

    def increment_by(self, value: Any) -> SyncWriteSegmentBuilder:
        """Alias for :meth:`add`.

        Args:
            value: Numeric value to add.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self.add(value)

    def append(self, value: str) -> SyncWriteSegmentBuilder:
        """Append a string to the bin.

        Args:
            value: String to append.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.append(self._bin, value)

    def prepend(self, value: str) -> SyncWriteSegmentBuilder:
        """Prepend a string to the bin.

        Args:
            value: String to prepend.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.prepend(self._bin, value)

    def remove(self) -> SyncWriteSegmentBuilder:
        """Delete the bin from the record.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.remove_bin(self._bin)

    def get(self) -> SyncWriteSegmentBuilder:
        """Read the bin value back within a write operate.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.get(self._bin)

    # -- CDT list structural operations ---------------------------------------

    def list_add(
        self, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Add *value* to an ordered list (sorted insert).

        Args:
            value: Value to insert.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            ListOrderType.ORDERED, unique=unique, bounded=bounded,
            no_fail=no_fail,
        )
        self._sync_segment.add_operation(
            ListOperation.append(self._bin, value, policy),
        )
        return self._sync_segment

    def list_append(
        self, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Append *value* to the end of an unordered list.

        Args:
            value: Value to append.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded, no_fail=no_fail,
        )
        self._sync_segment.add_operation(
            ListOperation.append(self._bin, value, policy),
        )
        return self._sync_segment

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> SyncWriteSegmentBuilder:
        """Remove all entries from the map bin.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.clear(self._bin))
        return self._sync_segment

    def map_size(self) -> SyncWriteSegmentBuilder:
        """Return the map element count (read within operate).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.size(self._bin))
        return self._sync_segment

    def map_upsert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Put multiple map entries (create or update each key).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.DEFAULT,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, policy),
        )
        return self._sync_segment

    def map_insert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Put map entries only for keys that do not yet exist.

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, policy),
        )
        return self._sync_segment

    def map_update_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Update existing map entries only (no new keys).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.UPDATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            MapOperation.put_items(self._bin, pairs, policy),
        )
        return self._sync_segment

    def map_create(self, order: MapOrder) -> SyncWriteSegmentBuilder:
        """Create an empty map with the given key order.

        Args:
            order: Key sort order for the map.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(MapOperation.create(self._bin, order))
        return self._sync_segment

    def map_set_policy(self, order: MapOrder) -> SyncWriteSegmentBuilder:
        """Set map sort order policy without changing entries.

        Args:
            order: Key sort order to apply.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            MapOperation.set_map_policy(self._bin, MapPolicy(order, None)),
        )
        return self._sync_segment

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> SyncWriteSegmentBuilder:
        """Remove all elements from the list bin.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.clear(self._bin))
        return self._sync_segment

    def list_sort(
        self, flags: ListSortFlags = ListSortFlags.DEFAULT,
    ) -> SyncWriteSegmentBuilder:
        """Sort the list bin.

        Args:
            flags: Sort behavior flags (default ``DEFAULT``).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.sort(self._bin, flags))
        return self._sync_segment

    def list_size(self) -> SyncWriteSegmentBuilder:
        """Return the list element count (read within operate).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.size(self._bin))
        return self._sync_segment

    def list_append_items(
        self, items: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Append values to an unordered list.

        Args:
            items: Values to append.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            ListOperation.append_items(self._bin, items, policy),
        )
        return self._sync_segment

    def list_add_items(
        self, items: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Values to insert.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            ListOrderType.ORDERED, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            ListOperation.append_items(self._bin, items, policy),
        )
        return self._sync_segment

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Create an empty list with the given order.

        Args:
            order: Element ordering.
            pad: If ``True``, allow sparse indexes.
            persist_index: If ``True``, maintain a persistent index.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.create(self._bin, order, pad, persist_index),
        )
        return self._sync_segment

    def list_set_order(self, order: ListOrderType) -> SyncWriteSegmentBuilder:
        """Set list sort order without changing elements.

        Args:
            order: Sort order to apply.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.set_order(self._bin, order))
        return self._sync_segment

    # -- Index-based list (whole-bin) ----------------------------------------

    def list_insert(
        self, index: int, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Insert *value* at *index* in an unordered list.

        Args:
            index: List index (0-based; negative counts from the end).
            value: Element to insert.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded, no_fail=no_fail,
        )
        self._sync_segment.add_operation(
            ListOperation.insert(self._bin, index, value, policy),
        )
        return self._sync_segment

    def list_insert_items(
        self, index: int, items: Sequence[Any],
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Insert a sequence of values starting at *index*.

        Args:
            index: List index at which to insert the first element.
            items: Values to insert in order.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        self._sync_segment.add_operation(
            ListOperation.insert_items(self._bin, index, items, policy),
        )
        return self._sync_segment

    def list_set(self, index: int, value: Any) -> SyncWriteSegmentBuilder:
        """Replace the element at *index* with *value*.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_set`.

        Args:
            index: List index (0-based; negative counts from the end).
            value: New element value.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.set(self._bin, index, value),
        )
        return self._sync_segment

    def list_increment(self, index: int, value: int = 1) -> SyncWriteSegmentBuilder:
        """Add *value* to the numeric element at *index* (default increment is ``1``).

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_increment`.

        Args:
            index: List index (0-based; negative counts from the end).
            value: Amount to add; ``1`` uses a dedicated server path.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        if value == 1:
            self._sync_segment.add_operation(
                ListOperation.increment_by_one(self._bin, index),
            )
        else:
            self._sync_segment.add_operation(
                ListOperation.increment(
                    self._bin, index, value, _UNORDERED_LIST_POLICY,
                ),
            )
        return self._sync_segment

    def list_remove(self, index: int) -> SyncWriteSegmentBuilder:
        """Remove the element at *index*.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_remove`.

        Args:
            index: List index (0-based; negative counts from the end).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.remove(self._bin, index))
        return self._sync_segment

    def list_remove_range(
        self, index: int, count: Optional[int] = None,
    ) -> SyncWriteSegmentBuilder:
        """Remove *count* elements starting at *index*, or all from *index* onward.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_remove_range`.

        Args:
            index: Starting list index.
            count: Number of elements to remove; ``None`` removes through the end.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        if count is None:
            op = ListOperation.remove_range_from(self._bin, index)
        else:
            op = ListOperation.remove_range(self._bin, index, count)
        self._sync_segment.add_operation(op)
        return self._sync_segment

    def list_pop(self, index: int) -> SyncWriteSegmentBuilder:
        """Remove and return the element at *index* (read in the operate result).

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_pop`.

        Args:
            index: List index (0-based; negative counts from the end).

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(ListOperation.pop(self._bin, index))
        return self._sync_segment

    def list_pop_range(
        self, index: int, count: Optional[int] = None,
    ) -> SyncWriteSegmentBuilder:
        """Pop *count* elements from *index*, or from *index* through the end.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_pop_range`.

        Args:
            index: Starting list index.
            count: Number of elements; ``None`` pops through the end.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        if count is None:
            op = ListOperation.pop_range_from(self._bin, index)
        else:
            op = ListOperation.pop_range(self._bin, index, count)
        self._sync_segment.add_operation(op)
        return self._sync_segment

    def list_trim(self, index: int, count: int) -> SyncWriteSegmentBuilder:
        """Keep only *count* elements starting at *index*; remove the rest.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.list_trim`.

        Args:
            index: Starting list index of the range to keep.
            count: Number of elements to keep.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        self._sync_segment.add_operation(
            ListOperation.trim(self._bin, index, count),
        )
        return self._sync_segment

    # -- HyperLogLog ----------------------------------------------------------

    def hll_init(
        self,
        config: HllConfig,
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Initialize an empty HyperLogLog sketch in this bin.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_init`.
        The synchronous API returns :class:`SyncWriteSegmentBuilder` and uses
        :meth:`SyncWriteSegmentBuilder.execute` instead of ``await``.

        Example::

            session.upsert(key).bin("visitors").hll_init(HllConfig.of(12))

        Args:
            config: Same as the async method.
            create_only: Same as the async method.
            update_only: Same as the async method.
            no_fail: Same as the async method.
            allow_fold: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_init`
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        self._sync_segment.add_operation(
            HllOperation.init(
                self._bin,
                config.index_bit_count,
                config.min_hash_bit_count,
                flags,
            ),
        )
        return self._sync_segment

    def hll_add(
        self,
        values: Sequence[Any],
        *,
        config: Optional[HllConfig] = None,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Add distinct values to the HyperLogLog sketch in this bin.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_add`.

        Example::

            session.upsert(key).bin("visitors").hll_add(["user-1", "user-2"])

        Args:
            values: Same as the async method.
            config: Same as the async method.
            create_only: Same as the async method.
            update_only: Same as the async method.
            no_fail: Same as the async method.
            allow_fold: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_add`
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        index_bit_count = config.index_bit_count if config is not None else -1
        min_hash_bit_count = config.min_hash_bit_count if config is not None else -1
        self._sync_segment.add_operation(
            HllOperation.add(
                self._bin,
                list(values),
                index_bit_count,
                min_hash_bit_count,
                flags,
            ),
        )
        return self._sync_segment

    def hll_set_union(
        self,
        hll_list: Sequence[Any],
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Merge other HyperLogLog sketches into this bin.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_set_union`.

        Example::

            session.update(key).bin("merged").hll_set_union([other_hll_blob])

        Args:
            hll_list: Same as the async method.
            create_only: Same as the async method.
            update_only: Same as the async method.
            no_fail: Same as the async method.
            allow_fold: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_set_union`
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        self._sync_segment.add_operation(
            HllOperation.set_union(self._bin, list(hll_list), flags),
        )
        return self._sync_segment

    def hll_fold(self, index_bit_count: int) -> SyncWriteSegmentBuilder:
        """Reduce sketch precision to a lower index bit count.

        Example::
            session.update(key).bin("hll").hll_fold(10)

        Args:
            index_bit_count: Same as :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_fold`.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_fold`
        """
        self._sync_segment.add_operation(HllOperation.fold(self._bin, index_bit_count))
        return self._sync_segment

    def hll_refresh_count(self) -> SyncWriteSegmentBuilder:
        """Refresh the cached cardinality estimate stored with the sketch.

        Example::
            session.update(key).bin("hll").hll_refresh_count()

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_refresh_count`
        """
        self._sync_segment.add_operation(HllOperation.refresh_count(self._bin))
        return self._sync_segment

    def hll_get_count(self) -> SyncWriteSegmentBuilder:
        """Read the estimated cardinality in a multi-operation write.

        Example::
            session.update(key).bin("hll").hll_get_count()

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_get_count`
            :meth:`aerospike_sdk.aio.operations.query.QueryBinBuilder.hll_get_count`
        """
        self._sync_segment.add_operation(HllOperation.get_count(self._bin))
        return self._sync_segment

    def hll_describe(self) -> SyncWriteSegmentBuilder:
        """Read index and min-hash bit parameters of the stored sketch.

        Example::
            session.update(key).bin("hll").hll_describe()

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_describe`
        """
        self._sync_segment.add_operation(HllOperation.describe(self._bin))
        return self._sync_segment

    def hll_get_union(self, hll_list: Sequence[Any]) -> SyncWriteSegmentBuilder:
        """Read the union sketch without modifying the stored bin.

        Example::
            session.update(key).bin("hll").hll_get_union([peer_blob])

        Args:
            hll_list: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_get_union`
        """
        self._sync_segment.add_operation(
            HllOperation.get_union(self._bin, list(hll_list)),
        )
        return self._sync_segment

    def hll_get_union_count(self, hll_list: Sequence[Any]) -> SyncWriteSegmentBuilder:
        """Read the estimated cardinality of the union with other sketches.

        Args:
            hll_list: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_get_union_count`
        """
        self._sync_segment.add_operation(
            HllOperation.get_union_count(self._bin, list(hll_list)),
        )
        return self._sync_segment

    def hll_get_intersect_count(self, hll_list: Sequence[Any]) -> SyncWriteSegmentBuilder:
        """Read the estimated intersection cardinality with other sketches.

        Args:
            hll_list: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_get_intersect_count`
        """
        self._sync_segment.add_operation(
            HllOperation.get_intersect_count(self._bin, list(hll_list)),
        )
        return self._sync_segment

    def hll_get_similarity(self, hll_list: Sequence[Any]) -> SyncWriteSegmentBuilder:
        """Read Jaccard similarity between this sketch and other sketches.

        Args:
            hll_list: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_get_similarity`
        """
        self._sync_segment.add_operation(
            HllOperation.get_similarity(self._bin, list(hll_list)),
        )
        return self._sync_segment

    # -- Bit (blob) -----------------------------------------------------------

    def bit_resize(
        self,
        byte_size: int,
        resize_flags: Optional[Any] = None,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Grow or shrink the raw bytes backing this bin.

        Semantics match :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_resize`.

        Example::
            session.upsert(key).bin("flags").bit_resize(4)

        Args:
            byte_size: Same as the async method.
            resize_flags: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_resize`
        """
        self._sync_segment.add_operation(
            BitOperation.resize(
                self._bin,
                byte_size,
                _resize_flags_or_default(resize_flags),
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_insert(
        self,
        byte_offset: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Insert bytes at a byte offset in the blob bin.

        Example::
            session.update(key).bin("blob").bit_insert(0, b"\\x01\\x02")

        Args:
            byte_offset: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_insert`
        """
        self._sync_segment.add_operation(
            BitOperation.insert(
                self._bin,
                byte_offset,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_remove(
        self,
        byte_offset: int,
        byte_size: int,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Remove a byte range from the blob bin.

        Example::
            session.update(key).bin("blob").bit_remove(0, 2)

        Args:
            byte_offset: Same as :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_remove`.
            byte_size: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_remove`
        """
        self._sync_segment.add_operation(
            BitOperation.remove(
                self._bin,
                byte_offset,
                byte_size,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_set(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Overwrite a bit range with a new value.

        Example::
            session.update(key).bin("blob").bit_set(0, 8, b"\\xff")

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_set`
        """
        self._sync_segment.add_operation(
            BitOperation.set(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_or(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Bitwise OR a value into a bit range.

        Example::
            session.update(key).bin("blob").bit_or(0, 8, b"\\x0f")

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_or`
        """
        self._sync_segment.add_operation(
            _bitwise_or(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_xor(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Bitwise XOR a value into a bit range.

        Example::
            session.update(key).bin("blob").bit_xor(0, 8, b"\\xff")

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_xor`
        """
        self._sync_segment.add_operation(
            BitOperation.xor(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_and(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Bitwise AND a value into a bit range.

        Example::
            session.update(key).bin("blob").bit_and(0, 8, b"\\xf0")

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_and`
        """
        self._sync_segment.add_operation(
            _bitwise_and(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_not(
        self,
        bit_offset: int,
        bit_size: int,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Invert every bit in a range.

        Example::
            session.update(key).bin("blob").bit_not(0, 8)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_not`
        """
        self._sync_segment.add_operation(
            _bitwise_not(
                self._bin,
                bit_offset,
                bit_size,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_lshift(
        self,
        bit_offset: int,
        bit_size: int,
        shift: int,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Left-shift the bits in a field.

        Example::
            session.update(key).bin("blob").bit_lshift(0, 16, 2)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            shift: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_lshift`
        """
        self._sync_segment.add_operation(
            BitOperation.lshift(
                self._bin,
                bit_offset,
                bit_size,
                shift,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_rshift(
        self,
        bit_offset: int,
        bit_size: int,
        shift: int,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Right-shift the bits in a field.

        Example::
            session.update(key).bin("blob").bit_rshift(0, 16, 2)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            shift: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_rshift`
        """
        self._sync_segment.add_operation(
            BitOperation.rshift(
                self._bin,
                bit_offset,
                bit_size,
                shift,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_add(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        signed: bool,
        action: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Add to an integer bit field (see :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_add`).

        Example::
            session.update(key).bin("blob").bit_add(0, 16, 1, False, BitwiseOverflowActions.WRAP)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            signed: Same as the async method.
            action: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_add`
        """
        self._sync_segment.add_operation(
            BitOperation.add(
                self._bin,
                bit_offset,
                bit_size,
                value,
                signed,
                action,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_subtract(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        signed: bool,
        action: Any,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Subtract from an integer bit field (see :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_subtract`).

        Example::
            session.update(key).bin("blob").bit_subtract(0, 16, 1, False, BitwiseOverflowActions.SATURATE)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            signed: Same as the async method.
            action: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_subtract`
        """
        self._sync_segment.add_operation(
            BitOperation.subtract(
                self._bin,
                bit_offset,
                bit_size,
                value,
                signed,
                action,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_set_int(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        policy: Optional[Any] = None,
    ) -> SyncWriteSegmentBuilder:
        """Write an integer value into a bit field.

        Example::
            session.update(key).bin("blob").bit_set_int(0, 16, 42)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.
            policy: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_set_int`
        """
        self._sync_segment.add_operation(
            BitOperation.set_int(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )
        return self._sync_segment

    def bit_get(self, bit_offset: int, bit_size: int) -> SyncWriteSegmentBuilder:
        """Read a bit range as bytes in a write operate.

        Example::
            session.update(key).bin("blob").bit_get(0, 8)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_get`
        """
        self._sync_segment.add_operation(
            BitOperation.get(self._bin, bit_offset, bit_size),
        )
        return self._sync_segment

    def bit_count(self, bit_offset: int, bit_size: int) -> SyncWriteSegmentBuilder:
        """Count bits set to 1 in a range.

        Example::
            session.update(key).bin("blob").bit_count(0, 8)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_count`
        """
        self._sync_segment.add_operation(
            BitOperation.count(self._bin, bit_offset, bit_size),
        )
        return self._sync_segment

    def bit_lscan(self, bit_offset: int, bit_size: int, value: bool) -> SyncWriteSegmentBuilder:
        """Return the leftmost bit index matching a value in a range.

        Example::
            session.update(key).bin("blob").bit_lscan(0, 8, True)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_lscan`
        """
        self._sync_segment.add_operation(
            BitOperation.lscan(self._bin, bit_offset, bit_size, value),
        )
        return self._sync_segment

    def bit_rscan(self, bit_offset: int, bit_size: int, value: bool) -> SyncWriteSegmentBuilder:
        """Return the rightmost bit index matching a value in a range.

        Example::
            session.update(key).bin("blob").bit_rscan(0, 8, False)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            value: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_rscan`
        """
        self._sync_segment.add_operation(
            BitOperation.rscan(self._bin, bit_offset, bit_size, value),
        )
        return self._sync_segment

    def bit_get_int(
        self, bit_offset: int, bit_size: int, signed: bool,
    ) -> SyncWriteSegmentBuilder:
        """Decode an integer from a bit field.

        Example::
            session.update(key).bin("blob").bit_get_int(0, 16, False)

        Args:
            bit_offset: Same as the async method.
            bit_size: Same as the async method.
            signed: Same as the async method.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder` for chaining.

        See Also:
            :meth:`aerospike_sdk.aio.operations.query.WriteBinBuilder.bit_get_int`
        """
        self._sync_segment.add_operation(
            BitOperation.get_int(self._bin, bit_offset, bit_size, signed),
        )
        return self._sync_segment

    # -- Map navigation (singular -> CdtWriteBuilder) -------------------------

    def on_map_index(self, index: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by index.

        Args:
            index: Map index to target.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            lambda rt: MapOperation.remove_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(
        self, key: Any, *, create_type: Optional[MapOrder] = None,
    ) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.
            create_type: If set, use a create-on-missing context for this key
                with the given map key order.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        _mp = MapPolicy(None, None)
        if create_type is not None:
            to_ctx = lambda: CTX.map_key_create(key, create_type)
        else:
            to_ctx = lambda: CTX.map_key(key)
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            lambda rt: MapOperation.remove_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=to_ctx,
            set_to_factory=lambda v: MapOperation.put(b, key, v, _mp),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _mp),
        )

    def on_map_rank(self, rank: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a map element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            lambda rt: MapOperation.remove_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
        )

    # -- Map navigation (invertable -> CdtWriteInvertableBuilder) -------------

    def on_map_value(self, value: Any) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            lambda rt: MapOperation.remove_by_value(b, value, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_value(value),
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by index range.

        Args:
            index: Start index.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_index_range_from(b, index, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range_from(b, index, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_index_range(b, index, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range(b, index, count, rt)
        return CdtWriteInvertableBuilder(
            self._sync_segment, get_f, rm_f, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by key range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_key_range(b, start, end, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by rank range.

        Args:
            rank: Start rank (0 = lowest value).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_rank_range_from(b, rank, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range_from(b, rank, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_rank_range(b, rank, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range(b, rank, count, rt)
        return CdtWriteInvertableBuilder(
            self._sync_segment, get_f, rm_f, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_value_range(b, start, end, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Anchor key.
            index: Relative index offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            lambda rt: MapOperation.remove_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map entries by value rank range relative to an anchor value.

        Args:
            value: Anchor value.
            rank: Relative rank offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: MapOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_list(self, keys: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a list of keys.

        Args:
            keys: Map keys to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            lambda rt: MapOperation.remove_by_key_list(b, keys, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to map elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            lambda rt: MapOperation.remove_by_value_list(b, values, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    # -- List navigation (singular -> CdtWriteBuilder) ------------------------

    def on_list_index(
        self, index: int,
        *,
        order: Optional[ListOrderType] = None,
        pad: bool = False,
    ) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a list element by index.

        Args:
            index: List index (0-based, negative counts from end).
            order: If set (or if *pad* is ``True``), use create-on-missing
                list context with this order; when only *pad* is ``True``,
                defaults to :data:`~aerospike_async.ListOrderType.UNORDERED`.
            pad: When using create-on-missing context, allow sparse indexes.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        use_create = order is not None or pad
        if use_create:
            eff_order = order if order is not None else ListOrderType.UNORDERED
            to_ctx = lambda: CTX.list_index_create(index, eff_order, pad)
        else:
            to_ctx = lambda: CTX.list_index(index)
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            lambda rt: ListOperation.remove_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=to_ctx,
        )

    def on_list_rank(self, rank: int) -> CdtWriteBuilder[SyncWriteSegmentBuilder]:
        """Navigate to a list element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b = self._bin
        return CdtWriteBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            lambda rt: ListOperation.remove_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- List navigation (invertable -> CdtWriteInvertableBuilder) ------------

    def on_list_value(self, value: Any) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            lambda rt: ListOperation.remove_by_value(b, value, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_value(value),
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by index range.

        Args:
            index: Start index.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            lambda rt: ListOperation.remove_by_index_range(b, index, count, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by rank range.

        Args:
            rank: Start rank (0 = lowest value).
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            lambda rt: ListOperation.remove_by_rank_range(b, rank, count, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            lambda rt: ListOperation.remove_by_value_range(b, start, end, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements by value rank range relative to an anchor value.

        Args:
            value: Anchor value.
            rank: Relative rank offset from the anchor.
            count: Maximum entries; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: ListOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[SyncWriteSegmentBuilder]:
        """Navigate to list elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._sync_segment,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            lambda rt: ListOperation.remove_by_value_list(b, values, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    # -- Expression operations ------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Read a computed value into this bin using an AEL expression.

        Args:
            expression: AEL string or ``FilterExpression``.
            ignore_eval_failure: If ``True``, suppress evaluation errors.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.select_from(
            self._bin, expression, ignore_eval_failure=ignore_eval_failure,
        )

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin does not already exist.

        Args:
            expression: AEL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.insert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result only if bin already exists.

        Args:
            expression: AEL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.update_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> SyncWriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin.

        Args:
            expression: AEL string or ``FilterExpression``.
            ignore_op_failure: If ``True``, suppress operation failures.
            ignore_eval_failure: If ``True``, suppress evaluation errors.
            delete_if_null: If ``True``, delete the bin when the expression evaluates to null.

        Returns:
            The parent :class:`SyncWriteSegmentBuilder`.
        """
        return self._sync_segment.upsert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    # -- Convenience transitions (delegate to segment) ------------------------

    def bin(self, bin_name: str) -> SyncWriteBinBuilder:
        """Start the next bin operation.

        Args:
            bin_name: Name of the next bin.

        Returns:
            :class:`SyncWriteBinBuilder` for the named bin.
        """
        return SyncWriteBinBuilder(self._sync_segment, bin_name)

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncQueryBuilder:
        """Shortcut: finalize write segment and start a read segment.

        Args:
            arg1: Key or list of keys.
            more_keys: Additional keys.

        Returns:
            :class:`SyncQueryBuilder` for read operations.
        """
        return self._sync_segment.query(arg1, *more_keys)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> SyncWriteSegmentBuilder:
        return self._sync_segment._start_write_verb(op_type, arg1, *more_keys)

    def execute(
        self, on_error: OnError | None = None,
    ) -> SyncRecordStream:
        """Shortcut: execute all accumulated operations.

        Args:
            on_error: Error handling strategy.

        Returns:
            :class:`SyncRecordStream` with operation results.
        """
        return self._sync_segment.execute(on_error)
