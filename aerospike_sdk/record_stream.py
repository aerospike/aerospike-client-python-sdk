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

"""RecordStream — async iterable of RecordResult for batch and query operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Sequence

from aerospike_async import Key, PartitionFilter, Record
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.record_result import RecordResult, batch_records_to_results

if TYPE_CHECKING:  # Not unused — needed for forward-reference type annotations and Sphinx autodoc.
    from aerospike_sdk.error_strategy import ErrorHandler
    from aerospike_sdk.exceptions import AerospikeError

log = logging.getLogger(__name__)


class _SingleResultIter:
    """Lightweight async iterator that yields exactly one RecordResult."""

    __slots__ = ("_result",)

    def __init__(self, result: RecordResult) -> None:
        self._result: RecordResult | None = result

    def __aiter__(self) -> _SingleResultIter:
        return self

    async def __anext__(self) -> RecordResult:
        r = self._result
        if r is None:
            raise StopAsyncIteration
        self._result = None
        return r


class RecordStream:
    """Async iterator of :class:`~aerospike_sdk.record_result.RecordResult` rows.

    Produced by ``await session.query(...).execute()`` and similar APIs. Prefer
    ``async for row in stream``, or helpers such as :meth:`collect` and
    :meth:`first`. Do not call ``RecordStream(...)`` directly; use factories
    like :meth:`from_list` or :meth:`from_batch_records`.

    Example:
        Typical consumption with ``async for``::

            stream = await session.query(key).bins(["name"]).execute()
            async for row in stream:
                if row.is_ok and row.record:
                    print(row.record.bins)

    See Also:
        :meth:`first_or_raise`: Assert a single OK row.
    """

    __slots__ = (
        "_source", "_closed", "_single_result",
        # Chunked-iteration state (set lazily by from_chunked_recordset).
        # Slots so set-after-init is allowed without per-instance dict.
        "_chunked", "_chunk_first", "_chunk_recordset",
        "_chunk_reexecute", "_chunk_limit", "_chunk_count", "_counter_ref",
    )

    def __init__(self, source: AsyncIterator[RecordResult]) -> None:
        self._source = source
        self._closed = False
        # Fast-path cache for single-result streams: avoids async
        # iteration overhead in first() / first_or_raise() / __anext__.
        self._single_result: RecordResult | None = None
        # Chunked fields lazily initialized: from_chunked_recordset is the
        # only path that touches them. has_more_chunks() reads via getattr
        # so a freshly-constructed stream needs no extra writes here.
        self._chunked = False
        self._chunk_first = True

    # -- factory constructors ------------------------------------------------

    @classmethod
    def from_list(cls, results: Sequence[RecordResult]) -> RecordStream:
        """Wrap an already-materialised list of results.

        Example::
            stream = RecordStream.from_list([row1, row2])
            rows = await stream.collect()
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            for r in results:
                yield r
        return cls(_iter())

    @classmethod
    def chain(cls, streams: Sequence[RecordStream]) -> RecordStream:
        """Yield all results from each stream in order.

        Example::
            combined = RecordStream.chain([stream_a, stream_b])
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            for st in streams:
                async for r in st:
                    yield r
        return cls(_iter())

    @classmethod
    def from_batch_records(cls, batch_records: Sequence) -> RecordStream:
        """Wrap a sequence of async-client ``BatchRecord`` objects.

        Example::
            stream = RecordStream.from_batch_records(batch_records)
        """
        return cls.from_list(batch_records_to_results(list(batch_records)))

    @classmethod
    def from_pac_batch_stream(
        cls, pac_stream: Any, on_error: ErrorHandler | None = None,
    ) -> RecordStream:
        """Lazy-feed adapter over a PAC ``BatchRecordStream``.

        The PAC stream yields ``(idx, BatchRecord)`` tuples in completion
        order (the node that responds first yields first), not input order.
        ``idx`` is the position of the originating op in the input ops list;
        it's mapped to :attr:`RecordResult.index` so positional consumers
        can still recover input order via ``stream.collect()`` followed by
        ``results.sort(key=lambda r: r.index)`` if needed.

        Per-key errors land on each ``BatchRecord.result_code`` and surface
        as :class:`RecordResult` with ``is_ok=False``. Cluster-level errors
        raise from ``__anext__`` and are converted to PSDK exceptions via
        :func:`_convert_pac_exception`.

        Args:
            pac_stream: PAC ``BatchRecordStream`` to drain.
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, per-key failures are dispatched to the handler
                and excluded from the returned stream; cluster-level
                errors still raise from ``__anext__``.
        """
        from aerospike_sdk.exceptions import _convert_pac_exception, _result_code_to_exception

        async def _iter() -> AsyncIterator[RecordResult]:
            try:
                async for idx, br in pac_stream:
                    rc = (
                        br.result_code
                        if br.result_code is not None
                        else ResultCode.OK
                    )
                    if on_error is not None and rc != ResultCode.OK:
                        on_error(br.key, idx, _result_code_to_exception(
                            rc, str(rc), br.in_doubt))
                        continue
                    yield RecordResult(
                        key=br.key,
                        record=br.record,
                        result_code=rc,
                        in_doubt=br.in_doubt,
                        index=idx,
                    )
            except Exception as e:
                raise _convert_pac_exception(e) from e

        return cls(_iter())

    @classmethod
    def from_recordset(cls, recordset) -> RecordStream:
        """Wrap a ``Recordset`` (async iterable of ``Record``).

        Each yielded ``Record`` is converted to a :class:`RecordResult` with
        ``result_code=OK`` and ``index=-1`` (queries have no positional index).

        Example::
            stream = RecordStream.from_recordset(recordset)
        """
        async def _iter() -> AsyncIterator[RecordResult]:
            async for record in recordset:
                key = record.key if hasattr(record, "key") and record.key is not None else Key("", "", 0)
                yield RecordResult(
                    key=key,
                    record=record,
                    result_code=ResultCode.OK,
                )
        return cls(_iter())

    @classmethod
    def from_chunked_recordset(
        cls,
        recordset: Any,
        reexecute: Callable[[PartitionFilter], Awaitable[Any]],
        limit: int = 0,
    ) -> RecordStream:
        """Wrap a ``Recordset`` for chunked iteration.

        The stream yields records from the current chunk.  Call
        :meth:`has_more_chunks` to advance to the next server chunk.

        Args:
            recordset: The PAC ``Recordset`` from the first query call.
            reexecute: An async callable that accepts an updated
                ``PartitionFilter`` and returns a new ``Recordset``.
            limit: Optional overall record limit (0 = unlimited).
        """
        stream = cls._make_chunk_iter(recordset, limit, 0)
        stream._chunked = True
        stream._chunk_recordset = recordset
        stream._chunk_reexecute = reexecute
        stream._chunk_limit = limit
        return stream

    @classmethod
    def _make_chunk_iter(
        cls, recordset: Any, limit: int, already_counted: int,
    ) -> RecordStream:
        """Build a RecordStream that counts records against *limit*."""
        counter = [already_counted]

        async def _iter() -> AsyncIterator[RecordResult]:
            async for record in recordset:
                if 0 < limit <= counter[0]:
                    break
                key = (
                    record.key
                    if hasattr(record, "key") and record.key is not None
                    else Key("", "", 0)
                )
                counter[0] += 1
                yield RecordResult(
                    key=key, record=record, result_code=ResultCode.OK,
                )

        inst = cls(_iter())
        inst._chunk_count = already_counted
        inst._counter_ref = counter
        return inst

    @classmethod
    def from_single(cls, key: Key, record: Record | None) -> RecordStream:
        """Wrap a single-key result.

        Example::
            stream = RecordStream.from_single(key, record)
        """
        rc = ResultCode.OK if record is not None else ResultCode.KEY_NOT_FOUND_ERROR
        result = RecordResult(key=key, record=record, result_code=rc, index=0)
        # Skip the _SingleResultIter allocation; __anext__ short-circuits via
        # _single_result instead. Saves an iterator + frame per single-key op.
        inst = cls.__new__(cls)
        inst._source = None  # type: ignore[assignment]
        inst._closed = False
        inst._single_result = result
        inst._chunked = False
        inst._chunk_first = True
        return inst

    @classmethod
    def from_error(
        cls,
        key: Key,
        result_code: ResultCode,
        in_doubt: bool = False,
        exception: AerospikeError | None = None,
    ) -> RecordStream:
        """Wrap a single-key error as a one-element stream.

        Example::
            stream = RecordStream.from_error(key, ResultCode.TIMEOUT)
        """
        return cls.from_list([RecordResult(
            key=key,
            record=None,
            result_code=result_code,
            in_doubt=in_doubt,
            index=0,
            exception=exception,
        )])

    # -- async iteration -----------------------------------------------------

    def __aiter__(self) -> RecordStream:
        """Return ``self`` for ``async for`` iteration."""
        return self

    async def __anext__(self) -> RecordResult:
        """Yield the next :class:`~aerospike_sdk.record_result.RecordResult`.

        Raises:
            StopAsyncIteration: When the stream is exhausted or :meth:`close` was
                called before more rows were read.
        """
        if self._closed:
            raise StopAsyncIteration
        # Single-result fast path: from_single skips iterator allocation
        # and stashes the result in _single_result. Drain it here without
        # hitting an underlying iterator.
        r = self._single_result
        if r is not None:
            self._single_result = None
            self._closed = True
            return r
        return await self._source.__anext__()

    # -- chunked iteration ---------------------------------------------------

    async def has_more_chunks(self) -> bool:
        """Check whether more server-side chunks remain.

        On the first call this returns ``True`` so the caller enters the
        iteration loop for the already-loaded first chunk.  Subsequent calls
        inspect the server's ``PartitionFilter`` cursor: if more partitions
        remain, a new query round-trip is issued transparently and ``True``
        is returned.

        Returns ``False`` when:
        * the server cursor is done (all partitions scanned), or
        * the overall ``limit`` has been reached, or
        * the stream was not created with :meth:`from_chunked_recordset`.

        Example::

            stream = await session.query(SET).chunk_size(10).execute()
            chunk = 0
            while await stream.has_more_chunks():
                chunk += 1
                print(f"Chunk: {chunk}")
                async for rr in stream:
                    print(rr.record.bins)
        """
        if not self._chunked:
            if self._chunk_first:
                self._chunk_first = False
                return True
            return False

        if self._chunk_first:
            self._chunk_first = False
            return True

        if 0 < self._chunk_limit <= self._chunk_count:
            return False

        pf = await self._chunk_recordset.partition_filter()
        if pf is None or pf.done():
            return False

        counted_so_far = self._counter_ref[0]
        log.debug("fetching next chunk (counted=%d)", counted_so_far)
        if self._chunk_reexecute is None:
            return False
        recordset = await self._chunk_reexecute(pf)
        self._chunk_recordset = recordset
        self._chunk_count = counted_so_far

        new_stream = self._make_chunk_iter(
            recordset, self._chunk_limit, counted_so_far,
        )
        self._source = new_stream._source
        self._counter_ref = new_stream._counter_ref
        self._closed = False
        return True

    # -- convenience methods -------------------------------------------------

    async def first(self) -> RecordResult | None:
        """Consume and return the first row, or ``None`` if there are no rows.

        Returns:
            The first :class:`~aerospike_sdk.record_result.RecordResult`, or
            ``None`` when the stream is empty.

        Note:
            This advances the iterator; remaining rows are left for further
            ``async for`` or other helpers only if the underlying source allows
            partial consumption (most SDK streams are single-pass).

        Example::

            stream = await session.query(key).execute()
            row = await stream.first()
            if row is None:
                ...
        """
        # Fast path: skip async iteration for single-result streams.
        r = self._single_result
        if r is not None:
            self._single_result = None
            self._closed = True
            return r
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def first_or_raise(self) -> RecordResult:
        """Return the first row and require success (see :meth:`RecordResult.or_raise`).

        Returns:
            The first OK :class:`~aerospike_sdk.record_result.RecordResult`.

        Raises:
            StopAsyncIteration: If the stream yields no rows (empty).
            AerospikeError: If the first row is not OK (from :meth:`RecordResult.or_raise`).

        Example:
            rec = (await stream.first_or_raise()).record_or_raise()
        """
        result = await self.first()
        if result is None:
            raise StopAsyncIteration("RecordStream is empty")
        return result.or_raise()

    async def first_udf_result(self) -> Any | None:
        """Scan forward for the first non-``None`` :attr:`~aerospike_sdk.record_result.RecordResult.udf_result`.

        Returns:
            The UDF return value, or ``None`` if no row carries a UDF result.

        Example::
            value = await stream.first_udf_result()

        See Also:
            :meth:`Session.execute_udf`: Produces streams with UDF results.
        """
        async for r in self:
            if r.udf_result is not None:
                return r.udf_result
        return None

    async def collect(self) -> list[RecordResult]:
        """Drain the stream into a list (order preserved).

        Returns:
            All remaining :class:`~aerospike_sdk.record_result.RecordResult`
            instances.

        Example:
            rows = await stream.collect()
            oks = [r for r in rows if r.is_ok]
        """
        results: list[RecordResult] = []
        async for r in self:
            results.append(r)
        return results

    async def failures(self) -> list[RecordResult]:
        """Drain the stream and return rows where :attr:`~aerospike_sdk.record_result.RecordResult.is_ok` is false.

        Returns:
            Only error or non-OK rows.

        Note:
            Like :meth:`collect`, this consumes the entire stream.
        """
        return [r async for r in self if not r.is_ok]

    def close(self) -> None:
        """Mark the stream closed; further :meth:`__anext__` calls stop iteration.

        Idempotent. Use when abandoning a stream early to cooperate with
        resource cleanup where supported.
        """
        self._closed = True
