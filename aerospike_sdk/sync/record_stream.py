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

"""SyncRecordStream — pure-sync iterator of :class:`RecordResult` rows.

Does not wrap an async :class:`RecordStream`. Sources are sync iterables —
typically a PAC :class:`Recordset`, a list of ``BatchRecord``, or a
materialized list of :class:`RecordResult`.

Factory classmethods mirror :class:`aerospike_sdk.record_stream.RecordStream`
so callers that already use ``from_list`` / ``from_batch_records`` /
``from_recordset`` / ``from_single`` / ``from_error`` / ``chain`` keep
the same shape.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Sequence

from aerospike_async import Key, ResultCode
from aerospike_sdk.record_result import RecordResult, batch_records_to_results

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from aerospike_async import Record
    from aerospike_sdk.error_strategy import ErrorHandler
    from aerospike_sdk.exceptions import AerospikeError


class SyncRecordStream:
    """Synchronous iterator of :class:`~aerospike_sdk.record_result.RecordResult`.

    Produced by sync builder terminals (``execute()``). Iterate with a
    regular ``for`` loop or use the helpers below (``first``, ``collect``,
    ``failures``, ...). Single-pass — most underlying sources do not
    support resetting.

    Example::

        for row in session.query(key).bins(["name"]).execute():
            if row.is_ok and row.record:
                print(row.record.bins)

    See Also:
        :class:`aerospike_sdk.record_stream.RecordStream`: async counterpart.
    """

    __slots__ = (
        "_source", "_closed", "_single_result",
        # Underlying closeable PAC producer (BatchRecordStream / Recordset)
        # when lazily fed from one; None for materialized sources. close()
        # forwards to it for deterministic release. See the async
        # RecordStream for the full rationale.
        "_closeable",
        # Chunked-recordset state (set by from_chunked_pac_recordset; left
        # unset for non-chunked streams). Slotted to allow assignment.
        "_chunked", "_chunk_recordset", "_chunk_reexecute",
        "_chunk_limit", "_chunk_count", "_chunk_first", "_counter_ref",
    )

    def __init__(self, source: Iterator[RecordResult]) -> None:
        self._source = source
        self._closed = False
        self._single_result: Optional[RecordResult] = None
        self._closeable: Any = None

    # -- factory constructors ------------------------------------------------

    @classmethod
    def from_list(cls, results: Sequence[RecordResult]) -> "SyncRecordStream":
        """Wrap an already-materialized list of results."""
        return cls(iter(results))

    @classmethod
    def from_batch_records(cls, batch_records: Sequence) -> "SyncRecordStream":
        """Wrap a list of PAC ``BatchRecord`` objects."""
        return cls.from_list(batch_records_to_results(list(batch_records)))

    @classmethod
    def from_pac_batch_stream(
        cls, pac_stream: Any, on_error: "ErrorHandler | None" = None,
    ) -> "SyncRecordStream":
        """Lazy-feed adapter over a PAC ``BatchRecordStream`` (sync iter).

        See :meth:`aerospike_sdk.record_stream.RecordStream.from_pac_batch_stream`
        for the contract. This sync variant pulls ``(idx, BatchRecord)``
        tuples via PAC's blocking ``__iter__``/``__next__`` and maps each
        to a :class:`RecordResult` with ``index=idx`` (NOT enumeration —
        completion order can differ from input order).

        Args:
            pac_stream: PAC ``BatchRecordStream`` (sync iter) to drain.
            on_error: Optional ``(key, index, exception) -> None`` callback.
                When set, per-key failures are dispatched to the handler
                and excluded from the returned stream; cluster-level errors
                still raise from ``__next__``.
        """
        from aerospike_sdk.exceptions import _convert_pac_exception, _result_code_to_exception

        def _gen() -> Iterator[RecordResult]:
            try:
                for idx, br in pac_stream:
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
            finally:
                # Release on exhaustion / early GeneratorExit even without an
                # explicit close() (e.g. the consumer breaks out of the loop).
                pac_stream.close()

        inst = cls(_gen())
        inst._closeable = pac_stream
        return inst

    @classmethod
    def from_pac_recordset(cls, recordset: Any) -> "SyncRecordStream":
        """Wrap a PAC ``Recordset`` (sync ``__iter__`` / ``__next__``).

        Each yielded ``Record`` becomes an OK :class:`RecordResult` with
        ``index=-1`` (queries have no positional index).
        """
        def _gen() -> Iterator[RecordResult]:
            try:
                for record in recordset:
                    key = (
                        record.key
                        if hasattr(record, "key") and record.key is not None
                        else Key("", "", 0)
                    )
                    yield RecordResult(
                        key=key, record=record, result_code=ResultCode.OK,
                    )
            finally:
                recordset.close()
        inst = cls(_gen())
        inst._closeable = recordset
        return inst

    @classmethod
    def from_chunked_pac_recordset(
        cls,
        recordset: Any,
        reexecute: Callable[[Any], Any],
        limit: int = 0,
    ) -> "SyncRecordStream":
        """Wrap a PAC ``Recordset`` for chunked iteration.

        ``reexecute`` is a *sync* callable that takes the current
        :class:`PartitionFilter` and returns the next ``Recordset`` (or
        ``None`` to stop). Use :meth:`has_more_chunks` to advance.
        """
        inst = cls(iter([]))  # placeholder source; replaced below
        inst._chunked = True  # type: ignore[attr-defined]
        inst._chunk_recordset = recordset  # type: ignore[attr-defined]
        inst._chunk_reexecute = reexecute  # type: ignore[attr-defined]
        inst._chunk_limit = limit  # type: ignore[attr-defined]
        inst._chunk_count = 0  # type: ignore[attr-defined]
        inst._chunk_first = True  # type: ignore[attr-defined]
        inst._counter_ref = [0]  # type: ignore[attr-defined]
        inst._source = _chunked_iter(recordset, limit, inst._counter_ref)  # type: ignore[attr-defined]
        # No finally-close in _chunked_iter: has_more_chunks reads the
        # recordset cursor after each chunk exhausts, so it must outlive
        # iteration. Release happens on chunk advance and in close().
        inst._closeable = recordset
        return inst

    @classmethod
    def from_single(
        cls, key: Key, record: Optional["Record"],
    ) -> "SyncRecordStream":
        """Wrap a single-key result.

        Sets ``result_code = OK`` when ``record is not None``; otherwise
        ``KEY_NOT_FOUND_ERROR``.
        """
        rc = ResultCode.OK if record is not None else ResultCode.KEY_NOT_FOUND_ERROR
        result = RecordResult(key=key, record=record, result_code=rc, index=0)
        inst = cls(iter([result]))
        inst._single_result = result
        return inst

    @classmethod
    def from_error(
        cls,
        key: Key,
        result_code: ResultCode,
        in_doubt: bool = False,
        exception: "Optional[AerospikeError]" = None,
    ) -> "SyncRecordStream":
        """Wrap a single-key error as a one-element stream."""
        return cls.from_list([RecordResult(
            key=key,
            record=None,
            result_code=result_code,
            in_doubt=in_doubt,
            index=0,
            exception=exception,
        )])

    @classmethod
    def chain(cls, streams: Sequence["SyncRecordStream"]) -> "SyncRecordStream":
        """Yield all results from each stream in order."""
        def _gen() -> Iterator[RecordResult]:
            for st in streams:
                yield from st
        return cls(_gen())

    # -- sync iteration ------------------------------------------------------

    def __iter__(self) -> "SyncRecordStream":
        return self

    def __next__(self) -> RecordResult:
        if self._closed:
            raise StopIteration
        return next(self._source)

    # -- convenience helpers -------------------------------------------------

    def pop(self) -> Optional[RecordResult]:
        """Advance one row and return it (or ``None``), keeping the stream open.

        Non-closing counterpart of :meth:`first`. Per-record failures come back
        as data on the :class:`RecordResult` (``is_ok=False``); cluster-level
        errors raise from iteration.
        """
        r = self._single_result
        if r is not None:
            self._single_result = None
            self._closed = True
            return r
        try:
            return next(self)
        except StopIteration:
            return None

    def pop_or_raise(self) -> RecordResult:
        """Advance one row and require success, keeping the stream open.

        Non-closing counterpart of :meth:`first_or_raise`.
        """
        result = self.pop()
        if result is None:
            raise StopIteration("SyncRecordStream is empty")
        return result.or_raise()

    def first(self) -> Optional[RecordResult]:
        """Return the first row (or ``None`` if empty), then close the stream.

        Terminal convenience: takes one row via :meth:`pop`, then :meth:`close`\\ s
        to release the underlying producer. Use :meth:`pop` to keep iterating.
        """
        try:
            return self.pop()
        finally:
            self.close()

    def first_or_raise(self) -> RecordResult:
        """Return the first row, require success, then close the stream.

        Terminal counterpart of :meth:`pop_or_raise`.
        """
        result = self.first()
        if result is None:
            raise StopIteration("SyncRecordStream is empty")
        return result.or_raise()

    def first_udf_result(self) -> Any | None:
        """Scan forward for the first non-``None`` ``udf_result``."""
        for r in self:
            if r.udf_result is not None:
                return r.udf_result
        return None

    def collect(self) -> list[RecordResult]:
        """Drain the stream into a list."""
        return list(self)

    def failures(self) -> list[RecordResult]:
        """Drain the stream, returning only rows whose ``is_ok`` is false."""
        return [r for r in self if not r.is_ok]

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> "SyncRecordStream":
        """Enter a ``with`` block, returning the stream itself.

        Pairs with :meth:`__exit__` so the stream is always :meth:`close`\\ d
        on block exit — the recommended way to consume a lazy stream::

            with session.query(keys).execute_stream() as stream:
                for row in stream:
                    ...
            # close() runs here, even on early break or exception.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the stream on ``with`` exit; never suppresses exceptions."""
        self.close()

    def close(self) -> None:
        """Stop iteration and release the underlying producer.

        Marks the stream closed so further iteration raises ``StopIteration``,
        and — for lazily-fed streams (a batch ``execute_stream`` or a query
        recordset) — forwards to the underlying producer's ``close()`` so its
        receiver / server-side scan is released now rather than at
        garbage-collection time. In-flight batch requests still complete in the
        background. Idempotent; always call it when abandoning a lazy stream
        before it is fully drained.
        """
        self._closed = True
        closeable = self._closeable
        if closeable is not None:
            self._closeable = None
            try:
                closeable.close()
            except Exception:
                log.debug("underlying stream close() raised", exc_info=True)

    # -- chunked iteration (rare, only used by partition-resumable queries) --

    def has_more_chunks(self) -> bool:
        """Return whether more server-side chunks remain.

        First call always returns ``True`` so the caller enters the loop
        for the already-loaded first chunk. Subsequent calls inspect the
        cursor; new chunks are fetched transparently via ``reexecute``.
        """
        chunked = getattr(self, "_chunked", False)
        if not chunked:
            first = getattr(self, "_chunk_first", True)
            if first:
                self._chunk_first = False  # type: ignore[attr-defined]
                return True
            return False

        if self._chunk_first:  # type: ignore[attr-defined]
            self._chunk_first = False  # type: ignore[attr-defined]
            return True

        if 0 < self._chunk_limit <= self._chunk_count:  # type: ignore[attr-defined]
            return False

        # PAC Recordset's partition_filter() is currently async. For the
        # sync path we rely on the recordset object to expose a
        # `partition_filter_sync()` method, OR the reexecute callable to
        # handle the cursor advance internally and return None when done.
        pf_getter = getattr(self._chunk_recordset, "partition_filter_sync", None)  # type: ignore[attr-defined]
        if pf_getter is None:
            return False
        pf = pf_getter()
        if pf is None or pf.done():
            return False

        counted_so_far = self._counter_ref[0]  # type: ignore[attr-defined]
        if self._chunk_reexecute is None:  # type: ignore[attr-defined]
            return False
        new_recordset = self._chunk_reexecute(pf)  # type: ignore[attr-defined]
        if new_recordset is None:
            return False
        # The prior chunk's recordset is consumed and its cursor read;
        # release it now rather than at GC time before adopting the new one.
        prior = self._closeable
        if prior is not None and prior is not new_recordset:
            prior.close()
        self._chunk_recordset = new_recordset  # type: ignore[attr-defined]
        self._closeable = new_recordset
        self._chunk_count = counted_so_far  # type: ignore[attr-defined]
        self._source = _chunked_iter(
            new_recordset, self._chunk_limit, self._counter_ref,  # type: ignore[attr-defined]
        )
        self._closed = False
        return True


def _chunked_iter(
    recordset: Any, limit: int, counter: list,
) -> Iterator[RecordResult]:
    """Iterator that counts records and stops at ``limit`` (0 = unlimited)."""
    for record in recordset:
        if 0 < limit <= counter[0]:
            return
        key = (
            record.key
            if hasattr(record, "key") and record.key is not None
            else Key("", "", 0)
        )
        counter[0] += 1
        yield RecordResult(
            key=key, record=record, result_code=ResultCode.OK,
        )
