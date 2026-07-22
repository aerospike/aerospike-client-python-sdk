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

"""Tests for RecordStream."""

import pytest
from types import SimpleNamespace
from typing import AsyncIterator

from aerospike_async import Key
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from aerospike_sdk.record_result import RecordResult
from aerospike_sdk.record_stream import RecordStream


def _key(val: int = 1) -> Key:
    return Key("test", "test", val)


def _record(**bins: object):
    return SimpleNamespace(bins=bins or {"a": 1})


def _ok_result(idx: int = 0) -> RecordResult:
    return RecordResult(
        key=_key(idx), record=_record(), result_code=ResultCode.OK, index=idx,
    )


def _fail_result(idx: int = 0) -> RecordResult:
    return RecordResult(
        key=_key(idx), record=None,
        result_code=ResultCode.KEY_NOT_FOUND_ERROR, index=idx,
    )


# ---------------------------------------------------------------------------
# from_list
# ---------------------------------------------------------------------------

class TestFromList:

    async def test_iterates_all(self):
        items = [_ok_result(0), _ok_result(1), _ok_result(2)]
        stream = RecordStream.from_list(items)
        collected = await stream.collect()
        assert len(collected) == 3
        assert [r.index for r in collected] == [0, 1, 2]

    async def test_empty_list(self):
        stream = RecordStream.from_list([])
        collected = await stream.collect()
        assert collected == []


# ---------------------------------------------------------------------------
# from_single
# ---------------------------------------------------------------------------

class TestFromSingle:

    async def test_found_record(self):
        rec = _record(x=1)
        stream = RecordStream.from_single(_key(), rec)
        results = await stream.collect()
        assert len(results) == 1
        assert results[0].is_ok
        assert results[0].record is rec

    async def test_not_found(self):
        stream = RecordStream.from_single(_key(), None)
        results = await stream.collect()
        assert len(results) == 1
        assert not results[0].is_ok
        assert results[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR


# ---------------------------------------------------------------------------
# from_error
# ---------------------------------------------------------------------------

class TestFromError:

    async def test_wraps_error_as_single_result(self):
        stream = RecordStream.from_error(_key(), ResultCode.KEY_NOT_FOUND_ERROR)
        results = await stream.collect()
        assert len(results) == 1
        assert not results[0].is_ok
        assert results[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR
        assert results[0].record is None
        assert results[0].in_doubt is False

    async def test_preserves_in_doubt(self):
        stream = RecordStream.from_error(_key(), ResultCode.TIMEOUT, in_doubt=True)
        results = await stream.collect()
        assert results[0].in_doubt is True


# ---------------------------------------------------------------------------
# from_batch_records
# ---------------------------------------------------------------------------

class TestFromBatchRecords:

    async def test_converts_and_iterates(self):
        br1 = SimpleNamespace(
            key=_key(1), record=_record(),
            result_code=ResultCode.OK, in_doubt=False,
        )
        br2 = SimpleNamespace(
            key=_key(2), record=None,
            result_code=ResultCode.KEY_NOT_FOUND_ERROR, in_doubt=False,
        )

        stream = RecordStream.from_batch_records([br1, br2])
        results = await stream.collect()
        assert len(results) == 2
        assert results[0].is_ok
        assert not results[1].is_ok


class _FakeBatchStream:
    """Minimal async-iterable stand-in for a PAC ``BatchRecordStream``.

    Models the real producer's ``close()`` contract so resource-release
    behavior can be asserted.
    """

    def __init__(self, tuples):
        self._items = iter(tuples)
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration

    def close(self):
        self.close_calls += 1


class TestFromPacBatchStreamOnError:
    """``from_pac_batch_stream(on_error=...)`` routes non-OK BatchRecords to
    the callback and excludes them from the yielded stream."""

    async def test_no_handler_includes_failures(self):
        br_ok = SimpleNamespace(
            key=_key(1), record=_record(),
            result_code=ResultCode.OK, in_doubt=False,
        )
        br_fail = SimpleNamespace(
            key=_key(2), record=None,
            result_code=ResultCode.KEY_NOT_FOUND_ERROR, in_doubt=False,
        )
        stream = RecordStream.from_pac_batch_stream(
            _FakeBatchStream([(0, br_ok), (1, br_fail)]),
        )
        results = await stream.collect()
        assert len(results) == 2
        assert results[0].is_ok and not results[1].is_ok

    async def test_handler_excludes_failures_and_receives_args(self):
        br_ok = SimpleNamespace(
            key=_key(1), record=_record(),
            result_code=ResultCode.OK, in_doubt=False,
        )
        br_fail = SimpleNamespace(
            key=_key(2), record=None,
            result_code=ResultCode.KEY_NOT_FOUND_ERROR, in_doubt=False,
        )

        captured: list = []
        stream = RecordStream.from_pac_batch_stream(
            _FakeBatchStream([(0, br_ok), (1, br_fail)]),
            on_error=lambda k, i, e: captured.append((k, i, e)),
        )
        results = await stream.collect()

        assert len(results) == 1
        assert results[0].key == _key(1)
        assert len(captured) == 1
        k, i, exc = captured[0]
        assert k == _key(2) and i == 1
        assert exc.result_code == ResultCode.KEY_NOT_FOUND_ERROR


# ---------------------------------------------------------------------------
# from_recordset
# ---------------------------------------------------------------------------

class _FakeRecordset:
    """Minimal async-iterable stand-in for a PAC Recordset.

    Models the real producer's ``close()`` contract so resource-release
    behavior can be asserted.
    """

    def __init__(self, recs):
        self._recs = iter(recs)
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._recs)
        except StopIteration:
            raise StopAsyncIteration

    def close(self):
        self.close_calls += 1


class TestFromRecordset:

    async def test_wraps_async_iterable(self):
        rec1 = SimpleNamespace(bins={"a": 1}, key=_key(1))
        rec2 = SimpleNamespace(bins={"b": 2}, key=_key(2))

        stream = RecordStream.from_recordset(_FakeRecordset([rec1, rec2]))
        results = await stream.collect()
        assert len(results) == 2
        assert all(r.is_ok for r in results)
        assert results[0].record is rec1

    async def test_empty_recordset(self):
        stream = RecordStream.from_recordset(_FakeRecordset([]))
        assert await stream.collect() == []

    async def test_fallback_key_when_no_key_attribute(self):
        rec = SimpleNamespace(bins={"x": 1})
        stream = RecordStream.from_recordset(_FakeRecordset([rec]))
        results = await stream.collect()
        assert len(results) == 1
        assert results[0].record is rec
        assert results[0].key == Key("", "", 0)


# ---------------------------------------------------------------------------
# first / first_or_raise
# ---------------------------------------------------------------------------

class TestFirst:

    async def test_first_returns_item(self):
        stream = RecordStream.from_list([_ok_result(0), _ok_result(1)])
        result = await stream.first()
        assert result is not None
        assert result.index == 0

    async def test_first_empty_returns_none(self):
        stream = RecordStream.from_list([])
        assert await stream.first() is None

    async def test_first_or_raise_ok(self):
        stream = RecordStream.from_list([_ok_result()])
        result = await stream.first_or_raise()
        assert result.is_ok

    async def test_first_or_raise_empty(self):
        stream = RecordStream.from_list([])
        with pytest.raises(StopAsyncIteration):
            await stream.first_or_raise()

    async def test_first_or_raise_error(self):
        stream = RecordStream.from_list([_fail_result()])
        with pytest.raises(AerospikeError):
            await stream.first_or_raise()


# ---------------------------------------------------------------------------
# pop (keep-open) vs first (terminal) — the 2x2
# ---------------------------------------------------------------------------

class TestPopKeepsOpen:
    """pop() / pop_or_raise() advance one row and leave the stream open."""

    async def test_pop_returns_head_and_keeps_open(self):
        stream = RecordStream.from_list([_ok_result(0), _ok_result(1), _ok_result(2)])
        head = await stream.pop()
        assert head.index == 0
        rest = await stream.collect()
        assert [r.index for r in rest] == [1, 2]

    async def test_pop_empty_returns_none(self):
        stream = RecordStream.from_list([])
        assert await stream.pop() is None

    async def test_pop_returns_error_as_data(self):
        # A non-OK row comes back as an envelope, not a raise.
        stream = RecordStream.from_list([_fail_result()])
        row = await stream.pop()
        assert row is not None and not row.is_ok

    async def test_pop_or_raise_ok_keeps_open(self):
        stream = RecordStream.from_list([_ok_result(0), _ok_result(1)])
        head = await stream.pop_or_raise()
        assert head.index == 0
        assert [r.index for r in await stream.collect()] == [1]

    async def test_pop_or_raise_empty(self):
        stream = RecordStream.from_list([])
        with pytest.raises(StopAsyncIteration):
            await stream.pop_or_raise()

    async def test_pop_or_raise_error(self):
        stream = RecordStream.from_list([_fail_result()])
        with pytest.raises(AerospikeError):
            await stream.pop_or_raise()

    async def test_pop_does_not_close_producer(self):
        fake = _FakeBatchStream([
            (i, SimpleNamespace(key=_key(i), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False))
            for i in range(3)
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        await stream.pop()
        assert fake.close_calls == 0          # still open
        rest = await stream.collect()          # drains → generator finally closes
        assert len(rest) == 2
        assert fake.close_calls >= 1


class TestFirstIsTerminal:
    """first() / first_or_raise() take one row, then close the stream and
    forward to the underlying producer."""

    async def test_first_closes_producer(self):
        fake = _FakeBatchStream([
            (i, SimpleNamespace(key=_key(i), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False))
            for i in range(5)
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        head = await stream.first()
        assert head.index == 0
        assert fake.close_calls >= 1           # closed by first()
        # And the stream is done: no more rows.
        assert await stream.collect() == []

    async def test_first_or_raise_closes_producer(self):
        fake = _FakeBatchStream([
            (0, SimpleNamespace(key=_key(0), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False)),
            (1, SimpleNamespace(key=_key(1), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False)),
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        head = await stream.first_or_raise()
        assert head.is_ok
        assert fake.close_calls >= 1

    async def test_first_or_raise_error_still_closes(self):
        fake = _FakeBatchStream([
            (0, SimpleNamespace(key=_key(0), record=None,
                                result_code=ResultCode.KEY_NOT_FOUND_ERROR,
                                in_doubt=False)),
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        with pytest.raises(AerospikeError):
            await stream.first_or_raise()
        # close() ran despite the raise (first() closes in a finally).
        assert fake.close_calls >= 1

    async def test_first_empty_closes(self):
        fake = _FakeBatchStream([])
        stream = RecordStream.from_pac_batch_stream(fake)
        assert await stream.first() is None
        assert fake.close_calls >= 1


# ---------------------------------------------------------------------------
# failures
# ---------------------------------------------------------------------------

class TestFailures:

    async def test_filters_to_non_ok(self):
        items = [_ok_result(0), _fail_result(1), _ok_result(2), _fail_result(3)]
        stream = RecordStream.from_list(items)
        fails = await stream.failures()
        assert len(fails) == 2
        assert [f.index for f in fails] == [1, 3]

    async def test_no_failures(self):
        stream = RecordStream.from_list([_ok_result()])
        fails = await stream.failures()
        assert fails == []


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestClose:

    async def test_close_stops_iteration(self):
        stream = RecordStream.from_list([_ok_result(), _ok_result()])
        stream.close()
        collected = await stream.collect()
        assert collected == []

    async def test_close_is_idempotent(self):
        stream = RecordStream.from_list([_ok_result()])
        stream.close()
        stream.close()  # should not raise
        assert await stream.collect() == []


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------

class TestErrorPropagation:

    async def test_source_error_propagates(self):
        async def _exploding() -> AsyncIterator[RecordResult]:
            yield _ok_result(0)
            raise RuntimeError("boom")

        stream = RecordStream(_exploding())
        results = []
        with pytest.raises(RuntimeError, match="boom"):
            async for r in stream:
                results.append(r)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# Stream exhaustion
# ---------------------------------------------------------------------------

class TestExhaustion:

    async def test_stream_exhausted_after_collect(self):
        stream = RecordStream.from_list([_ok_result(), _ok_result()])
        first = await stream.collect()
        second = await stream.collect()
        assert len(first) == 2
        assert second == []


# ---------------------------------------------------------------------------
# close() resource release
# ---------------------------------------------------------------------------

class TestCloseReleasesProducer:
    """close() must forward to the underlying PAC producer (deterministic
    release) and stop further iteration."""

    async def test_close_forwards_to_batch_stream(self):
        fake = _FakeBatchStream([
            (0, SimpleNamespace(key=_key(1), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False)),
            (1, SimpleNamespace(key=_key(2), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False)),
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        # Consume one, then abandon.
        first = await stream.__anext__()
        assert first.key == _key(1)
        stream.close()
        assert fake.close_calls >= 1

    async def test_close_stops_iteration(self):
        fake = _FakeBatchStream([
            (i, SimpleNamespace(key=_key(i), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False))
            for i in range(5)
        ])
        stream = RecordStream.from_pac_batch_stream(fake)
        stream.close()
        assert await stream.collect() == []

    async def test_close_is_idempotent(self):
        fake = _FakeBatchStream([])
        stream = RecordStream.from_pac_batch_stream(fake)
        stream.close()
        stream.close()
        stream.close()
        # Exactly one forward despite repeated close() (producer ref cleared).
        assert fake.close_calls == 1

    async def test_full_drain_releases_producer(self):
        fake = _FakeRecordset([
            SimpleNamespace(bins={"a": 1}, key=_key(1)),
            SimpleNamespace(bins={"b": 2}, key=_key(2)),
        ])
        stream = RecordStream.from_recordset(fake)
        rows = await stream.collect()
        assert len(rows) == 2
        # Draining to exhaustion releases the recordset via the generator's
        # finally, without an explicit close() call.
        assert fake.close_calls >= 1

    async def test_close_on_single_key_stream_is_safe(self):
        # from_single bypasses __init__; close() must not raise (no producer).
        stream = RecordStream.from_single(_key(1), _record())
        stream.close()
        assert await stream.collect() == []

    async def test_close_on_materialized_stream_is_safe(self):
        # from_list has no underlying producer; close() is a pure flag flip.
        stream = RecordStream.from_list([_ok_result(0), _ok_result(1)])
        stream.close()
        assert await stream.collect() == []


# ---------------------------------------------------------------------------
# async context manager
# ---------------------------------------------------------------------------

class TestAsyncContextManager:
    """`async with` must close the stream on exit, including on early break
    and on exception, forwarding to the underlying producer."""

    async def test_normal_exit_closes(self):
        fake = _FakeBatchStream([
            (i, SimpleNamespace(key=_key(i), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False))
            for i in range(3)
        ])
        rows = []
        async with RecordStream.from_pac_batch_stream(fake) as stream:
            async for r in stream:
                rows.append(r)
        assert len(rows) == 3
        assert fake.close_calls >= 1

    async def test_early_break_closes(self):
        fake = _FakeBatchStream([
            (i, SimpleNamespace(key=_key(i), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False))
            for i in range(10)
        ])
        async with RecordStream.from_pac_batch_stream(fake) as stream:
            async for _ in stream:
                break
        assert fake.close_calls >= 1

    async def test_exception_closes_and_propagates(self):
        fake = _FakeBatchStream([
            (0, SimpleNamespace(key=_key(0), record=_record(),
                                result_code=ResultCode.OK, in_doubt=False)),
        ])
        with pytest.raises(RuntimeError, match="boom"):
            async with RecordStream.from_pac_batch_stream(fake) as stream:
                async for _ in stream:
                    raise RuntimeError("boom")
        assert fake.close_calls >= 1

    async def test_aenter_returns_self(self):
        stream = RecordStream.from_list([_ok_result(0)])
        async with stream as entered:
            assert entered is stream
