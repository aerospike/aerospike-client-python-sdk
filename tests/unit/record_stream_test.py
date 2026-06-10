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
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.exceptions import AerospikeError
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
        stream = RecordStream.from_error(
            _key(), ResultCode.TIMEOUT, in_doubt=True)
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
    """Minimal async-iterable stand-in for a PAC ``BatchRecordStream``."""

    def __init__(self, tuples):
        self._items = iter(tuples)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


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
    """Minimal async-iterable stand-in for a PAC Recordset."""

    def __init__(self, recs):
        self._recs = iter(recs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._recs)
        except StopIteration:
            raise StopAsyncIteration


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
