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

"""Tests for SyncRecordStream close()/resource-release semantics.

The sync stream is an independent implementation (sync generators, blocking
producers), so its close-forwarding path is covered separately from the async
RecordStream rather than mirrored mechanically.
"""

from types import SimpleNamespace

import pytest

from aerospike_sdk import Key
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from aerospike_sdk.record_result import RecordResult
from aerospike_sdk.sync.record_stream import SyncRecordStream


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


class _FakeBatchStream:
    """Sync-iterable stand-in for a PAC ``BatchRecordStream`` with close()."""

    def __init__(self, tuples):
        self._items = iter(tuples)
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._items)

    def close(self):
        self.close_calls += 1


class _FakeRecordset:
    """Sync-iterable stand-in for a PAC ``Recordset`` with close()."""

    def __init__(self, recs):
        self._recs = iter(recs)
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._recs)

    def close(self):
        self.close_calls += 1


def _br(idx: int, ok: bool = True):
    return SimpleNamespace(
        key=_key(idx), record=_record() if ok else None,
        result_code=ResultCode.OK if ok else ResultCode.KEY_NOT_FOUND_ERROR,
        in_doubt=False,
    )


class TestSyncCloseReleasesProducer:

    def test_close_forwards_to_batch_stream(self):
        fake = _FakeBatchStream([(0, _br(0)), (1, _br(1))])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        first = next(stream)
        assert first.key == _key(0)
        stream.close()
        assert fake.close_calls >= 1

    def test_close_stops_iteration(self):
        fake = _FakeBatchStream([(i, _br(i)) for i in range(5)])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        stream.close()
        assert list(stream) == []

    def test_close_is_idempotent(self):
        fake = _FakeBatchStream([])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        stream.close()
        stream.close()
        stream.close()
        assert fake.close_calls == 1

    def test_full_drain_releases_recordset(self):
        fake = _FakeRecordset([
            SimpleNamespace(bins={"a": 1}, key=_key(1)),
            SimpleNamespace(bins={"b": 2}, key=_key(2)),
        ])
        stream = SyncRecordStream._from_pac_recordset(fake)
        rows = list(stream)
        assert len(rows) == 2
        assert fake.close_calls >= 1

    def test_close_on_single_key_stream_is_safe(self):
        stream = SyncRecordStream.from_single(_key(1), _record())
        stream.close()
        assert list(stream) == []

    def test_close_on_materialized_stream_is_safe(self):
        stream = SyncRecordStream.from_list([_ok_result(0), _ok_result(1)])
        stream.close()
        assert list(stream) == []


class TestSyncContextManager:
    """`with` must close the stream on exit, including on early break and on
    exception, forwarding to the underlying producer."""

    def test_normal_exit_closes(self):
        fake = _FakeBatchStream([(i, _br(i)) for i in range(3)])
        rows = []
        with SyncRecordStream._from_pac_batch_stream(fake) as stream:
            for r in stream:
                rows.append(r)
        assert len(rows) == 3
        assert fake.close_calls >= 1

    def test_early_break_closes(self):
        fake = _FakeBatchStream([(i, _br(i)) for i in range(10)])
        with SyncRecordStream._from_pac_batch_stream(fake) as stream:
            for _ in stream:
                break
        assert fake.close_calls >= 1

    def test_exception_closes_and_propagates(self):
        fake = _FakeBatchStream([(0, _br(0))])
        with pytest.raises(RuntimeError, match="boom"):
            with SyncRecordStream._from_pac_batch_stream(fake) as stream:
                for _ in stream:
                    raise RuntimeError("boom")
        assert fake.close_calls >= 1

    def test_enter_returns_self(self):
        stream = SyncRecordStream.from_list([_ok_result(0)])
        with stream as entered:
            assert entered is stream


class TestSyncPopKeepsOpen:
    """pop() / pop_or_raise() advance one row and leave the stream open."""

    def test_pop_returns_head_and_keeps_open(self):
        stream = SyncRecordStream.from_list([_ok_result(0), _ok_result(1), _ok_result(2)])
        head = stream.pop()
        assert head.index == 0
        assert [r.index for r in stream.collect()] == [1, 2]

    def test_pop_empty_returns_none(self):
        assert SyncRecordStream.from_list([]).pop() is None

    def test_pop_returns_error_as_data(self):
        row = SyncRecordStream.from_list([_fail_result()]).pop()
        assert row is not None and not row.is_ok

    def test_pop_or_raise_ok_keeps_open(self):
        stream = SyncRecordStream.from_list([_ok_result(0), _ok_result(1)])
        assert stream.pop_or_raise().index == 0
        assert [r.index for r in stream.collect()] == [1]

    def test_pop_or_raise_empty(self):
        with pytest.raises(StopIteration):
            SyncRecordStream.from_list([]).pop_or_raise()

    def test_pop_or_raise_error(self):
        with pytest.raises(AerospikeError):
            SyncRecordStream.from_list([_fail_result()]).pop_or_raise()

    def test_pop_does_not_close_producer(self):
        fake = _FakeBatchStream([(i, _br(i)) for i in range(3)])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        stream.pop()
        assert fake.close_calls == 0
        assert len(stream.collect()) == 2
        assert fake.close_calls >= 1


class TestSyncFirstIsTerminal:
    """first() / first_or_raise() take one row, then close and forward to the
    underlying producer."""

    def test_first_closes_producer(self):
        fake = _FakeBatchStream([(i, _br(i)) for i in range(5)])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        assert stream.first().index == 0
        assert fake.close_calls >= 1
        assert stream.collect() == []

    def test_first_or_raise_closes_producer(self):
        fake = _FakeBatchStream([(0, _br(0)), (1, _br(1))])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        assert stream.first_or_raise().is_ok
        assert fake.close_calls >= 1

    def test_first_or_raise_error_still_closes(self):
        fake = _FakeBatchStream([(0, _br(0, ok=False))])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        with pytest.raises(AerospikeError):
            stream.first_or_raise()
        assert fake.close_calls >= 1

    def test_first_empty_closes(self):
        fake = _FakeBatchStream([])
        stream = SyncRecordStream._from_pac_batch_stream(fake)
        assert stream.first() is None
        assert fake.close_calls >= 1
