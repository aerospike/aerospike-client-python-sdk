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

"""Tests for ErrorStrategy, ErrorHandler, and disposition resolution."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from aerospike_sdk import Key
from aerospike_async import Expiration, FilterExpression
from aerospike_sdk.exceptions import AerospikeError, GenerationError, ResultCode, TimeoutError

from aerospike_sdk.aio.operations.query import QueryBuilder, WriteSegmentBuilder
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder
from aerospike_sdk.error_strategy import (
    ErrorStrategy,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_sdk.operations_shared import _to_expiration
from aerospike_sdk.record_result import RecordResult


@pytest.fixture(autouse=True)
def _mock_server_compiled_ael_string(monkeypatch):
    def _fake(ael, *, supports_server_compiled_ael=True):
        return FilterExpression.from_server_compiled_ael(ael)

    monkeypatch.setattr(
        "aerospike_sdk.query_shared.filter_expression_from_ael_string",
        _fake,
    )


def _key(val: int = 1) -> Key:
    return Key("test", "test", val)


# ---------------------------------------------------------------------------
# ErrorStrategy enum
# ---------------------------------------------------------------------------

class TestErrorStrategy:

    def test_in_stream_value(self):
        assert ErrorStrategy.IN_STREAM.value == "in_stream"

    def test_is_enum(self):
        assert isinstance(ErrorStrategy.IN_STREAM, ErrorStrategy)


# ---------------------------------------------------------------------------
# _resolve_disposition
# ---------------------------------------------------------------------------

class TestResolveDisposition:

    def test_none_single_key_returns_throw(self):
        assert _resolve_disposition(None, is_single_key=True) is _ErrorDisposition.THROW

    def test_none_multi_key_returns_in_stream(self):
        assert _resolve_disposition(None, is_single_key=False) is _ErrorDisposition.IN_STREAM

    def test_in_stream_single_key_returns_in_stream(self):
        result = _resolve_disposition(ErrorStrategy.IN_STREAM, is_single_key=True)
        assert result is _ErrorDisposition.IN_STREAM

    def test_in_stream_multi_key_returns_in_stream(self):
        result = _resolve_disposition(ErrorStrategy.IN_STREAM, is_single_key=False)
        assert result is _ErrorDisposition.IN_STREAM

    def test_callable_returns_handler(self):
        def my_handler(key, index, exc):
            pass
        result = _resolve_disposition(my_handler, is_single_key=True)
        assert result is _ErrorDisposition.HANDLER

    def test_callable_multi_key_returns_handler(self):
        result = _resolve_disposition(lambda k, i, e: None, is_single_key=False)
        assert result is _ErrorDisposition.HANDLER


# ---------------------------------------------------------------------------
# RecordResult with exception field
# ---------------------------------------------------------------------------

class TestRecordResultException:

    def test_exception_defaults_to_none(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.OK)
        assert rr.exception is None

    def test_exception_stored(self):
        exc = TimeoutError("timed out")
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.TIMEOUT, exception=exc,
        )
        assert rr.exception is exc
        assert rr.is_ok is False

    def test_or_raise_uses_stored_exception(self):
        exc = TimeoutError("timed out")
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.TIMEOUT, exception=exc,
        )
        with pytest.raises(TimeoutError, match="timed out"):
            rr.or_raise()

    def test_or_raise_falls_back_to_result_code(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.GENERATION_ERROR,
        )
        with pytest.raises(GenerationError):
            rr.or_raise()

    def test_as_bool_raises_stored_exception(self):
        exc = AerospikeError("server error", result_code=ResultCode.SERVER_ERROR)
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.SERVER_ERROR, exception=exc,
        )
        with pytest.raises(AerospikeError, match="server error"):
            rr.as_bool()

    def test_record_or_raise_uses_stored_exception(self):
        exc = TimeoutError("timed out")
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.TIMEOUT, exception=exc,
        )
        with pytest.raises(TimeoutError):
            rr.record_or_raise()

    def test_client_side_error_row_is_not_ok(self):
        # An attached exception makes the row a failure whatever its code
        # says: reporting it as success would claim a write that never happened.
        exc = AerospikeError("client rejected the command")
        assert exc.result_code is None
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.OK, exception=exc, index=1,
        )
        assert rr.is_ok is False

    def test_or_raise_raises_client_side_error_despite_ok_code(self):
        exc = AerospikeError("client rejected the command")
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.OK, exception=exc, index=1,
        )
        with pytest.raises(AerospikeError, match="client rejected"):
            rr.or_raise()

    def test_as_bool_raises_client_side_error_despite_ok_code(self):
        exc = TimeoutError("client deadline expired", client=True)
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.OK, exception=exc,
        )
        with pytest.raises(TimeoutError, match="client deadline"):
            rr.as_bool()


# ---------------------------------------------------------------------------
# Error funnels: a failure without a result code
# ---------------------------------------------------------------------------

class TestCodeLessFailureRows:
    """A failure the client cannot attribute to a code (here a plain
    ``ValueError`` escaping PAC) reports ``CLIENT_ERROR``, never ``OK``."""

    async def test_single_key_in_stream_row(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        stream = qb._handle_error(
            _key(), ValueError("bad argument"), _ErrorDisposition.IN_STREAM, None,
        )
        [row] = await stream.collect()
        assert row.result_code == ResultCode.CLIENT_ERROR
        assert isinstance(row.exception, AerospikeError)

    def test_batch_in_stream_rows(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        rows = qb._handle_batch_error_list(
            [_key(1), _key(2)], ValueError("bad argument"),
            _ErrorDisposition.IN_STREAM, None,
        )
        assert [r.result_code for r in rows] == [ResultCode.CLIENT_ERROR] * 2
        assert all(not r.is_ok for r in rows)

    def test_blocking_single_key_in_stream_row(self):
        qb = SyncQueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        [row] = qb._handle_error_blocking_singlekey(
            _key(), ValueError("bad argument"), "upsert",
            _ErrorDisposition.IN_STREAM, None,
        )
        assert row.result_code == ResultCode.CLIENT_ERROR
        assert isinstance(row.exception, AerospikeError)


class TestFilteredBatchErrorDetail:
    """A batch row's failure detail reaches the raise and the handler intact."""

    def _rows(self):
        failed = SimpleNamespace(
            record=None, result_code=ResultCode.OP_NOT_APPLICABLE, in_doubt=False,
            sub_code=4, server_message="bin type mismatch", exp_trace=None,
        )
        return [failed], [_key(1)]

    def test_throw_carries_row_detail(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        with pytest.raises(AerospikeError) as excinfo:
            qb._filtered_batch_list(*self._rows(), _ErrorDisposition.THROW)
        assert excinfo.value.sub_code == 4
        assert excinfo.value.server_message == "bin type mismatch"

    def test_handler_receives_row_detail(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        captured: list = []
        out = qb._filtered_batch_list(
            *self._rows(), _ErrorDisposition.HANDLER, lambda k, i, e: captured.append(e),
        )
        assert out == []
        assert captured[0].sub_code == 4
        assert captured[0].server_message == "bin type mismatch"


# ---------------------------------------------------------------------------
# Bucket 3: Builder flag wiring
# ---------------------------------------------------------------------------

class TestBuilderFlagWiring:
    """Verify WriteSegmentBuilder flag methods set state on the QueryBuilder."""

    def _make_wsb(self):
        qb = QueryBuilder(
            client=MagicMock(),
            namespace="test",
            set_name="test",
            supports_server_compiled_ael=True,
        )
        qb._op_type = "upsert"
        qb._single_key = _key()
        return WriteSegmentBuilder(qb), qb

    def test_fail_on_filtered_out_sets_flag(self):
        wsb, qb = self._make_wsb()
        assert qb._fail_on_filtered_out is False
        wsb.fail_on_filtered_out()
        assert qb._fail_on_filtered_out is True

    def test_query_builder_include_missing_keys_sets_flag(self):
        _, qb = self._make_wsb()
        assert qb._respond_all_keys is False
        result = qb.include_missing_keys()
        assert qb._respond_all_keys is True
        assert result is qb

    def test_write_segment_include_missing_keys_sets_flag(self):
        wsb, qb = self._make_wsb()
        assert qb._respond_all_keys is False
        result = wsb.include_missing_keys()
        assert qb._respond_all_keys is True
        assert result is wsb

    def test_with_durable_delete_sets_flag(self):
        wsb, qb = self._make_wsb()
        assert qb._durable_delete is None
        wsb.with_durable_delete()
        assert qb._durable_delete is True

    def test_ensure_generation_is_sets_value(self):
        wsb, qb = self._make_wsb()
        assert qb._generation is None
        wsb.ensure_generation_is(42)
        assert qb._generation == 42

    def test_expire_record_after_seconds_sets_value(self):
        wsb, qb = self._make_wsb()
        assert qb._ttl_seconds is None
        wsb.expire_record_after_seconds(3600)
        assert qb._ttl_seconds == 3600

    def test_never_expire_sets_sentinel(self):
        wsb, qb = self._make_wsb()
        wsb.never_expire()
        assert qb._ttl_seconds == -1

    def test_with_no_change_in_expiration_sets_sentinel(self):
        wsb, qb = self._make_wsb()
        wsb.with_no_change_in_expiration()
        assert qb._ttl_seconds == -2

    def test_expiry_from_server_default_sets_sentinel(self):
        wsb, qb = self._make_wsb()
        wsb.expiry_from_server_default()
        assert qb._ttl_seconds == 0

    def test_where_sets_filter_expression(self):
        wsb, qb = self._make_wsb()
        assert qb._filter_expression is None
        wsb.where("$.v == 1")
        assert qb._filter_expression is not None

    def test_chaining_returns_self(self):
        wsb, _ = self._make_wsb()
        result = wsb.fail_on_filtered_out()
        assert result is wsb
        result = wsb.include_missing_keys()
        assert result is wsb
        result = wsb.with_durable_delete()
        assert result is wsb
        result = wsb.ensure_generation_is(1)
        assert result is wsb
        result = wsb.expire_record_after_seconds(100)
        assert result is wsb
        result = wsb.never_expire()
        assert result is wsb
        result = wsb.with_no_change_in_expiration()
        assert result is wsb
        result = wsb.expiry_from_server_default()
        assert result is wsb


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

class TestBuilderValidation:

    def _make_wsb(self):
        qb = QueryBuilder(
            client=MagicMock(),
            namespace="test",
            set_name="test",
        )
        qb._op_type = "upsert"
        qb._single_key = _key()
        return WriteSegmentBuilder(qb), qb

    def test_ensure_generation_is_zero_raises(self):
        wsb, _ = self._make_wsb()
        with pytest.raises(ValueError, match="greater than 0"):
            wsb.ensure_generation_is(0)

    def test_ensure_generation_is_negative_raises(self):
        wsb, _ = self._make_wsb()
        with pytest.raises(ValueError, match="greater than 0"):
            wsb.ensure_generation_is(-1)

    def test_ensure_generation_is_positive_succeeds(self):
        wsb, qb = self._make_wsb()
        wsb.ensure_generation_is(1)
        assert qb._generation == 1

    def test_expire_record_after_seconds_passes_sentinels_through(self):
        # 0/-1/-2 are TTL sentinels, not errors: the value is stored verbatim
        # and mapped to an Expiration when the write is built.
        wsb, qb = self._make_wsb()
        wsb.expire_record_after_seconds(0)
        assert qb._ttl_seconds == 0
        wsb.expire_record_after_seconds(-1)
        assert qb._ttl_seconds == -1

    def test_default_expire_record_after_seconds_passes_zero_through(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        qb.default_expire_record_after_seconds(0)
        assert qb._default_ttl_seconds == 0

    def test_bins_empty_list_raises(self):
        qb = QueryBuilder(
            client=MagicMock(),
            namespace="test",
            set_name="test",
        )
        with pytest.raises(ValueError, match="must not be empty"):
            qb.bins([])


# ---------------------------------------------------------------------------
# TTL special-value conversion
# ---------------------------------------------------------------------------

class TestToExpiration:

    def test_never_expire(self):
        assert _to_expiration(-1) is Expiration.NEVER_EXPIRE

    def test_dont_update(self):
        assert _to_expiration(-2) is Expiration.DONT_UPDATE

    def test_server_default(self):
        assert _to_expiration(0) is Expiration.NAMESPACE_DEFAULT

    def test_positive_seconds(self):
        exp = _to_expiration(3600)
        assert exp is not None


# ---------------------------------------------------------------------------
# QueryBuilder default TTL methods
# ---------------------------------------------------------------------------

class TestDefaultTtlMethods:

    def _make_qb(self):
        return QueryBuilder(client=MagicMock(), namespace="test", set_name="test")

    def test_default_never_expire(self):
        qb = self._make_qb()
        result = qb.default_never_expire()
        assert qb._default_ttl_seconds == -1
        assert result is qb

    def test_default_with_no_change_in_expiration(self):
        qb = self._make_qb()
        result = qb.default_with_no_change_in_expiration()
        assert qb._default_ttl_seconds == -2
        assert result is qb

    def test_default_expiry_from_server_default(self):
        qb = self._make_qb()
        result = qb.default_expiry_from_server_default()
        assert qb._default_ttl_seconds == 0
        assert result is qb
