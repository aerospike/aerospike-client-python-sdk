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

import pytest

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.error_strategy import (
    ErrorStrategy,
    _ErrorDisposition,
    _resolve_disposition,
)
from aerospike_sdk.exceptions import AerospikeError, TimeoutError
from aerospike_sdk.record_result import RecordResult


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
        from aerospike_sdk.exceptions import GenerationError
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


# ---------------------------------------------------------------------------
# Bucket 3: Builder flag wiring
# ---------------------------------------------------------------------------

class TestBuilderFlagWiring:
    """Verify WriteSegmentBuilder flag methods set state on the QueryBuilder."""

    def _make_wsb(self):
        from unittest.mock import MagicMock
        from aerospike_sdk.aio.operations.query import QueryBuilder, WriteSegmentBuilder

        qb = QueryBuilder(
            client=MagicMock(),
            namespace="test",
            set_name="test",
        )
        qb._op_type = "upsert"
        qb._single_key = _key()
        return WriteSegmentBuilder(qb), qb

    def test_fail_on_filtered_out_sets_flag(self):
        wsb, qb = self._make_wsb()
        assert qb._fail_on_filtered_out is False
        wsb.fail_on_filtered_out()
        assert qb._fail_on_filtered_out is True

    def test_respond_all_keys_sets_flag(self):
        wsb, qb = self._make_wsb()
        assert qb._respond_all_keys is False
        wsb.respond_all_keys()
        assert qb._respond_all_keys is True

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
        result = wsb.respond_all_keys()
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
        from unittest.mock import MagicMock
        from aerospike_sdk.aio.operations.query import QueryBuilder, WriteSegmentBuilder

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

    def test_expire_record_after_seconds_zero_raises(self):
        wsb, _ = self._make_wsb()
        with pytest.raises(ValueError, match="greater than 0"):
            wsb.expire_record_after_seconds(0)

    def test_expire_record_after_seconds_negative_raises(self):
        wsb, _ = self._make_wsb()
        with pytest.raises(ValueError, match="greater than 0"):
            wsb.expire_record_after_seconds(-1)

    def test_default_expire_record_after_seconds_zero_raises(self):
        from unittest.mock import MagicMock
        from aerospike_sdk.aio.operations.query import QueryBuilder

        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="test")
        with pytest.raises(ValueError, match="greater than 0"):
            qb.default_expire_record_after_seconds(0)

    def test_bins_empty_list_raises(self):
        from unittest.mock import MagicMock
        from aerospike_sdk.aio.operations.query import QueryBuilder

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
        from aerospike_async import Expiration
        from aerospike_sdk.aio.operations.query import _to_expiration
        assert _to_expiration(-1) is Expiration.NEVER_EXPIRE

    def test_dont_update(self):
        from aerospike_async import Expiration
        from aerospike_sdk.aio.operations.query import _to_expiration
        assert _to_expiration(-2) is Expiration.DONT_UPDATE

    def test_server_default(self):
        from aerospike_async import Expiration
        from aerospike_sdk.aio.operations.query import _to_expiration
        assert _to_expiration(0) is Expiration.NAMESPACE_DEFAULT

    def test_positive_seconds(self):
        from aerospike_sdk.aio.operations.query import _to_expiration
        exp = _to_expiration(3600)
        assert exp is not None


# ---------------------------------------------------------------------------
# QueryBuilder default TTL methods
# ---------------------------------------------------------------------------

class TestDefaultTtlMethods:

    def _make_qb(self):
        from unittest.mock import MagicMock
        from aerospike_sdk.aio.operations.query import QueryBuilder
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
