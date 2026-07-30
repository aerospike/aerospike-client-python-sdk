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

"""Tests for RecordResult."""

import pytest
from types import SimpleNamespace

from aerospike_sdk import Key
from aerospike_sdk.exceptions import AerospikeError, GenerationError, ResultCode

from aerospike_sdk import HllConfig
from aerospike_sdk.record_result import RecordResult, batch_records_to_results
from aerospike_sdk.operation_result import OperationResult


def _key(val: int = 1) -> Key:
    return Key("test", "test", val)


def _record(**bins: object):
    return SimpleNamespace(bins=bins or {"a": 1})


def _batch_record(
    key_val: int = 1,
    record: object = None,
    result_code: ResultCode = ResultCode.OK,
    in_doubt: bool = False,
    sub_code: int | None = None,
):
    return SimpleNamespace(
        key=_key(key_val), record=record,
        result_code=result_code, in_doubt=in_doubt,
        sub_code=sub_code,
    )


# ---------------------------------------------------------------------------
# is_ok
# ---------------------------------------------------------------------------

class TestIsOk:

    def test_ok(self):
        rr = RecordResult(key=_key(), record=_record(), result_code=ResultCode.OK)
        assert rr.is_ok is True

    def test_not_ok(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.KEY_NOT_FOUND_ERROR)
        assert rr.is_ok is False


# ---------------------------------------------------------------------------
# or_raise
# ---------------------------------------------------------------------------

class TestOrRaise:

    def test_returns_self_on_ok(self):
        rr = RecordResult(key=_key(), record=_record(), result_code=ResultCode.OK)
        assert rr.or_raise() is rr

    def test_raises_typed_exception(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.GENERATION_ERROR, in_doubt=True,
        )
        with pytest.raises(GenerationError) as exc_info:
            rr.or_raise()
        assert exc_info.value.in_doubt is True

    def test_raises_base_for_unmapped_code(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.SERVER_ERROR,
        )
        with pytest.raises(AerospikeError):
            rr.or_raise()

    def test_raised_exception_carries_result_code(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.GENERATION_ERROR,
        )
        with pytest.raises(GenerationError) as exc_info:
            rr.or_raise()
        assert exc_info.value.result_code == ResultCode.GENERATION_ERROR


# ---------------------------------------------------------------------------
# record_or_raise
# ---------------------------------------------------------------------------

class TestRecordOrRaise:

    def test_returns_record_on_ok(self):
        rec = _record(x=42)
        rr = RecordResult(key=_key(), record=rec, result_code=ResultCode.OK)
        assert rr.record_or_raise() is rec

    def test_raises_on_error_code(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.GENERATION_ERROR)
        with pytest.raises(GenerationError):
            rr.record_or_raise()

    def test_raises_value_error_when_record_is_none_but_ok(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.OK)
        with pytest.raises(ValueError, match="Record is None"):
            rr.record_or_raise()


# ---------------------------------------------------------------------------
# as_bool
# ---------------------------------------------------------------------------

class TestAsBool:

    def test_true_on_ok(self):
        rr = RecordResult(key=_key(), record=_record(), result_code=ResultCode.OK)
        assert rr.as_bool() is True

    def test_false_on_key_not_found(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.KEY_NOT_FOUND_ERROR)
        assert rr.as_bool() is False

    def test_raises_on_other_error(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.GENERATION_ERROR)
        with pytest.raises(GenerationError):
            rr.as_bool()


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

class TestDefaults:

    def test_in_doubt_defaults_false(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.OK)
        assert rr.in_doubt is False

    def test_index_defaults_minus_one(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.OK)
        assert rr.index == -1


# ---------------------------------------------------------------------------
# Immutability (frozen dataclass)
# ---------------------------------------------------------------------------

class TestImmutability:

    def test_cannot_set_attribute(self):
        rr = RecordResult(key=_key(), record=_record(), result_code=ResultCode.OK)
        # `setattr` instead of direct `rr.record = ...` to bypass static
        # analyzers (PyCharm `PyDataclass`, mypy `misc`) that flag the
        # intentional frozen-dataclass mutation. Runtime behavior is
        # identical: any attribute assignment raises `FrozenInstanceError`
        # (an `AttributeError` subclass).
        with pytest.raises(AttributeError):
            setattr(rr, "record", None)


# ---------------------------------------------------------------------------
# batch_records_to_results
# ---------------------------------------------------------------------------

class TestBatchRecordsToResults:

    def test_converts_list(self):
        rec = _record()
        brs = [
            _batch_record(key_val=1, record=rec, result_code=ResultCode.OK),
            _batch_record(key_val=2, record=None, result_code=ResultCode.KEY_NOT_FOUND_ERROR),
        ]
        results = batch_records_to_results(brs)

        assert len(results) == 2
        assert results[0].is_ok
        assert results[0].record is rec
        assert results[0].index == 0
        assert not results[1].is_ok
        assert results[1].record is None
        assert results[1].index == 1

    def test_none_result_code_defaults_to_ok(self):
        br = _batch_record(result_code=None)
        results = batch_records_to_results([br])
        assert results[0].result_code == ResultCode.OK

    def test_in_doubt_propagated(self):
        br = _batch_record(in_doubt=True)
        results = batch_records_to_results([br])
        assert results[0].in_doubt is True

    def test_sub_code_propagated(self):
        br = _batch_record(result_code=ResultCode.OP_NOT_APPLICABLE, sub_code=4)
        results = batch_records_to_results([br])
        assert results[0].sub_code == 4

    def test_sub_code_defaults_to_none(self):
        results = batch_records_to_results([_batch_record()])
        assert results[0].sub_code is None

    def test_or_raise_carries_sub_code(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.OP_NOT_APPLICABLE, sub_code=4,
        )
        with pytest.raises(AerospikeError) as exc_info:
            rr.or_raise()
        assert exc_info.value.sub_code == 4

    def test_or_raise_sub_code_none_when_absent(self):
        rr = RecordResult(
            key=_key(), record=None,
            result_code=ResultCode.KEY_NOT_FOUND_ERROR,
        )
        with pytest.raises(AerospikeError) as exc_info:
            rr.or_raise()
        assert exc_info.value.sub_code is None

    def test_empty_list_returns_empty(self):
        assert batch_records_to_results([]) == []

    def test_keys_preserved(self):
        brs = [_batch_record(key_val=10), _batch_record(key_val=20)]
        results = batch_records_to_results(brs)
        assert results[0].key is brs[0].key
        assert results[1].key is brs[1].key


# ---------------------------------------------------------------------------
# get_hll_config
# ---------------------------------------------------------------------------

class TestGetHllConfig:

    def test_returns_config_from_two_element_list(self):
        rr = RecordResult(
            key=_key(), record=_record(h=[14, -1]), result_code=ResultCode.OK,
        )
        assert rr.get_hll_config("h") == HllConfig.of(14)

    def test_returns_config_with_minhash(self):
        rr = RecordResult(
            key=_key(), record=_record(h=[12, 20]), result_code=ResultCode.OK,
        )
        assert rr.get_hll_config("h") == HllConfig.of(12, 20)

    def test_returns_none_when_record_is_none(self):
        rr = RecordResult(key=_key(), record=None, result_code=ResultCode.OK)
        assert rr.get_hll_config("h") is None

    def test_returns_none_when_bin_absent(self):
        rr = RecordResult(
            key=_key(), record=_record(other=1), result_code=ResultCode.OK,
        )
        assert rr.get_hll_config("h") is None

    def test_raises_when_bin_is_not_a_list(self):
        rr = RecordResult(
            key=_key(), record=_record(h="not a list"), result_code=ResultCode.OK,
        )
        with pytest.raises(TypeError, match="not a 2-element list"):
            rr.get_hll_config("h")

    def test_raises_when_list_has_wrong_length(self):
        rr = RecordResult(
            key=_key(), record=_record(h=[1, 2, 3]), result_code=ResultCode.OK,
        )
        with pytest.raises(TypeError, match="not a 2-element list"):
            rr.get_hll_config("h")


# ---------------------------------------------------------------------------
# positional operation results (raw + typed accessors)
# ---------------------------------------------------------------------------

class TestPositionalOperationResults:
    """Contract shared by ``operation_result`` and ``typed_operation_result``:
    one slot per op in request order, write-only ops occupy a slot with no
    value, out-of-range indices and error rows yield ``None``."""

    def _rec(self, results):
        def _op_result(i):
            if results is None or not 0 <= i < len(results):
                return None
            return results[i]
        return SimpleNamespace(bins={}, results=results, operation_result=_op_result)

    def _rr(self, results):
        return RecordResult(
            key=_key(), record=self._rec(results), result_code=ResultCode.OK,
        )

    def test_raw_slots_are_op_aligned(self):
        rr = self._rr([1, None, 11])
        assert [rr.operation_result(i) for i in range(3)] == [1, None, 11]

    def test_raw_out_of_range_returns_none(self):
        assert self._rr([1]).operation_result(5) is None

    def test_raw_error_row_returns_none(self):
        rr = RecordResult(
            key=_key(), record=None, result_code=ResultCode.KEY_NOT_FOUND_ERROR,
        )
        assert rr.operation_result(0) is None

    def test_typed_wraps_slot_value(self):
        wrapped = self._rr([1, None, 11]).typed_operation_result(2)
        assert isinstance(wrapped, OperationResult)
        assert wrapped.get_long() == 11

    def test_typed_write_only_slot_wraps_none(self):
        # In-range slot with no value: a wrapper around None, so the typed
        # defaults apply — not a bare None, which is reserved for "no slot".
        wrapped = self._rr([1, None, 11]).typed_operation_result(1)
        assert isinstance(wrapped, OperationResult)
        assert wrapped.value is None
        assert wrapped.get_long() == 0

    def test_typed_out_of_range_returns_none(self):
        assert self._rr([1]).typed_operation_result(5) is None

    def test_typed_error_row_returns_none(self):
        rr = RecordResult(
            key=_key(), record=None, result_code=ResultCode.KEY_NOT_FOUND_ERROR,
        )
        assert rr.typed_operation_result(0) is None

    def test_typed_no_positional_results_returns_none(self):
        assert self._rr(None).typed_operation_result(0) is None
