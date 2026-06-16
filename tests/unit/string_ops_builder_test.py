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

"""Unit tests for string-operation fluent builders (no cluster)."""

from aerospike_async import (
    FilterExpression,
    StringOperation,
    StringRegexFlags,
    StringWriteFlags,
)

from aerospike_sdk import Exp
from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)


def _make_wbb(bin_name: str = "s") -> tuple[WriteBinBuilder, WriteSegmentBuilder]:
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
    segment = WriteSegmentBuilder(qb)
    return WriteBinBuilder(segment, bin_name), segment


def _make_qbb(bin_name: str = "s") -> tuple[QueryBinBuilder, QueryBuilder]:
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
    return QueryBinBuilder(qb, bin_name), qb


# ---------------------------------------------------------------------------
# Op factory shape — each str_* registers a StringOperation of the right type
# ---------------------------------------------------------------------------

class TestStringOpFactoryShape:

    def test_str_strlen_registers_strlen_op(self):
        wbb, seg = _make_wbb("msg")
        wbb.str_strlen()
        assert len(seg._qb._operations) == 1
        assert isinstance(seg._qb._operations[0], type(StringOperation.strlen("x")))

    def test_str_substr_two_form(self):
        wbb, seg = _make_wbb()
        wbb.str_substr(1, 4)
        assert len(seg._qb._operations) == 1

    def test_str_substr_suffix_form(self):
        wbb, seg = _make_wbb()
        wbb.str_substr(3)
        assert len(seg._qb._operations) == 1

    def test_str_upper_registers_upper_op(self):
        wbb, seg = _make_wbb()
        wbb.str_upper()
        assert isinstance(seg._qb._operations[0], type(StringOperation.upper("x")))

    def test_str_concat_with_list_value(self):
        wbb, seg = _make_wbb()
        wbb.str_concat(["a", "b", "c"])
        assert len(seg._qb._operations) == 1

    def test_str_regex_compare_registers_regex_op(self):
        wbb, seg = _make_wbb()
        wbb.str_regex_compare(r"\d+")
        assert isinstance(seg._qb._operations[0], type(StringOperation.regex_compare("x", "p")))


# ---------------------------------------------------------------------------
# Polish: typed StringWriteFlags / StringRegexFlags accepted at builder boundary
# (no int() cast required at call site)
# ---------------------------------------------------------------------------

class TestStringFlagAcceptance:

    def test_str_upper_accepts_typed_write_flag(self):
        wbb, seg = _make_wbb()
        wbb.str_upper(flags=StringWriteFlags.NO_FAIL)
        assert len(seg._qb._operations) == 1

    def test_str_upper_accepts_raw_int_flag(self):
        wbb, seg = _make_wbb()
        wbb.str_upper(flags=int(StringWriteFlags.NO_FAIL))
        assert len(seg._qb._operations) == 1

    def test_str_concat_accepts_typed_write_flag(self):
        wbb, seg = _make_wbb()
        wbb.str_concat("x", flags=StringWriteFlags.DEFAULT)
        assert len(seg._qb._operations) == 1

    def test_str_regex_replace_accepts_typed_regex_flag(self):
        wbb, seg = _make_wbb()
        wbb.str_regex_replace(r"\d+", "N", flags=StringRegexFlags.DEFAULT)
        assert len(seg._qb._operations) == 1


# ---------------------------------------------------------------------------
# Polish: Exp.val matches the Exp.{type}_val namespace (was top-level-only before)
# ---------------------------------------------------------------------------

class TestExpVal:

    def test_exp_val_dispatches_string(self):
        e = Exp.val("hello")
        assert isinstance(e, FilterExpression)

    def test_exp_val_dispatches_int(self):
        e = Exp.val(42)
        assert isinstance(e, FilterExpression)

    def test_exp_val_dispatches_none(self):
        e = Exp.val(None)
        assert isinstance(e, FilterExpression)

    def test_exp_string_strlen_compiles(self):
        e = Exp.string_strlen(Exp.string_bin("s"))
        assert isinstance(e, FilterExpression)

    def test_exp_string_find_with_exp_val(self):
        e = Exp.string_find(Exp.val("needle"), Exp.string_bin("s"))
        assert isinstance(e, FilterExpression)


# ---------------------------------------------------------------------------
# Polish: add_operation returns self so calls chain
# ---------------------------------------------------------------------------

class TestChainableAddOperation:

    def test_query_builder_add_operation_chains(self):
        qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
        result = qb.add_operation(StringOperation.strlen("s"))
        assert result is qb
        assert len(qb._operations) == 1

    def test_query_builder_add_operation_multi_chain(self):
        qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
        result = (qb
                  .add_operation(StringOperation.strlen("s"))
                  .add_operation(StringOperation.upper("s"))
                  .add_operation(StringOperation.lower("s")))
        assert result is qb
        assert len(qb._operations) == 3

    def test_write_segment_add_operation_chains(self):
        qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
        seg = WriteSegmentBuilder(qb)
        result = seg.add_operation(StringOperation.upper("s"))
        assert result is seg
        assert len(qb._operations) == 1


# ---------------------------------------------------------------------------
# Surface check: PSDK exposes all 35 str_* methods on WriteBinBuilder and
# all 18 read-only str_* methods on QueryBinBuilder
# ---------------------------------------------------------------------------

class TestStringMethodSurface:

    def test_write_bin_builder_str_method_count(self):
        methods = [m for m in dir(WriteBinBuilder) if m.startswith("str_")]
        assert len(methods) == 35, f"expected 35 str_* on WriteBinBuilder, got {len(methods)}"

    def test_query_bin_builder_str_method_count(self):
        methods = [m for m in dir(QueryBinBuilder) if m.startswith("str_")]
        assert len(methods) == 18, f"expected 18 str_* on QueryBinBuilder, got {len(methods)}"

    def test_query_bin_builder_has_no_modify_methods(self):
        """Modifies (e.g. str_upper, str_concat) live ONLY on WriteBinBuilder."""
        modify_methods = ("str_upper", "str_lower", "str_concat", "str_insert", "str_snip")
        for m in modify_methods:
            assert not hasattr(QueryBinBuilder, m), \
                f"QueryBinBuilder should not expose modify method {m}"
