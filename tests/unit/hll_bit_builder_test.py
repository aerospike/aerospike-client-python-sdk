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
# distributed under the License is distributed on an "AS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Unit tests for HyperLogLog and bit-operation fluent builders."""

from aerospike_async import (
    BitOperation,
    BitPolicy,
    BitwiseOverflowActions,
    BitwiseResizeFlags,
    BitWriteFlags,
    HllOperation,
)

from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
    _bit_policy_or_default,
    _resize_flags_or_default,
)


def _make_wbb(bin_name: str = "h") -> tuple[WriteBinBuilder, WriteSegmentBuilder]:
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
    segment = WriteSegmentBuilder(qb)
    return WriteBinBuilder(segment, bin_name), segment


class _OpCollector:
    def __init__(self) -> None:
        self.operations: list = []

    def add_operation(self, op: object) -> None:
        self.operations.append(op)


class TestWriteBinHll:
    def test_hll_init_add_get_count_chain(self):
        from aerospike_sdk import HllConfig
        wbb, segment = _make_wbb("sk")
        wbb.hll_init(HllConfig.of(12, 4)).bin("sk").hll_add(["a", "b"]).bin("sk").hll_get_count()
        ops = segment._qb._operations
        assert len(ops) == 3
        assert isinstance(ops[0], type(HllOperation.init("x", 8)))
        assert isinstance(ops[1], type(HllOperation.add("x", [], -1, -1, 0)))
        assert isinstance(ops[2], type(HllOperation.get_count("x")))

    def test_hll_set_union_fold_refresh(self):
        wbb, segment = _make_wbb("u")
        other = [b"\x01\x02"]
        wbb.hll_set_union(other).bin("u").hll_fold(10).bin("u").hll_refresh_count()
        ops = segment._qb._operations
        assert len(ops) == 3

    def test_hll_reads_on_write_builder(self):
        wbb, segment = _make_wbb("r")
        lst = [b"\xff"]
        wbb.hll_describe()
        wbb.hll_get_union(lst)
        wbb.hll_get_union_count(lst)
        wbb.hll_get_intersect_count(lst)
        wbb.hll_get_similarity(lst)
        assert len(segment._qb._operations) == 5


class TestWriteBinBit:
    def test_bit_resize_default_policy_and_flags(self):
        wbb, segment = _make_wbb("blob")
        wbb.bit_resize(8)
        op = segment._qb._operations[-1]
        assert isinstance(op, type(BitOperation.resize("b", 1, BitwiseResizeFlags.DEFAULT, BitPolicy(BitWriteFlags.DEFAULT))))

    def test_bit_set_get_round_trip_ops(self):
        wbb, segment = _make_wbb("blob")
        pol = BitPolicy(BitWriteFlags.DEFAULT)
        wbb.bit_resize(2, BitwiseResizeFlags.GROW_ONLY, pol)
        wbb.bit_set(0, 8, b"\xaa", pol)
        wbb.bit_get(0, 8)
        assert len(segment._qb._operations) == 3

    def test_bit_logical_and_shift(self):
        wbb, segment = _make_wbb("b")
        wbb.bit_or(0, 8, b"\x0f").bin("b").bit_xor(0, 8, b"\xf0").bin("b").bit_and(0, 8, b"\xff")
        wbb.bin("b").bit_not(0, 8)
        wbb.bin("b").bit_lshift(0, 8, 1).bin("b").bit_rshift(0, 8, 1)
        assert len(segment._qb._operations) == 6

    def test_bit_integer_math(self):
        wbb, segment = _make_wbb("b")
        wbb.bit_set_int(0, 16, 7, None)
        wbb.bit_add(0, 16, 1, False, BitwiseOverflowActions.WRAP, None)
        wbb.bit_subtract(0, 16, 1, False, BitwiseOverflowActions.SATURATE, None)
        wbb.bit_get_int(0, 16, False)
        wbb.bit_count(0, 16)
        wbb.bit_lscan(0, 16, True).bin("b").bit_rscan(0, 16, False)
        assert len(segment._qb._operations) == 7

    def test_bit_insert_remove(self):
        wbb, segment = _make_wbb("b")
        wbb.bit_insert(0, b"\x01\x02").bin("b").bit_remove(0, 1)
        assert len(segment._qb._operations) == 2


class TestQueryBinHllBitReads:
    def test_query_bin_hll_and_bit_reads(self):
        parent = _OpCollector()
        qbb: QueryBinBuilder[_OpCollector] = QueryBinBuilder(parent, "qb")
        hll_other = [b"\x01"]
        qbb.hll_get_count()
        qbb.hll_describe()
        qbb.hll_get_union(hll_other)
        qbb.hll_get_union_count(hll_other)
        qbb.hll_get_intersect_count(hll_other)
        qbb.hll_get_similarity(hll_other)
        qbb.bit_get(0, 8)
        qbb.bit_count(0, 8)
        qbb.bit_lscan(0, 8, True)
        qbb.bit_rscan(0, 8, False)
        qbb.bit_get_int(0, 8, True)
        assert len(parent.operations) == 11


class TestBitDefaults:
    def test_bit_policy_or_default(self):
        p = _bit_policy_or_default(None)
        assert isinstance(p, BitPolicy)

    def test_resize_flags_or_default(self):
        assert _resize_flags_or_default(None) == BitwiseResizeFlags.DEFAULT
