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

"""Unit tests for the 7 read-side HLL AEL path functions.

Covers parse-equivalence between the AEL string form (e.g.
``$.bin.hllCount()``) and the programmatic ``Exp.*`` builders. Write-side
AEL (``hllInit``, ``hllAdd``) is deferred — those expressions only make
sense as record-write operations, not as filter predicates.
"""

from aerospike_sdk import Exp, parse_ael


HLL_BLOB_A = b"\x00\x04\x0c" + b"\x00" * 64
HLL_BLOB_B = b"\x00\x04\x0c" + b"\x11" * 64


class TestHllCount:

    def test_basic(self):
        result = parse_ael("$.h.hllCount() > 0")
        expected = Exp.gt(Exp.hll_get_count(Exp.hll_bin("h")), Exp.int_val(0))
        assert result == expected

    def test_eq_zero(self):
        result = parse_ael("$.h.hllCount() == 0")
        expected = Exp.eq(Exp.hll_get_count(Exp.hll_bin("h")), Exp.int_val(0))
        assert result == expected

    def test_comparison_between_two_bins(self):
        result = parse_ael("$.a.hllCount() > $.b.hllCount()")
        expected = Exp.gt(
            Exp.hll_get_count(Exp.hll_bin("a")),
            Exp.hll_get_count(Exp.hll_bin("b")),
        )
        assert result == expected


class TestHllDescribe:

    def test_describe_equals_list_literal(self):
        result = parse_ael("$.h.hllDescribe() == [14, -1]")
        expected = Exp.eq(Exp.hll_describe(Exp.hll_bin("h")), Exp.list_val([14, -1]))
        assert result == expected


class TestHllMayContain:

    def test_with_int_list_literal(self):
        result = parse_ael("$.h.hllMayContain([1, 2, 3]) > 0")
        expected = Exp.gt(
            Exp.hll_may_contain(Exp.list_val([1, 2, 3]), Exp.hll_bin("h")),
            Exp.int_val(0),
        )
        assert result == expected

    def test_with_string_list_literal(self):
        result = parse_ael("$.h.hllMayContain(['alice', 'bob']) == 1")
        expected = Exp.eq(
            Exp.hll_may_contain(Exp.list_val(["alice", "bob"]), Exp.hll_bin("h")),
            Exp.int_val(1),
        )
        assert result == expected

    def test_with_placeholder(self):
        result = parse_ael("$.h.hllMayContain(?0) > 0", [b"x", b"y"])
        expected = Exp.gt(
            Exp.hll_may_contain(Exp.list_val([b"x", b"y"]), Exp.hll_bin("h")),
            Exp.int_val(0),
        )
        assert result == expected


class TestHllUnion:

    def test_get_union_count_with_placeholder(self):
        blobs = [HLL_BLOB_A, HLL_BLOB_B]
        result = parse_ael("$.h.hllUnionCount(?0) > 100", blobs)
        expected = Exp.gt(
            Exp.hll_get_union_count(Exp.list_val(blobs), Exp.hll_bin("h")),
            Exp.int_val(100),
        )
        assert result == expected

    def test_get_union_count_with_single_bin_ref(self):
        """``$.h.hllUnionCount($.a)`` — bare HLL bin reference, server treats
        it as an implicit single-element list."""
        result = parse_ael("$.h.hllUnionCount($.a) > 100")
        expected = Exp.gt(
            Exp.hll_get_union_count(Exp.hll_bin("a"), Exp.hll_bin("h")),
            Exp.int_val(100),
        )
        assert result == expected

    def test_get_union_with_placeholder(self):
        blobs = [HLL_BLOB_A]
        result = parse_ael("$.h.hllUnion(?0) == ?1", blobs, HLL_BLOB_B)
        expected = Exp.eq(
            Exp.hll_get_union(Exp.list_val(blobs), Exp.hll_bin("h")),
            Exp.blob_val(HLL_BLOB_B),
        )
        assert result == expected


class TestHllIntersectCount:

    def test_with_placeholder(self):
        blobs = [HLL_BLOB_A, HLL_BLOB_B]
        result = parse_ael("$.h.hllIntersectCount(?0) > 50", blobs)
        expected = Exp.gt(
            Exp.hll_get_intersect_count(Exp.list_val(blobs), Exp.hll_bin("h")),
            Exp.int_val(50),
        )
        assert result == expected

    def test_with_single_bin_ref(self):
        result = parse_ael("$.h.hllIntersectCount($.a) > 50")
        expected = Exp.gt(
            Exp.hll_get_intersect_count(Exp.hll_bin("a"), Exp.hll_bin("h")),
            Exp.int_val(50),
        )
        assert result == expected


class TestHllSimilarity:

    def test_with_placeholder(self):
        blobs = [HLL_BLOB_A]
        result = parse_ael("$.h.hllSimilarity(?0) > 0.5", blobs)
        expected = Exp.gt(
            Exp.hll_get_similarity(Exp.list_val(blobs), Exp.hll_bin("h")),
            Exp.float_val(0.5),
        )
        assert result == expected

    def test_with_single_bin_ref(self):
        result = parse_ael("$.a.hllSimilarity($.b) > 0.5")
        expected = Exp.gt(
            Exp.hll_get_similarity(Exp.hll_bin("b"), Exp.hll_bin("a")),
            Exp.float_val(0.5),
        )
        assert result == expected

    def test_compare_two_similarities(self):
        result = parse_ael(
            "$.a.hllSimilarity(?0) >= $.a.hllSimilarity(?1)",
            [HLL_BLOB_A], [HLL_BLOB_B],
        )
        expected = Exp.ge(
            Exp.hll_get_similarity(Exp.list_val([HLL_BLOB_A]), Exp.hll_bin("a")),
            Exp.hll_get_similarity(Exp.list_val([HLL_BLOB_B]), Exp.hll_bin("a")),
        )
        assert result == expected


class TestHllInsideLogicalClauses:

    def test_and_clause(self):
        result = parse_ael(
            "$.a.hllCount() > 1000 and $.b.hllCount() > 1000",
        )
        expected = Exp.and_([
            Exp.gt(Exp.hll_get_count(Exp.hll_bin("a")), Exp.int_val(1000)),
            Exp.gt(Exp.hll_get_count(Exp.hll_bin("b")), Exp.int_val(1000)),
        ])
        assert result == expected
