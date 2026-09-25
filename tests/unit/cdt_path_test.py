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

"""Unit tests for the fluent CDT path builder: the context each step appends.

The path steps only accumulate ``CTX`` entries; the server does the work. These
tests pin the entries each step emits and the client-side rules for
``and_filter``, which the server would otherwise reject with a parameter error.
"""

import pytest

from aerospike_sdk import CTX, Exp, LoopVarPart
from aerospike_sdk.aio.operations.cdt_read import CdtPathBuilder
from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)


class _OpCollector:
    """Minimal parent that satisfies the add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


def _over(threshold: int):
    return Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(threshold))


def _write_bin(bin_name: str = "m") -> tuple[WriteBinBuilder, WriteSegmentBuilder]:
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
    segment = WriteSegmentBuilder(qb)
    return WriteBinBuilder(segment, bin_name), segment


class TestMapKeysInStep:
    """``on_map_keys_in`` is a first hop on both bin builders and a step deeper in."""

    def test_query_bin_first_hop(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_keys_in(["a", "c"])
        assert isinstance(path, CdtPathBuilder)
        assert path._ctx == (CTX.map_keys_in(["a", "c"]),)

    def test_write_bin_first_hop(self):
        wbb, _ = _write_bin()
        path = wbb.on_map_keys_in(["a", "c"])
        assert isinstance(path, CdtPathBuilder)
        assert path._ctx == (CTX.map_keys_in(["a", "c"]),)

    def test_after_element_navigation(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_key("prices").on_map_keys_in([1, 3])
        assert path._ctx == (CTX.map_key("prices"), CTX.map_keys_in([1, 3]))

    def test_after_a_path_step(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_each_child().on_map_keys_in(["a"])
        assert path._ctx == (CTX.all_children(), CTX.map_keys_in(["a"]))

    def test_any_iterable_of_keys(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_keys_in(("a", 1))
        assert path._ctx == (CTX.map_keys_in(["a", 1]),)

    def test_terminal_emits_one_operation(self):
        parent = _OpCollector()
        result = QueryBinBuilder(parent, "m").on_map_keys_in(["a"]).collect_values()
        assert result is parent
        assert len(parent.operations) == 1

    def test_write_terminal_emits_one_operation(self):
        wbb, segment = _write_bin()
        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        assert wbb.on_map_keys_in(["a"]).modify_by(add_1) is segment
        assert len(segment._qb._operations) == 1


class TestAndFilter:
    """``and_filter`` narrows a key selection; anywhere else it cannot succeed."""

    def test_narrows_a_keys_in_selection(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_keys_in(["a", "b"]).and_filter(_over(10))
        assert path._ctx == (CTX.map_keys_in(["a", "b"]), CTX.and_filter(_over(10)))

    def test_path_continues_after_the_filter(self):
        path = (
            QueryBinBuilder(_OpCollector(), "m")
            .on_map_keys_in(["a", "b"]).and_filter(_over(10)).on_each_child()
        )
        assert path._ctx == (
            CTX.map_keys_in(["a", "b"]), CTX.and_filter(_over(10)), CTX.all_children(),
        )

    def test_rejected_as_the_first_step(self):
        with pytest.raises(TypeError, match="and_filter"):
            CdtPathBuilder(_OpCollector(), "m", []).and_filter(_over(10))

    def test_rejected_after_on_each_child(self):
        with pytest.raises(TypeError, match="and_filter"):
            QueryBinBuilder(_OpCollector(), "m").on_each_child().and_filter(_over(10))

    def test_rejected_after_on_each_child_where(self):
        with pytest.raises(TypeError, match="and_filter"):
            QueryBinBuilder(_OpCollector(), "m").on_each_child_where(_over(0)).and_filter(_over(10))

    def test_rejected_after_element_navigation(self):
        with pytest.raises(TypeError, match="and_filter"):
            QueryBinBuilder(_OpCollector(), "m").on_each_child().on_map_key("x").and_filter(_over(10))

    def test_rejected_when_chained(self):
        with pytest.raises(TypeError, match="and_filter"):
            (
                QueryBinBuilder(_OpCollector(), "m")
                .on_map_keys_in(["a"]).and_filter(_over(10)).and_filter(_over(20))
            )
