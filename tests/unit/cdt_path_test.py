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

from aerospike_native import ExpOperation

from aerospike_sdk import Behavior, CTX, Exp, ExpType, LoopVarPart, MapOrder, SelectFlags
from aerospike_sdk.aio.operations import cdt_read
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
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit", behavior=Behavior.DEFAULT)
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

    def test_bytes_element_is_one_blob_key_among_mixed_keys(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_keys_in([b"\x01\x02", "s", 3])
        assert path._ctx == (CTX.map_keys_in([b"\x01\x02", "s", 3]),)
        assert path._ctx != (CTX.map_keys_in([1, 2, "s", 3]),)

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


_COLLECTION_STEPS = {
    "on_map_keys_in": lambda b, v: b.on_map_keys_in(v),
    "on_map_key_list": lambda b, v: b.on_map_key_list(v),
    "on_map_value_list": lambda b, v: b.on_map_value_list(v),
    "on_list_value_list": lambda b, v: b.on_list_value_list(v),
}

_STEP_OWNERS = {
    "query_bin": lambda: QueryBinBuilder(_OpCollector(), "m"),
    "write_bin": lambda: _write_bin()[0],
    "cdt_read": lambda: QueryBinBuilder(_OpCollector(), "m").on_map_key("x"),
    "cdt_write": lambda: _write_bin()[0].on_map_key("x"),
}


class TestCollectionArguments:
    """Steps taking a collection of keys or values refuse a bare ``str`` or ``bytes``.

    Both are iterable, so they would otherwise select one entry per character
    or one integer key per byte.
    """

    @pytest.mark.parametrize("step", _COLLECTION_STEPS.values(), ids=_COLLECTION_STEPS.keys())
    @pytest.mark.parametrize("owner", _STEP_OWNERS.values(), ids=_STEP_OWNERS.keys())
    def test_every_step_rejects_bare_bytes(self, owner, step):
        with pytest.raises(TypeError, match="must be a collection"):
            step(owner(), b"ab")

    def test_path_step_rejects_bare_bytes(self):
        with pytest.raises(TypeError, match="must be a collection"):
            QueryBinBuilder(_OpCollector(), "m").on_each_child().on_map_keys_in(b"ab")

    @pytest.mark.parametrize("value", ["ab", b"ab", bytearray(b"ab")], ids=["str", "bytes", "bytearray"])
    def test_message_names_the_type_and_the_fix(self, value):
        with pytest.raises(TypeError, match=rf"not a single {type(value).__name__}; wrap"):
            QueryBinBuilder(_OpCollector(), "m").on_map_key_list(value)

    def test_list_step_accepts_a_generator(self):
        parent = _OpCollector()
        QueryBinBuilder(parent, "m").on_map_key_list(k for k in ("a", "b")).get_values()
        assert len(parent.operations) == 1


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

    def test_refines_a_single_element_step(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_each_child().on_map_key("x").and_filter(_over(10))
        assert path._ctx == (CTX.all_children(), CTX.map_key("x"), CTX.and_filter(_over(10)))

    def test_rejected_when_chained(self):
        with pytest.raises(TypeError, match="and_filter"):
            (
                QueryBinBuilder(_OpCollector(), "m")
                .on_map_keys_in(["a"]).and_filter(_over(10)).and_filter(_over(20))
            )

    @pytest.mark.parametrize(("step", "expected"), [
        (lambda b: b.on_map_key("b"), CTX.map_key("b")),
        (lambda b: b.on_map_key("b", create_type=MapOrder.KEY_ORDERED),
         CTX.map_key_create("b", MapOrder.KEY_ORDERED)),
        (lambda b: b.on_map_value(15), CTX.map_value(15)),
        (lambda b: b.on_list_index(1), CTX.list_index(1)),
        (lambda b: b.on_list_rank(1), CTX.list_rank(1)),
        (lambda b: b.on_list_value(15), CTX.list_value(15)),
    ], ids=["map_key", "map_key_create", "map_value", "list_index", "list_rank", "list_value"])
    def test_refines_a_first_element_step(self, step, expected):
        path = step(QueryBinBuilder(_OpCollector(), "m")).and_filter(_over(10))
        assert isinstance(path, CdtPathBuilder)
        assert path._ctx == (expected, CTX.and_filter(_over(10)))

    def test_refines_chained_element_steps(self):
        path = QueryBinBuilder(_OpCollector(), "m").on_map_key("a").on_list_index(0).and_filter(_over(10))
        assert path._ctx == (CTX.map_key("a"), CTX.list_index(0), CTX.and_filter(_over(10)))

    def test_refines_a_write_bin_element_step(self):
        wbb, segment = _write_bin()
        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        assert wbb.on_map_key("b").and_filter(_over(10)).modify_by(add_1) is segment
        assert len(segment._qb._operations) == 1

    @pytest.mark.parametrize("step", [
        lambda b: b.on_map_index(0),
        lambda b: b.on_map_rank(0),
        lambda b: b.on_map_key("a").on_map_index(0),
        lambda b: b.on_map_key("a").on_map_rank(0),
    ], ids=["map_index", "map_rank", "nested_map_index", "nested_map_rank"])
    def test_rejected_after_a_map_index_or_rank_step(self, step):
        with pytest.raises(TypeError, match="and_filter"):
            step(QueryBinBuilder(_OpCollector(), "m")).and_filter(_over(10))

    def test_rejected_after_a_write_bin_map_index_step(self):
        wbb, _ = _write_bin()
        with pytest.raises(TypeError, match="and_filter"):
            wbb.on_map_index(0).and_filter(_over(10))

    def test_rejected_after_a_range_selection(self):
        with pytest.raises(TypeError, match="and_filter"):
            QueryBinBuilder(_OpCollector(), "m").on_map_key_range("a", "d").and_filter(_over(10))


class _RecordingCdtOperation:
    """Stands in for ``CdtOperation``, whose operations do not compare equal."""

    def __getattr__(self, factory):
        def record(bin_name, *args):
            return (factory, *args[:-1])
        return record


class TestCollectTerminals:
    """Each ``collect_*`` terminal emits its select, adding ``NO_FAIL`` on request."""

    @pytest.mark.parametrize(("terminal", "plain", "flags"), [
        ("collect_values", "select_values", SelectFlags.VALUE),
        ("collect_map_keys", "select_map_keys", SelectFlags.MAP_KEY),
        ("collect_map_entries", "select_map_entries", SelectFlags.MAP_KEY_VALUE),
        ("collect_matching_tree", "select_matching_tree", SelectFlags.MATCHING_TREE),
    ], ids=["values", "map_keys", "map_entries", "matching_tree"])
    @pytest.mark.parametrize("no_fail", [False, True])
    def test_emits_the_select_and_flags(self, monkeypatch, terminal, plain, flags, no_fail):
        monkeypatch.setattr(cdt_read, "CdtOperation", _RecordingCdtOperation())
        parent = _OpCollector()
        getattr(QueryBinBuilder(parent, "m").on_each_child(), terminal)(no_fail=no_fail)
        expected = ("select_by_path", flags | SelectFlags.NO_FAIL) if no_fail else (plain,)
        assert parent.operations == [expected]


class TestExpressionReadTerminal:
    """``collect_values_as_expression_read`` emits one expression read op."""

    @pytest.mark.parametrize("bin_type", [ExpType.MAP, ExpType.LIST])
    def test_emits_one_expression_read(self, bin_type):
        parent = _OpCollector()
        result = QueryBinBuilder(parent, "m").on_each_child().collect_values_as_expression_read(
            bin_type, no_fail=True, ignore_eval_failure=True,
        )
        assert result is parent
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ExpOperation)

    @pytest.mark.parametrize("bin_type", [ExpType.INT, ExpType.STRING])
    def test_rejects_a_non_collection_bin_type(self, bin_type):
        parent = _OpCollector()
        with pytest.raises(ValueError, match="ExpType.MAP or ExpType.LIST"):
            QueryBinBuilder(parent, "m").on_each_child().collect_values_as_expression_read(bin_type)
        assert parent.operations == []
