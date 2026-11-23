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

"""Tests for PreparedAel: typed binding of named parameters into AEL text (no server needed)."""

from collections import OrderedDict
from types import MappingProxyType
from unittest.mock import MagicMock

import pytest

from aerospike_sdk import Behavior, PreparedAel, SortedMap
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.server_filter import bind_ael_params, bind_prepared_ael_params


def _bind(value):
    return PreparedAel("?v").bind(v=value)


def _query_builder():
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test", behavior=Behavior.DEFAULT)


class TestPlaceholders:
    def test_binds_by_name(self):
        template = PreparedAel("$.name == ?name and $.age > ?min_age")
        assert template.bind(name="Tim", min_age=30) == "$.name == 'Tim' and $.age > 30"

    def test_value_order_does_not_matter(self):
        template = PreparedAel("$.name == ?name and $.age > ?min_age")
        assert template.bind(min_age=30, name="Tim") == "$.name == 'Tim' and $.age > 30"

    def test_placeholder_may_repeat(self):
        template = PreparedAel("$.low < ?n and $.high > ?n")
        assert template.bind(n=5) == "$.low < 5 and $.high > 5"

    @pytest.mark.parametrize("name", ["_", "_private", "a", "a1", "min_age_2", "Z9_"])
    def test_name_grammar(self, name):
        assert PreparedAel(f"$.a == ?{name}").bind(**{name: 1}) == "$.a == 1"

    def test_names_are_case_sensitive(self):
        template = PreparedAel("$.a == ?age and $.b == ?Age")
        assert template.bind(age=1, Age=2) == "$.a == 1 and $.b == 2"
        with pytest.raises(ValueError, match=r"no value for \?Age"):
            PreparedAel("$.a == ?Age").bind(age=1)

    def test_name_ends_at_the_first_other_character(self):
        assert PreparedAel("?a+?b,?c)?d").bind(a=1, b=2, c=3, d=4) == "1+2,3)4"

    @pytest.mark.parametrize(
        ("template", "name"),
        [("$.a == ?año", "año"), ("$.a == ?nameé and $.b == 1", "nameé"), ("?n٣", "n٣"), ("?a_ñ_b", "a_ñ_b")],
    )
    def test_name_running_into_a_non_ascii_character_raises(self, template, name):
        with pytest.raises(ValueError, match=rf"placeholder names are ASCII .* got \?{name} in"):
            PreparedAel(template)

    def test_non_ascii_text_beside_a_name_is_allowed(self):
        assert PreparedAel("?name → 'ñ?año'").bind(name=1) == "1 → 'ñ?año'"

    def test_no_placeholders_binds_to_itself(self):
        assert PreparedAel("$.age > 30").bind() == "$.age > 30"

    def test_percent_in_the_template_is_text(self):
        template = PreparedAel("$.n % 2 == ?rem and $.s == '100%' and $.t == '%(rem)s %s %%'")
        assert template.bind(rem=1) == "$.n % 2 == 1 and $.s == '100%' and $.t == '%(rem)s %s %%'"
        assert PreparedAel("$.n % 2 == 1").bind() == "$.n % 2 == 1"

    @pytest.mark.parametrize("template", ["$.l.[?(@ > 3)]", "$.a ? $.b", "$.a == ?", "?é", "? name"])
    def test_question_mark_without_a_name_is_text(self, template):
        assert PreparedAel(template).bind() == template

    def test_filter_syntax_beside_a_placeholder(self):
        assert PreparedAel("$.l.[?(@ > ?floor)]").bind(floor=3) == "$.l.[?(@ > 3)]"

    @pytest.mark.parametrize("template", ["$.a == ?0", "$.a == ?1 and $.b == ?name", "?12", "?0abc"])
    def test_numbered_placeholder_raises_at_construction(self, template):
        with pytest.raises(ValueError, match=r"placeholders are named, like \?min_age; got \?\d+"):
            PreparedAel(template)

    def test_missing_names_raise(self):
        with pytest.raises(ValueError, match=r"no value for \?b, \?c$"):
            PreparedAel("$.a == ?a and $.b == ?b and $.c == ?c").bind(a=1)

    def test_extra_names_raise(self):
        with pytest.raises(ValueError, match=r"no placeholder outside quotes and comments named \?b, \?c$"):
            PreparedAel("$.a == ?a").bind(a=1, c=3, b=2)

    def test_misspelled_name_reports_both_sides(self):
        with pytest.raises(ValueError, match=r"no value for \?min_age and no placeholder .* named \?minage$"):
            PreparedAel("$.age > ?min_age").bind(minage=21)

    def test_placeholder_named_self(self):
        assert PreparedAel("$.owner == ?self").bind(self="tim") == "$.owner == 'tim'"

    def test_positional_value_is_refused(self):
        with pytest.raises(TypeError):
            PreparedAel("$.a == ?a").bind(1)

    def test_non_string_template_raises(self):
        with pytest.raises(TypeError, match="must be a str"):
            PreparedAel(42)


class TestQuotedAndCommentedText:
    """A value substituted inside a literal or comment would land outside its own quotes."""

    @pytest.mark.parametrize(
        "template",
        [
            "$.name == '?name'",
            '$.name == "?name"',
            "$.name == 'prefix-?name'",
            "$.name == 'it\\'s ?name'",
            '$.name == "say \\"?name\\""',
            "$.name == 'x' /* was ?name */",
            "$.name == 'unterminated ?name",
            "$.name == 'x' /* unterminated ?name",
            "$.name == '?0'",
            '$.name == "?0"',
            "$.name == 'x' /* was ?0 */",
        ],
    )
    def test_placeholder_inside_is_plain_text(self, template):
        assert PreparedAel(template).bind() == template

    def test_quoted_placeholder_with_a_value_raises(self):
        with pytest.raises(ValueError, match=r"no placeholder outside quotes and comments named \?name"):
            PreparedAel("$.name == '?name'").bind(name="Tim")

    def test_placeholder_after_a_literal_still_binds(self):
        template = PreparedAel("$.a == 'it\\'s ?n' and $.b == \"?n\" and $.c == ?n")
        assert template.bind(n=7) == "$.a == 'it\\'s ?n' and $.b == \"?n\" and $.c == 7"

    def test_placeholder_after_a_comment_still_binds(self):
        assert PreparedAel("/* ?other */ $.a == ?n").bind(n=7) == "/* ?other */ $.a == 7"

    def test_question_mark_digits_in_a_literal_do_not_count(self):
        assert PreparedAel("$.q == 'is it 2?3' and $.a == ?n").bind(n=1) == (
            "$.q == 'is it 2?3' and $.a == 1"
        )


class TestStringLiterals:
    def test_single_quoted(self):
        assert _bind("Tim") == "'Tim'"

    def test_empty(self):
        assert _bind("") == "''"

    def test_single_quote_is_escaped(self):
        assert _bind("o'brien") == "'o\\'brien'"

    def test_double_quote_needs_no_escape(self):
        assert _bind('say "hi"') == "'say \"hi\"'"

    def test_both_quote_characters(self):
        assert _bind("it's \"x\"") == "'it\\'s \"x\"'"

    def test_backslash_is_escaped(self):
        assert _bind("a\\") == "'a\\\\'"

    def test_backslash_before_quote(self):
        assert _bind("\\'") == "'\\\\\\''"

    @pytest.mark.parametrize(
        ("char", "escaped"),
        [("\n", "\\n"), ("\r", "\\r"), ("\t", "\\t"), ("\0", "\\0"), ("\x01", "\\x01"), ("\x7f", "\\x7f")],
    )
    def test_control_characters_are_escaped(self, char, escaped):
        assert _bind(f"a{char}b") == f"'a{escaped}b'"

    def test_non_ascii_passes_through(self):
        assert _bind("héllo ☃ 😀") == "'héllo ☃ 😀'"

    def test_placeholder_and_percent_text_in_a_value_stay_text(self):
        template = PreparedAel("$.a == ?a and $.b == ?b")
        assert template.bind(a="?b", b="%s") == "$.a == '?b' and $.b == '%s'"

    def test_unpaired_surrogate_raises(self):
        with pytest.raises(ValueError, match=r"\?v.*unpaired surrogate"):
            _bind("\ud800")

    def test_str_subclass(self):
        class Name(str):
            pass

        assert _bind(Name("Tim")) == "'Tim'"


class TestNumberLiterals:
    def test_booleans_are_lowercase(self):
        assert _bind(True) == "true"
        assert _bind(False) == "false"

    def test_integers(self):
        assert _bind(42) == "42"
        assert _bind(-7) == "-7"
        assert _bind(0) == "0"

    def test_integer_one_is_not_a_boolean(self):
        assert _bind(1) == "1"

    def test_signed_64_bit_limits(self):
        assert _bind(2**63 - 1) == "9223372036854775807"
        assert _bind(-(2**63)) == "-9223372036854775808"

    @pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
    def test_integer_out_of_range_raises(self, value):
        with pytest.raises(ValueError, match="signed 64-bit"):
            _bind(value)

    @pytest.mark.parametrize(
        ("value", "text"),
        [
            (1.5, "1.5"),
            (1.0, "1.0"),
            (-0.5, "-0.5"),
            (0.0, "0.0"),
            (1e-7, "0.0000001"),
            (1e20, "100000000000000000000.0"),
            (0.1 + 0.2, "0.30000000000000004"),
            (123456789.12345679, "123456789.12345679"),
        ],
    )
    def test_floats_are_plain_decimal_with_a_point(self, value, text):
        assert _bind(value) == text

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_float_raises(self, value):
        with pytest.raises(ValueError, match="no AEL literal"):
            _bind(value)

    def test_longest_accepted_float(self):
        assert len(_bind(1e60)) == 63

    @pytest.mark.parametrize("value", [1e61, 1e-62, -1e60, 5e-324])
    def test_float_past_the_server_limit_raises(self, value):
        with pytest.raises(ValueError, match="at most 63"):
            _bind(value)


class TestBlobLiterals:
    def test_bytes(self):
        assert _bind(b"\xff\x00") == "X'ff00'"

    def test_empty_bytes(self):
        assert _bind(b"") == "X''"

    def test_bytearray_and_memoryview(self):
        assert _bind(bytearray(b"\x01\x02")) == "X'0102'"
        assert _bind(memoryview(b"\x01\x02")) == "X'0102'"

    def test_blob_inside_an_expression(self):
        assert PreparedAel("$.digest == ?digest").bind(digest=b"\xab") == "$.digest == X'ab'"


class TestCollectionLiterals:
    def test_list(self):
        assert _bind([1, "a", 1.5, True, b"\x01"]) == "[1, 'a', 1.5, true, X'01']"

    def test_tuple_binds_as_a_list(self):
        assert _bind((1, 2)) == "[1, 2]"

    def test_empty_list_and_map(self):
        assert _bind([]) == "[]"
        assert _bind({}) == "{}"

    def test_nested(self):
        assert _bind([1, [2, {"k": "v'"}]]) == "[1, [2, {'k': 'v\\''}]]"

    def test_map_keeps_insertion_order(self):
        assert _bind({"b": 2, "a": 1}) == "{'b': 2, 'a': 1}"

    def test_map_with_integer_and_bytes_keys(self):
        assert _bind({2: "x", b"\x01": 1}) == "{2: 'x', X'01': 1}"

    def test_sorted_map_binds_as_a_map(self):
        assert _bind(SortedMap({"a": 1})) == "{'a': 1}"

    @pytest.mark.parametrize("key", [1.5, True, (1, 2), None])
    def test_unsupported_map_key_raises(self, key):
        with pytest.raises(TypeError, match="keys must be str, int or bytes"):
            _bind({key: 1})

    def test_bad_element_names_the_placeholder(self):
        with pytest.raises(TypeError, match=r"cannot bind \?items: .*NoneType"):
            PreparedAel("$.a == ?a and $.l == ?items").bind(a=1, items=[1, None])


class TestUnsupportedValues:
    def test_none_raises(self):
        with pytest.raises(TypeError, match=r"cannot bind \?v: .*NoneType"):
            _bind(None)

    @pytest.mark.parametrize("value", [object(), {1, 2}, 1 + 2j])
    def test_other_types_raise(self, value):
        with pytest.raises(TypeError, match="has no AEL literal"):
            _bind(value)


class TestObject:
    def test_template_property_and_str(self):
        template = PreparedAel("$.a == ?a")
        assert template.template == "$.a == ?a"
        assert str(template) == "$.a == ?a"
        assert repr(template) == "PreparedAel('$.a == ?a')"

    def test_equal_by_template_and_hashable(self):
        assert PreparedAel("$.a == ?a") == PreparedAel("$.a == ?a")
        assert PreparedAel("$.a == ?a") != PreparedAel("$.b == ?a")
        assert PreparedAel("$.a == ?a") != "$.a == ?a"
        assert len({PreparedAel("$.a == ?a"), PreparedAel("$.a == ?a")}) == 1

    def test_immutable(self):
        with pytest.raises(AttributeError):
            PreparedAel("$.a == ?a").template = "$.b == ?a"

    def test_reusable(self):
        template = PreparedAel("$.name == ?name")
        assert template.bind(name="Tim") == "$.name == 'Tim'"
        assert template.bind(name="Jane") == "$.name == 'Jane'"


class TestBinderEntryPoints:
    def test_prepared_template_binds_typed(self):
        bound = bind_ael_params(PreparedAel("$.name == ?name"), ({"name": "o'brien"},))
        assert bound == "$.name == 'o\\'brien'"

    def test_prepared_template_without_params(self):
        assert bind_ael_params(PreparedAel("$.age > 30"), ()) == "$.age > 30"

    def test_prepared_template_with_an_empty_mapping(self):
        assert bind_ael_params(PreparedAel("$.age > 30"), ({},)) == "$.age > 30"

    def test_prepared_template_missing_params_raises(self):
        with pytest.raises(ValueError, match=r"no value for \?name"):
            bind_ael_params(PreparedAel("$.name == ?name"), ())

    def test_mapping_for_a_template_with_no_placeholders_raises(self):
        with pytest.raises(ValueError, match=r"no placeholder .* named \?min_age"):
            bind_ael_params(PreparedAel("$.age > 30"), ({"min_age": 21},))

    @pytest.mark.parametrize(
        "mapping",
        [OrderedDict(name="Tim"), MappingProxyType({"name": "Tim"})],
    )
    def test_any_mapping_type_is_accepted(self, mapping):
        assert bind_ael_params(PreparedAel("$.name == ?name"), (mapping,)) == "$.name == 'Tim'"

    def test_python_keyword_as_a_placeholder_name(self):
        bound = bind_ael_params(PreparedAel("$.class == ?class and $.n > ?from"), ({"class": "a", "from": 1},))
        assert bound == "$.class == 'a' and $.n > 1"

    def test_dict_as_a_placeholder_value_binds_as_a_map(self):
        assert bind_ael_params(PreparedAel("$.m == ?m"), ({"m": {"a": 1}},)) == "$.m == {'a': 1}"

    @pytest.mark.parametrize(
        "params",
        [(21,), ("Tim",), ([("min_age", 21)],), ({"min_age": 21}, {"name": "Tim"}), ({"min_age": 21}, 30)],
    )
    def test_values_that_are_not_one_mapping_raise(self, params):
        with pytest.raises(TypeError, match=r"as one mapping of placeholder name to value"):
            bind_ael_params(PreparedAel("$.age > ?min_age"), params)

    def test_non_string_placeholder_name_raises(self):
        with pytest.raises(TypeError, match=r"placeholder names for AEL template '\$\.a == \?a' must be str"):
            bind_ael_params(PreparedAel("$.a == ?a"), ({1: 2},))

    def test_value_error_keeps_its_type_through_the_entry_point(self):
        with pytest.raises(TypeError, match=r"cannot bind \?a: .*NoneType"):
            bind_ael_params(PreparedAel("$.a == ?a"), ({"a": None},))

    def test_string_with_params_is_still_printf(self):
        bound = bind_ael_params("$.name == '%s' and $.q == '?name'", ("Tim",))
        assert bound == "$.name == 'Tim' and $.q == '?name'"

    def test_typed_only_entry_binds_a_template(self):
        assert bind_prepared_ael_params(PreparedAel("$.a + ?step"), ({"step": 1.5},)) == "$.a + 1.5"

    def test_typed_only_entry_refuses_a_positional_value(self):
        with pytest.raises(TypeError, match="as one mapping"):
            bind_prepared_ael_params(PreparedAel("$.a + ?step"), (1.5,))

    def test_typed_only_entry_passes_other_expressions_through(self):
        expression = MagicMock()
        assert bind_prepared_ael_params(expression, ()) is expression
        assert bind_prepared_ael_params("$.a + 1", ()) == "$.a + 1"

    def test_typed_only_entry_rejects_params_on_a_string(self):
        with pytest.raises(TypeError, match="require a PreparedAel"):
            bind_prepared_ael_params("$.a + %d", (1,))


class TestBuilderWiring:
    def test_where_binds_a_template(self):
        template = PreparedAel("$.name == ?name and $.age > ?min_age")
        qb = _query_builder().where(template, {"name": "Tim", "min_age": 30})
        assert qb._where_ael == "$.name == 'Tim' and $.age > 30"

    def test_default_where_binds_a_template(self):
        qb = _query_builder().default_where(PreparedAel("$.status == ?status"), {"status": "active"})
        assert qb._default_where_ael == "$.status == 'active'"

    def test_where_with_a_positional_value_raises(self):
        with pytest.raises(TypeError, match="as one mapping"):
            _query_builder().where(PreparedAel("$.name == ?name"), "Tim")

    def test_where_with_an_unknown_name_raises(self):
        with pytest.raises(ValueError, match=r"named \?age"):
            _query_builder().where(PreparedAel("$.name == ?name"), {"name": "Tim", "age": 30})
