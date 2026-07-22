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

"""Unit tests for AEL explicit type expressions."""

import pytest
from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


class TestExplicitTypes:
    """Test explicit type expressions.

    List and map explicit types are tested in their own test classes.
    """

    # Integer comparison tests
    def test_integer_comparison(self):
        """Test $.intBin1.get(type: INT) > 5."""
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(5))
        result = parse_ael("$.intBin1.get(type: INT) > 5")
        assert result == expected

    def test_integer_comparison_reversed(self):
        """Test 5 < $.intBin1.get(type: INT)."""
        expected = Exp.lt(Exp.int_val(5), Exp.int_bin("intBin1"))
        result = parse_ael("5 < $.intBin1.get(type: INT)")
        assert result == expected

    # String comparison tests
    def test_string_comparison_double_quotes(self):
        """Test $.stringBin1.get(type: STRING) == "yes"."""
        expected = Exp.eq(Exp.string_bin("stringBin1"), Exp.string_val("yes"))
        result = parse_ael('$.stringBin1.get(type: STRING) == "yes"')
        assert result == expected

    def test_string_comparison_single_quotes(self):
        """Test $.stringBin1.get(type: STRING) == 'yes'."""
        expected = Exp.eq(Exp.string_bin("stringBin1"), Exp.string_val("yes"))
        result = parse_ael("$.stringBin1.get(type: STRING) == 'yes'")
        assert result == expected

    def test_string_comparison_reversed_double_quotes(self):
        """Test "yes" == $.stringBin1.get(type: STRING)."""
        expected = Exp.eq(Exp.string_val("yes"), Exp.string_bin("stringBin1"))
        result = parse_ael('"yes" == $.stringBin1.get(type: STRING)')
        assert result == expected

    def test_string_comparison_reversed_single_quotes(self):
        """Test 'yes' == $.stringBin1.get(type: STRING)."""
        expected = Exp.eq(Exp.string_val("yes"), Exp.string_bin("stringBin1"))
        result = parse_ael("'yes' == $.stringBin1.get(type: STRING)")
        assert result == expected

    def test_string_comparison_negative_unquoted(self):
        """Test that unquoted string constant raises error."""
        with pytest.raises(AelParseException):
            parse_ael("$.stringBin1.get(type: STRING) == yes")

    # Blob comparison tests
    def test_blob_comparison(self):
        """Test $.blobBin1.get(type: BLOB) == base64_encoded_data."""
        import base64
        data = bytes([1, 2, 3])
        encoded = base64.b64encode(data).decode('ascii')
        expected = Exp.eq(Exp.blob_bin("blobBin1"), Exp.blob_val(data))
        result = parse_ael(f'$.blobBin1.get(type: BLOB) == "{encoded}"')
        assert result == expected

    def test_blob_comparison_reversed(self):
        """Test base64_encoded_data == $.blobBin1.get(type: BLOB)."""
        import base64
        data = bytes([1, 2, 3])
        encoded = base64.b64encode(data).decode('ascii')
        expected = Exp.eq(Exp.blob_val(data), Exp.blob_bin("blobBin1"))
        result = parse_ael(f'"{encoded}" == $.blobBin1.get(type: BLOB)')
        assert result == expected

    # Float comparison tests
    def test_float_comparison(self):
        """Test $.floatBin1.get(type: FLOAT) == 1.5."""
        expected = Exp.eq(Exp.float_bin("floatBin1"), Exp.float_val(1.5))
        result = parse_ael("$.floatBin1.get(type: FLOAT) == 1.5")
        assert result == expected

    def test_float_comparison_reversed(self):
        """Test 1.5 == $.floatBin1.get(type: FLOAT)."""
        expected = Exp.eq(Exp.float_val(1.5), Exp.float_bin("floatBin1"))
        result = parse_ael("1.5 == $.floatBin1.get(type: FLOAT)")
        assert result == expected

    # Boolean comparison tests
    def test_boolean_comparison(self):
        """Test $.boolBin1.get(type: BOOL) == true."""
        expected = Exp.eq(Exp.bool_bin("boolBin1"), Exp.bool_val(True))
        result = parse_ael("$.boolBin1.get(type: BOOL) == true")
        assert result == expected

    def test_boolean_comparison_reversed(self):
        """Test true == $.boolBin1.get(type: BOOL)."""
        expected = Exp.eq(Exp.bool_val(True), Exp.bool_bin("boolBin1"))
        result = parse_ael("true == $.boolBin1.get(type: BOOL)")
        assert result == expected

    def test_boolean_comparison_negative_type_mismatch(self):
        """Test that BOOL compared to INT raises error."""
        with pytest.raises(AelParseException, match="Cannot compare BOOL to INT"):
            parse_ael("$.boolBin1.get(type: BOOL) == 5")

    # List comparison tests - constant on right side
    def test_list_comparison_single_int(self):
        """Test $.listBin1.get(type: LIST) == [100]."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val([100]))
        result = parse_ael("$.listBin1.get(type: LIST) == [100]")
        assert result == expected

    def test_list_comparison_shorthand(self):
        """Test $.listBin1.[] == [100]."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val([100]))
        result = parse_ael("$.listBin1.[] == [100]")
        assert result == expected

    def test_list_comparison_multiple_ints(self):
        """Test $.listBin1.get(type: LIST) == [100, 200, 300, 400]."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val([100, 200, 300, 400]))
        result = parse_ael("$.listBin1.get(type: LIST) == [100, 200, 300, 400]")
        assert result == expected

    def test_list_comparison_single_string(self):
        """Test $.listBin1.get(type: LIST) == ['yes']."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val(["yes"]))
        result = parse_ael("$.listBin1.get(type: LIST) == ['yes']")
        assert result == expected

    def test_list_comparison_multiple_strings(self):
        """Test $.listBin1.get(type: LIST) == ['yes', 'of course']."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val(["yes", "of course"]))
        result = parse_ael("$.listBin1.get(type: LIST) == ['yes', 'of course']")
        assert result == expected

    def test_list_comparison_double_quote_strings(self):
        """Test $.listBin1.get(type: LIST) == ["yes", "of course"]."""
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val(["yes", "of course"]))
        result = parse_ael('$.listBin1.get(type: LIST) == ["yes", "of course"]')
        assert result == expected

    def test_list_comparison_negative_unquoted_strings(self):
        """Test that unquoted strings in list raise error."""
        with pytest.raises(AelParseException):
            parse_ael("$.listBin1.get(type: LIST) == [yes, of course]")

    # List comparison tests - constant on left side
    def test_list_comparison_left_single_int(self):
        """Test [100] == $.listBin1.get(type: LIST)."""
        expected = Exp.eq(Exp.list_val([100]), Exp.list_bin("listBin1"))
        result = parse_ael("[100] == $.listBin1.get(type: LIST)")
        assert result == expected

    def test_list_comparison_left_shorthand(self):
        """Test [100] == $.listBin1.[]."""
        expected = Exp.eq(Exp.list_val([100]), Exp.list_bin("listBin1"))
        result = parse_ael("[100] == $.listBin1.[]")
        assert result == expected

    def test_list_comparison_left_multiple_ints(self):
        """Test [100, 200, 300, 400] == $.listBin1.get(type: LIST)."""
        expected = Exp.eq(Exp.list_val([100, 200, 300, 400]), Exp.list_bin("listBin1"))
        result = parse_ael("[100, 200, 300, 400] == $.listBin1.get(type: LIST)")
        assert result == expected

    def test_list_comparison_left_strings(self):
        """Test ['yes', 'of course'] == $.listBin1.get(type: LIST)."""
        expected = Exp.eq(Exp.list_val(["yes", "of course"]), Exp.list_bin("listBin1"))
        result = parse_ael("['yes', 'of course'] == $.listBin1.get(type: LIST)")
        assert result == expected

    def test_list_comparison_left_negative_unquoted(self):
        """Test that unquoted strings on left raise error."""
        with pytest.raises(AelParseException):
            parse_ael("[yes, of course] == $.listBin1.get(type: LIST)")

    # List comparison tests - negative values in constants
    def test_list_with_negative_ints(self):
        expected = Exp.eq(Exp.list_bin("listBin1"), Exp.list_val([-1, 2]))
        assert parse_ael("$.listBin1.get(type: LIST) == [-1, 2]") == expected

    def test_list_with_negative_ints_left(self):
        expected = Exp.eq(Exp.list_val([-1, 2]), Exp.list_bin("listBin1"))
        assert parse_ael("[-1, 2] == $.listBin1.get(type: LIST)") == expected

    def test_list_with_all_negative_ints(self):
        expected = Exp.eq(Exp.list_bin("bin"), Exp.list_val([-1, -2, -3]))
        assert parse_ael("$.bin.get(type: LIST) == [-1, -2, -3]") == expected

    def test_list_with_negative_floats(self):
        expected = Exp.eq(Exp.list_bin("bin"), Exp.list_val([-1.5, 0.0, 1.5]))
        assert parse_ael("$.bin.get(type: LIST) == [-1.5, 0.0, 1.5]") == expected

    def test_list_with_unary_plus(self):
        expected = Exp.eq(Exp.list_bin("bin"), Exp.list_val([5, -3]))
        assert parse_ael("$.bin.get(type: LIST) == [+5, -3]") == expected

    # Map comparison tests - negative values in constants
    def test_map_with_negative_value(self):
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({"a": -5}))
        assert parse_ael('$.mapBin1.get(type: MAP) == {"a": -5}') == expected

    def test_map_with_negative_float_value(self):
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({"x": -3.14}))
        assert parse_ael('$.mapBin1.get(type: MAP) == {"x": -3.14}') == expected

    def test_map_with_mixed_sign_values(self):
        expected = Exp.eq(Exp.map_bin("bin"), Exp.map_val({"a": -1, "b": 2}))
        assert parse_ael('$.bin.get(type: MAP) == {"a": -1, "b": 2}') == expected

    # Map comparison tests - constant on right side
    def test_map_comparison_single_pair(self):
        """Test $.mapBin1.get(type: MAP) == {100:100}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({100: 100}))
        result = parse_ael("$.mapBin1.get(type: MAP) == {100:100}")
        assert result == expected

    def test_map_comparison_with_spaces(self):
        """Test $.mapBin1.get(type: MAP) == {100 : 100}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({100: 100}))
        result = parse_ael("$.mapBin1.get(type: MAP) == {100 : 100}")
        assert result == expected

    def test_map_comparison_shorthand(self):
        """Test $.mapBin1.{} == {100:100}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({100: 100}))
        result = parse_ael("$.mapBin1.{} == {100:100}")
        assert result == expected

    def test_map_comparison_multiple_pairs(self):
        """Test $.mapBin1.get(type: MAP) == {100:200, 300:400}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({100: 200, 300: 400}))
        result = parse_ael("$.mapBin1.get(type: MAP) == {100:200, 300:400}")
        assert result == expected

    def test_map_comparison_blob_key_right(self):
        """Test $.mapBin1.{} == {base64_encoded_key: 100} (blob as map key)."""
        import base64
        blob_key = bytes([1, 2, 3])
        encoded = base64.b64encode(blob_key).decode("ascii")
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({encoded: 100}))
        result = parse_ael(f'$.mapBin1.{{}} == {{\'{encoded}\':100}}')
        assert result == expected

    def test_map_comparison_string_keys(self):
        """Test $.mapBin1.get(type: MAP) == {'yes?':'yes!'}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({"yes?": "yes!"}))
        result = parse_ael("$.mapBin1.get(type: MAP) == {'yes?':'yes!'}")
        assert result == expected

    def test_map_comparison_double_quote_strings(self):
        """Test $.mapBin1.get(type: MAP) == {"yes" : "yes"}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({"yes": "yes"}))
        result = parse_ael('$.mapBin1.get(type: MAP) == {"yes" : "yes"}')
        assert result == expected

    def test_map_comparison_nested_list_value(self):
        """Test $.mapBin1.get(type: MAP) == {"yes" : ["yes", "of course"]}."""
        expected = Exp.eq(Exp.map_bin("mapBin1"), Exp.map_val({"yes": ["yes", "of course"]}))
        result = parse_ael('$.mapBin1.get(type: MAP) == {"yes" : ["yes", "of course"]}')
        assert result == expected

    def test_map_comparison_negative_unquoted_strings(self):
        """Test that unquoted strings in map raise error."""
        with pytest.raises(AelParseException):
            parse_ael("$.mapBin1.get(type: MAP) == {yes, of course}")

    def test_map_comparison_negative_list_type(self):
        """Test that comparing MAP to LIST raises error."""
        with pytest.raises(AelParseException, match="Cannot compare MAP to LIST"):
            parse_ael("$.mapBin1.get(type: MAP) == ['yes', 'of course']")

    def test_map_comparison_negative_list_key(self):
        """Test that list as map key raises error."""
        with pytest.raises(AelParseException):
            parse_ael("$.mapBin1.get(type: MAP) == {[100]:[100]}")

    # Map comparison tests - constant on left side
    def test_map_comparison_left_blob_key(self):
        """Test {base64_encoded_key: 100} == $.mapBin1.{} (blob as map key)."""
        import base64
        blob_key = bytes([1, 2, 3])
        encoded = base64.b64encode(blob_key).decode("ascii")
        expected = Exp.eq(Exp.map_val({encoded: 100}), Exp.map_bin("mapBin1"))
        result = parse_ael(f"{{'{encoded}':100}} == $.mapBin1.{{}}")
        assert result == expected

    def test_map_comparison_left_single_pair(self):
        """Test {100:100} == $.mapBin1.get(type: MAP)."""
        expected = Exp.eq(Exp.map_val({100: 100}), Exp.map_bin("mapBin1"))
        result = parse_ael("{100:100} == $.mapBin1.get(type: MAP)")
        assert result == expected

    def test_map_comparison_left_shorthand(self):
        """Test {100:100} == $.mapBin1.{}."""
        expected = Exp.eq(Exp.map_val({100: 100}), Exp.map_bin("mapBin1"))
        result = parse_ael("{100:100} == $.mapBin1.{}")
        assert result == expected

    def test_map_comparison_left_multiple_pairs(self):
        """Test {100:200, 300:400} == $.mapBin1.get(type: MAP)."""
        expected = Exp.eq(Exp.map_val({100: 200, 300: 400}), Exp.map_bin("mapBin1"))
        result = parse_ael("{100:200, 300:400} == $.mapBin1.get(type: MAP)")
        assert result == expected

    def test_map_comparison_left_string_keys(self):
        """Test {'yes?':'yes!'} == $.mapBin1.get(type: MAP)."""
        expected = Exp.eq(Exp.map_val({"yes?": "yes!"}), Exp.map_bin("mapBin1"))
        result = parse_ael("{'yes?':'yes!'} == $.mapBin1.get(type: MAP)")
        assert result == expected

    def test_map_comparison_left_nested_list(self):
        """Test {"yes" : ["yes", "of course"]} == $.mapBin1.get(type: MAP)."""
        expected = Exp.eq(Exp.map_val({"yes": ["yes", "of course"]}), Exp.map_bin("mapBin1"))
        result = parse_ael('{"yes" : ["yes", "of course"]} == $.mapBin1.get(type: MAP)')
        assert result == expected

    def test_map_comparison_left_negative_unquoted(self):
        """Test that unquoted strings on left raise error."""
        with pytest.raises(AelParseException):
            parse_ael("{yes, of course} == $.mapBin1.get(type: MAP)")

    def test_map_comparison_left_negative_list_type(self):
        """Test that comparing LIST to MAP raises error."""
        with pytest.raises(AelParseException, match="Cannot compare LIST to MAP"):
            parse_ael("['yes', 'of course'] == $.mapBin1.get(type: MAP)")

    def test_map_comparison_left_negative_list_key(self):
        """Test that list as map key on left raises error."""
        with pytest.raises(AelParseException):
            parse_ael("{[100]:[100]} == $.mapBin1.get(type: MAP)")

    # Two bins comparison tests
    def test_two_string_bins(self):
        """Test $.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)."""
        expected = Exp.eq(Exp.string_bin("stringBin1"), Exp.string_bin("stringBin2"))
        result = parse_ael("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)")
        assert result == expected

    def test_two_int_bins(self):
        """Test $.intBin1.get(type: INT) == $.intBin2.get(type: INT)."""
        expected = Exp.eq(Exp.int_bin("intBin1"), Exp.int_bin("intBin2"))
        result = parse_ael("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)")
        assert result == expected

    def test_two_float_bins(self):
        """Test $.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)."""
        expected = Exp.eq(Exp.float_bin("floatBin1"), Exp.float_bin("floatBin2"))
        result = parse_ael("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)")
        assert result == expected

    def test_two_blob_bins(self):
        """Test $.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)."""
        expected = Exp.eq(Exp.blob_bin("blobBin1"), Exp.blob_bin("blobBin2"))
        result = parse_ael("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)")
        assert result == expected

    def test_two_different_types_negative(self):
        """Test that comparing STRING to FLOAT raises error."""
        with pytest.raises(AelParseException, match="Cannot compare STRING to FLOAT"):
            parse_ael("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)")

    # Arithmetic with explicit types
    def test_second_degree_explicit_float(self):
        """Test ($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) > 10.5."""
        expected = Exp.gt(
            Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]), Exp.float_val(10.5)
        )
        result = parse_ael("($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) > 10.5")
        assert result == expected

    def test_fourth_degree_explicit_float(self):
        """Test (($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT)) + ...) > 10.5."""
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.num_add([Exp.float_bin("oranges"), Exp.float_bin("acai")])
            ]),
            Exp.float_val(10.5)
        )
        result = parse_ael(
            "(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))"
            " + ($.oranges.get(type: FLOAT) + $.acai.get(type: FLOAT))) > 10.5"
        )
        assert result == expected

    # Complicated when expressions with explicit types
    def test_complicated_when_explicit_type_int(self):
        """Test complex when expression with explicit INT types."""
        expected = Exp.eq(
            Exp.int_bin("a"),
            Exp.cond([
                Exp.eq(Exp.int_bin("b"), Exp.int_val(1)), Exp.int_bin("a1"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(2)), Exp.int_bin("a2"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(3)), Exp.int_bin("a3"),
                Exp.num_add([Exp.int_bin("a4"), Exp.int_val(1)])
            ])
        )
        result = parse_ael(
            "$.a.get(type: INT) == "
            "(when($.b.get(type: INT) == 1 => $.a1.get(type: INT),"
            " $.b.get(type: INT) == 2 => $.a2.get(type: INT),"
            " $.b.get(type: INT) == 3 => $.a3.get(type: INT),"
            " default => $.a4.get(type: INT) + 1))"
        )
        assert result == expected

    def test_complicated_when_explicit_type_string(self):
        """Test complex when expression with explicit STRING types."""
        expected = Exp.eq(
            Exp.string_bin("a"),
            Exp.cond([
                Exp.eq(Exp.int_bin("b"), Exp.int_val(1)), Exp.string_bin("a1"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(2)), Exp.string_bin("a2"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(3)), Exp.string_bin("a3"),
                Exp.string_val("hello")
            ])
        )
        result = parse_ael(
            '$.a.get(type: STRING) == '
            '(when($.b == 1 => $.a1.get(type: STRING),'
            ' $.b == 2 => $.a2.get(type: STRING),'
            ' $.b == 3 => $.a3.get(type: STRING),'
            ' default => "hello"))'
        )
        assert result == expected


class TestEmptyCollectionLiterals:
    """Test [] and {} as empty list/map literals vs CDT path designators."""

    def test_empty_list_literal(self):
        """[] parses as an empty list value."""
        expected = Exp.list_val([])
        result = parse_ael("[]")
        assert result == expected

    def test_empty_list_literal_in_comparison(self):
        """$.listBin.get(type: LIST) == []"""
        expected = Exp.eq(Exp.list_bin("listBin"), Exp.list_val([]))
        result = parse_ael("$.listBin.get(type: LIST) == []")
        assert result == expected

    def test_empty_map_literal(self):
        """{} parses as an empty map value."""
        expected = Exp.map_val({})
        result = parse_ael("{}")
        assert result == expected

    def test_empty_map_literal_in_comparison(self):
        """$.mapBin.get(type: MAP) == {}"""
        expected = Exp.eq(Exp.map_bin("mapBin"), Exp.map_val({}))
        result = parse_ael("$.mapBin.get(type: MAP) == {}")
        assert result == expected

    def test_nonempty_list_still_works(self):
        """[1, 2, 3] still parses correctly."""
        expected = Exp.eq(Exp.list_bin("b"), Exp.list_val([1, 2, 3]))
        result = parse_ael("$.b.get(type: LIST) == [1, 2, 3]")
        assert result == expected

    def test_nonempty_map_still_works(self):
        """{'a': 1} still parses correctly."""
        expected = Exp.eq(Exp.map_bin("b"), Exp.map_val({"a": 1}))
        result = parse_ael("$.b.get(type: MAP) == {'a': 1}")
        assert result == expected

    def test_list_type_designator_in_path(self):
        """$.listBin.[] in a path context still works as CDT designator."""
        expected = Exp.eq(Exp.list_bin("listBin"), Exp.list_val([100]))
        result = parse_ael("$.listBin.[] == [100]")
        assert result == expected

    def test_map_type_designator_in_path(self):
        """$.mapBin.{} in a path context still works as CDT designator."""
        expected = Exp.eq(Exp.map_bin("mapBin"), Exp.map_val({100: 100}))
        result = parse_ael("$.mapBin.{} == {100:100}")
        assert result == expected
