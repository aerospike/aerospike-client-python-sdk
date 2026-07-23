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

"""Unit tests for AEL map and list combined expressions."""

from aerospike_sdk import CTX, Exp, ExpType, ListReturnType, MapReturnType, parse_ael


class TestMapAndListExpressions:
    """Test nested map and list CDT expressions. Order matches MapAndListExpressionsTests."""

    def test_list_inside_a_map(self):
        """Map with list value: $.mapBin1.a.[0] and $.mapBin1.a.cc.[2].get(type: INT)."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a")],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.a.[0] == 100")
        assert result == expected

        expected_gt = Exp.gt(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(2),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("cc")],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.a.cc.[2].get(type: INT) > 100")
        assert result == expected_gt

    def test_map_list_list(self):
        """Map then list then list: $.mapBin1.a.[0].[0]."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.list_index(0)],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.a.[0].[0] == 100")
        assert result == expected

    def test_map_inside_a_list(self):
        """List with map value: $.listBin1.[2].cc.get(type: INT)."""
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.list_bin("listBin1"),
                [CTX.list_index(2)],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[2].cc.get(type: INT) > 100")
        assert result == expected

    def test_list_map_map(self):
        """List then map then map: $.listBin1.[2].aa.cc (with optional get(type: INT))."""
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.list_bin("listBin1"),
                [CTX.list_index(2), CTX.map_key("aa")],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[2].aa.cc > 100")
        assert result == expected
        result = parse_ael("$.listBin1.[2].aa.cc.get(type: INT) > 100")
        assert result == expected

    def test_list_map_list(self):
        """List then map then list: $.listBin1.[1].a.[0]."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [CTX.list_index(1), CTX.map_key("a")],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[1].a.[0] == 100")
        assert result == expected

    def test_list_map_list_size(self):
        """Size of list at path: $.listBin1.[1].a.[0].count() and [].count() variant."""
        expected = Exp.eq(
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.LIST,
                    Exp.int_val(0),
                    Exp.list_bin("listBin1"),
                    [CTX.list_index(1), CTX.map_key("a")],
                ),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[1].a.[0].count() == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[1].a.[0].[].count() == 100")
        assert result == expected

    def test_map_list_map(self):
        """Map then list then map: $.mapBin1.a.[0].cc (with optional get(type: INT))."""
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("cc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.list_index(0)],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.a.[0].cc > 100")
        assert result == expected
        result = parse_ael("$.mapBin1.a.[0].cc.get(type: INT) > 100")
        assert result == expected

    def test_nested_list_access(self):
        """List then list: $.listBin.[0].[0] (list-list only)."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin"),
                [CTX.list_index(0)],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin.[0].[0] == 100")
        assert result == expected

    def test_nested_map_access(self):
        """Map then map then map: $.mapBin.a.bb.ccc (map-map-map only)."""
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("ccc"),
                Exp.map_bin("mapBin"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin.a.bb.ccc == 200")
        assert result == expected
