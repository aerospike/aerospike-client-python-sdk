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

"""Unit tests for AEL CTX parsing. Order matches CtxTests."""

import io
import contextlib
import pytest

from aerospike_sdk import AelParseException, CTX, parse_ctx


def _parse_ctx_quiet(path: str):
    """Parse path and suppress parser/lexer stderr."""
    with contextlib.redirect_stderr(io.StringIO()):
        return parse_ctx(path)


def _assert_ctx_eq(path: str, expected: list):
    """Parse path and assert result equals expected CTX list."""
    result = parse_ctx(path)
    assert result == expected


class TestCtx:
    """Test CTX parsing from AEL paths."""

    def test_list_expression_only_bin_no_ctx(self):
        with pytest.raises(AelParseException, match="CDT context is not provided"):
            parse_ctx("$.listBin1")

    def test_list_expression_empty_or_malformed_input(self):
        with pytest.raises(AelParseException, match="Path must not be null or empty"):
            parse_ctx(None)

        with pytest.raises(AelParseException, match="Path must not be null or empty"):
            parse_ctx("")

        with pytest.raises(AelParseException, match=r"Could not parse the given AEL path input|no viable alternative|line \d+:\d+"):
            _parse_ctx_quiet("$..listBin1")

        with pytest.raises(AelParseException, match=r"Could not parse the given AEL path input|no viable alternative|line \d+:\d+"):
            _parse_ctx_quiet("$listBin1")

    def test_list_expression_one_level(self):
        _assert_ctx_eq("$.listBin1.[0]", [CTX.list_index(0)])
        _assert_ctx_eq("$.listBin1.[=100]", [CTX.list_value(100)])
        _assert_ctx_eq("$.listBin1.[#-1]", [CTX.list_rank(-1)])

    def test_list_expression_one_level_with_path_function(self):
        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.listBin1.[0].get(type: INT)")

        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.listBin1.[=100].get(type: INT, return: VALUE)")

        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.listBin1.[#-1].asInt()")

    def test_list_expression_one_level_with_full_ael_expression(self):
        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.listBin1.[0] == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.listBin1.[=100].get(type: INT, return: VALUE) == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.listBin1.[#-1].asInt() == 100")

    def test_list_expression_two_levels(self):
        _assert_ctx_eq("$.listBin1.[0].[1]", [CTX.list_index(0), CTX.list_index(1)])
        _assert_ctx_eq("$.listBin1.[0].[=100]", [CTX.list_index(0), CTX.list_value(100)])
        _assert_ctx_eq("$.listBin1.[#-1].[=100]", [CTX.list_rank(-1), CTX.list_value(100)])

    def test_list_expression_three_levels(self):
        _assert_ctx_eq(
            "$.listBin1.[0].[1].[2]",
            [CTX.list_index(0), CTX.list_index(1), CTX.list_index(2)],
        )
        _assert_ctx_eq(
            "$.listBin1.[#-1].[0].[=100]",
            [CTX.list_rank(-1), CTX.list_index(0), CTX.list_value(100)],
        )
        _assert_ctx_eq(
            "$.listBin1.[#-1].[=100].[0]",
            [CTX.list_rank(-1), CTX.list_value(100), CTX.list_index(0)],
        )

    def test_map_expression_only_bin_no_ctx(self):
        with pytest.raises(AelParseException, match="CDT context is not provided"):
            parse_ctx("$.mapBin1")

    def test_map_expression_one_level(self):
        _assert_ctx_eq("$.mapBin1.a", [CTX.map_key("a")])
        _assert_ctx_eq("$.mapBin1.{0}", [CTX.map_index(0)])
        _assert_ctx_eq("$.mapBin1.{#-1}", [CTX.map_rank(-1)])
        _assert_ctx_eq("$.mapBin1.{=100}", [CTX.map_value(100)])

    def test_map_expression_one_level_with_path_function(self):
        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.mapBin1.a.get(type: INT)")

        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.mapBin1.{0}.get(type: INT)")

        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.mapBin1.{=100}.get(type: INT, return: VALUE)")

        with pytest.raises(AelParseException, match="Path function is unsupported"):
            parse_ctx("$.mapBin1.{#-1}.asInt()")

    def test_map_expression_one_level_with_full_ael_expression(self):
        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin1.a == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin1.{0}.get(type: INT, return: VALUE) == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin1.{=100}.asInt() == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin1.{#-1}.asInt() == 100")

        with pytest.raises(AelParseException, match="EXPRESSION_CONTAINER"):
            parse_ctx("$.mapBin1.a > 50")

    def test_map_expression_two_levels(self):
        _assert_ctx_eq("$.mapBin1.{0}.a", [CTX.map_index(0), CTX.map_key("a")])
        _assert_ctx_eq("$.mapBin1.{0}.{=100}", [CTX.map_index(0), CTX.map_value(100)])
        _assert_ctx_eq("$.mapBin1.{#-1}.{=100}", [CTX.map_rank(-1), CTX.map_value(100)])
        _assert_ctx_eq("$.mapBin1.a.bb", [CTX.map_key("a"), CTX.map_key("bb")])

    def test_map_expression_three_levels(self):
        _assert_ctx_eq(
            "$.mapBin1.{0}.a.{#-1}",
            [CTX.map_index(0), CTX.map_key("a"), CTX.map_rank(-1)],
        )
        _assert_ctx_eq(
            "$.mapBin1.{0}.{=100}.a",
            [CTX.map_index(0), CTX.map_value(100), CTX.map_key("a")],
        )
        _assert_ctx_eq(
            "$.mapBin1.{=100}.{#-1}.{0}",
            [CTX.map_value(100), CTX.map_rank(-1), CTX.map_index(0)],
        )

    def test_combined_list_map_expression_four_levels(self):
        _assert_ctx_eq(
            "$.mapBin1.{0}.a.{#-1}.[=100]",
            [
                CTX.map_index(0),
                CTX.map_key("a"),
                CTX.map_rank(-1),
                CTX.list_value(100),
            ],
        )
        _assert_ctx_eq(
            "$.listBin1.[0].[=100].a.{0}",
            [
                CTX.list_index(0),
                CTX.list_value(100),
                CTX.map_key("a"),
                CTX.map_index(0),
            ],
        )
        _assert_ctx_eq(
            "$.mapBin1.{=100}.[#-1].{#-1}.[0]",
            [
                CTX.map_value(100),
                CTX.list_rank(-1),
                CTX.map_rank(-1),
                CTX.list_index(0),
            ],
        )
