# Copyright 2025-2026 Aerospike, Inc.
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

"""Unit tests for printf-style parameter binding on AEL templates."""

from unittest.mock import MagicMock

import pytest

from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.server_filter import bind_ael_params


def _query_builder():
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test")


class TestPassthrough:
    """No params means the template is never treated as a format string."""

    def test_plain_ael_unchanged(self):
        assert bind_ael_params("$.age > 30", ()) == "$.age > 30"

    def test_bare_modulo_survives_without_params(self):
        """AEL's ``%`` operator needs no escaping when no params are supplied."""
        assert bind_ael_params("$.id % 100 == 0", ()) == "$.id % 100 == 0"

    def test_non_string_passes_through(self):
        expression = MagicMock()
        assert bind_ael_params(expression, ()) is expression


class TestJavaParity:
    """Outputs must match what Java's ``String.format`` produces."""

    @pytest.mark.parametrize(
        ("template", "params", "expected"),
        [
            ("$.age > %d", (30,), "$.age > 30"),
            ("$.name == '%s'", ("Alice",), "$.name == 'Alice'"),
            ("$.score > %s", (3.14,), "$.score > 3.14"),
            ("$.score > %f", (3.14,), "$.score > 3.140000"),
            ("$.name == '%s'", ("O'Brien",), "$.name == 'O'Brien'"),
            (
                "$.a > %d and $.b == '%s'",
                (1, "x"),
                "$.a > 1 and $.b == 'x'",
            ),
        ],
    )
    def test_matches_string_format(self, template, params, expected):
        assert bind_ael_params(template, params) == expected


class TestBooleanLowering:
    """Python's ``%s`` yields ``True``; AEL and Java both want ``true``."""

    def test_true_is_lowered(self):
        assert bind_ael_params("$.flag == %s", (True,)) == "$.flag == true"

    def test_false_is_lowered(self):
        assert bind_ael_params("$.flag == %s", (False,)) == "$.flag == false"

    def test_int_one_is_not_lowered(self):
        """``1 is True`` is false in Python, so integers keep their identity."""
        assert bind_ael_params("$.n == %d", (1,)) == "$.n == 1"


class TestErrors:
    def test_params_with_non_string_expression(self):
        with pytest.raises(TypeError, match="string template"):
            bind_ael_params(MagicMock(), (1,))

    def test_unescaped_modulo_reports_the_fix(self):
        with pytest.raises(ValueError, match=r"'%%'"):
            bind_ael_params("$.id % 100 == 0 and $.age > %d", (30,))

    def test_escaped_modulo_binds(self):
        assert (
            bind_ael_params("$.id %% 100 == 0 and $.age > %d", (30,))
            == "$.id % 100 == 0 and $.age > 30"
        )


class TestBuilderWiring:
    """The bound string is what reaches the builder's pending-AEL slot."""

    def test_where_binds_params(self):
        qb = _query_builder().where("$.age > %d", 30)
        assert qb._where_ael == "$.age > 30"

    def test_where_without_params_unchanged(self):
        qb = _query_builder().where("$.id % 100 == 0")
        assert qb._where_ael == "$.id % 100 == 0"

    def test_default_where_binds_params(self):
        qb = _query_builder().default_where("$.status == '%s'", "active")
        assert qb._default_where_ael == "$.status == 'active'"

    def test_where_rejects_params_on_filter_expression(self):
        with pytest.raises(TypeError, match="string template"):
            _query_builder().where(MagicMock(), 30)
