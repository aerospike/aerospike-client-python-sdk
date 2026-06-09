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

"""Unit tests for the ``unknown`` and ``error`` AEL keywords.

Both keywords compile to ``Exp.unknown()`` — at evaluation the server raises
an evaluator-unknown error, which short-circuits enclosing expressions.
"""

import pytest
from aerospike_async import ExpType, MapReturnType
from aerospike_sdk import Exp, parse_ael


class TestUnknownAndErrorAreEquivalent:
    """``unknown`` and ``error`` both lower to ``Exp.unknown()``."""

    def test_unknown_is_exp_unknown(self):
        assert parse_ael("unknown") == Exp.unknown()

    def test_error_is_exp_unknown(self):
        assert parse_ael("error") == Exp.unknown()

    def test_unknown_equals_error(self):
        assert parse_ael("unknown") == parse_ael("error")


class TestUnknownInComparisons:
    """``unknown`` / ``error`` compose with comparison and logical operators."""

    def test_eq_unknown(self):
        expected = Exp.eq(Exp.int_bin("a"), Exp.unknown())
        assert parse_ael("$.a == unknown") == expected

    def test_eq_error(self):
        expected = Exp.eq(Exp.int_bin("a"), Exp.unknown())
        assert parse_ael("$.a == error") == expected

    def test_unknown_in_and(self):
        expected = Exp.and_([
            Exp.eq(Exp.int_bin("a"), Exp.int_val(5)),
            Exp.unknown(),
        ])
        assert parse_ael("$.a == 5 and unknown") == expected

    def test_unknown_in_or(self):
        expected = Exp.or_([
            Exp.eq(Exp.int_bin("a"), Exp.int_val(5)),
            Exp.unknown(),
        ])
        assert parse_ael("$.a == 5 or unknown") == expected

    def test_not_unknown(self):
        expected = Exp.not_(Exp.unknown())
        assert parse_ael("not(unknown)") == expected


class TestUnknownInControlStructures:
    """``unknown`` / ``error`` may be the action in ``when`` / branches."""

    def test_unknown_as_when_action(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("a"), Exp.int_val(1)),
            Exp.unknown(),
            Exp.int_val(0),
        ])
        assert parse_ael("when($.a == 1 => unknown, default => 0)") == expected

    def test_error_as_default(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("a"), Exp.int_val(1)),
            Exp.int_val(10),
            Exp.unknown(),
        ])
        assert parse_ael("when($.a == 1 => 10, default => error)") == expected


class TestKeywordsStillUsableAsBinNames:
    """``unknown`` / ``error`` remain valid bin names — they only become
    operands when used in operand position."""

    @pytest.mark.parametrize("name", ["unknown", "error"])
    def test_keyword_as_bin(self, name):
        expected = Exp.eq(Exp.int_bin(name), Exp.int_val(1))
        assert parse_ael(f"$.{name} == 1") == expected

    @pytest.mark.parametrize("name", ["unknown", "error"])
    def test_keyword_as_quoted_map_key(self, name):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val(name), Exp.map_bin("mb"), [],
            ),
            Exp.int_val(1),
        )
        assert parse_ael(f"$.mb.'{name}' == 1") == expected
