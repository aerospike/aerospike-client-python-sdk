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

"""Unit tests for AEL GeoJSON expressions: geoJson() and geoCompare()."""

import pytest

from aerospike_sdk import AelParseException, Exp, parse_ael


POINT = '{"type":"Point","coordinates":[-122.4,37.7]}'
OTHER_POINT = '{"type":"Point","coordinates":[-122.0986857,37.4214209]}'
AERO_CIRCLE = '{"type":"AeroCircle","coordinates":[[-122.1708441,37.4241193],3000.0]}'


class TestGeoJsonLiteral:
    """``geoJson('...')`` produces a GEO value expression."""

    def test_geo_json_single_quoted(self):
        result = parse_ael(f"geoJson('{POINT}')")
        assert result == Exp.geo_val(POINT)

    def test_geo_json_double_quoted_literal(self):
        # AEL accepts both quote styles; the inner JSON uses ", so wrap with '.
        result = parse_ael(f'geoJson("{POINT.replace(chr(34), chr(39))}")')
        assert result == Exp.geo_val(POINT.replace('"', "'"))

    def test_geo_json_aerocircle(self):
        result = parse_ael(f"geoJson('{AERO_CIRCLE}')")
        assert result == Exp.geo_val(AERO_CIRCLE)


class TestGeoCompare:
    """``geoCompare(a, b)`` compares two GEO operands and yields a BOOL."""

    def test_bin_left_literal_right(self):
        """``geoCompare($.loc, geoJson('...'))`` — bin on left, literal on right."""
        result = parse_ael(f"geoCompare($.loc, geoJson('{OTHER_POINT}'))")
        expected = Exp.geo_compare(Exp.geo_bin("loc"), Exp.geo_val(OTHER_POINT))
        assert result == expected

    def test_literal_left_bin_right(self):
        """Reverse operand order — same expression, swapped sides."""
        result = parse_ael(f"geoCompare(geoJson('{OTHER_POINT}'), $.loc)")
        expected = Exp.geo_compare(Exp.geo_val(OTHER_POINT), Exp.geo_bin("loc"))
        assert result == expected

    def test_explicit_get_type_geo(self):
        """``$.loc.get(type: GEO)`` resolves to a geo bin."""
        result = parse_ael(f"geoCompare($.loc.get(type: GEO), geoJson('{OTHER_POINT}'))")
        expected = Exp.geo_compare(Exp.geo_bin("loc"), Exp.geo_val(OTHER_POINT))
        assert result == expected

    def test_inside_and_clause(self):
        """``geoCompare(...)`` participates in a logical conjunction."""
        result = parse_ael(
            f"$.active == true and geoCompare($.loc, geoJson('{OTHER_POINT}'))",
        )
        expected = Exp.and_([
            Exp.eq(Exp.bool_bin("active"), Exp.bool_val(True)),
            Exp.geo_compare(Exp.geo_bin("loc"), Exp.geo_val(OTHER_POINT)),
        ])
        assert result == expected


class TestGeoErrors:
    """Parse-time errors for malformed geo expressions."""

    def test_geo_json_wrong_arity_zero(self):
        with pytest.raises(AelParseException):
            parse_ael("geoJson()")

    def test_geo_json_wrong_arity_two(self):
        with pytest.raises(AelParseException):
            parse_ael(f"geoJson('{POINT}', '{OTHER_POINT}')")

    def test_geo_json_non_string_arg(self):
        with pytest.raises(AelParseException):
            parse_ael("geoJson(123)")

    def test_geo_compare_wrong_arity_one(self):
        with pytest.raises(AelParseException):
            parse_ael(f"geoCompare(geoJson('{POINT}'))")

    def test_geo_compare_wrong_arity_three(self):
        with pytest.raises(AelParseException):
            parse_ael(
                f"geoCompare(geoJson('{POINT}'), geoJson('{OTHER_POINT}'), $.loc)",
            )
