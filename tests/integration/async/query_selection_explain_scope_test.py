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

"""Field ``44`` explain scope across index shapes.

Covers scalar, blob, geo, map-key, CDT-context, and expression indexes. PAC's
query plan exposes the selection, index name, and collection type but not the
index-range bytes, so those are not asserted here.
"""

from __future__ import annotations

import pytest

from aerospike_sdk import CollectionIndexType, DataSet

from tests.integration.query_selection_helpers import (
    NS,
    QuerySelection,
    SCOPE_AGE_BIN,
    SCOPE_AGE_PLUS_INDEX,
    SCOPE_AGE_PLUS_MATCH,
    SCOPE_BLOB_BIN,
    SCOPE_BLOB_INDEX,
    SCOPE_INT_INDEX,
    SCOPE_LARGE_REGION,
    SCOPE_LOC_BIN,
    SCOPE_LOC_INDEX,
    SCOPE_MAP_BIN,
    SCOPE_MAP_INDEX,
    SCOPE_MAP_KEY,
    SCOPE_MATCH_POINT,
    SCOPE_NAME_BIN,
    SCOPE_PT_BIN,
    SCOPE_PT_INDEX,
    SCOPE_SCORE_LIST_BIN,
    SCOPE_SCORE_LIST_INDEX,
    SCOPE_SCORE_LIST_MATCH,
    SCOPE_SCORE_LIST_POSITION,
    SCOPE_SET_NAME,
    SCOPE_BLOB_BYTES,
    SCOPE_TAG_BIN,
    SCOPE_TAG_INDEX,
    SCOPE_TAG_MATCH,
    SCOPE_UPPER_MATCH,
    SCOPE_VENUE_BIN,
    SCOPE_VENUE_INDEX,
    SCOPE_VENUE_KEY,
    STRING_BOUND_MAX,
    blob_hex_literal,
    count_matches_async,
    count_records_async,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection

SCOPE_DS = DataSet.of(NS, SCOPE_SET_NAME)
_GEO_MATCH = f"geoCompare($.{SCOPE_LOC_BIN}, geoJson('{SCOPE_MATCH_POINT}'))"
_LARGE_REGION_MATCH = f"geoCompare($.{SCOPE_PT_BIN}, geoJson('{SCOPE_LARGE_REGION}'))"
_CTX_SCALAR_MATCH = (
    f"$.{SCOPE_SCORE_LIST_BIN}.[{SCOPE_SCORE_LIST_POSITION}] == {SCOPE_SCORE_LIST_MATCH}"
)
_CTX_GEO_MATCH = (
    f"geoCompare($.{SCOPE_VENUE_BIN}.{SCOPE_VENUE_KEY}, geoJson('{SCOPE_MATCH_POINT}'))"
)
_EXP_ARITH_MATCH = f"($.{SCOPE_AGE_BIN} + 1) == {SCOPE_AGE_PLUS_MATCH}"
_UPPER_MATCH = f"$.{SCOPE_NAME_BIN}.upper() == '{SCOPE_UPPER_MATCH}'"


class TestQuerySelectionExplainScope:
    @requires_query_selection
    async def test_explain_scalar_integer_secondary_index_succeeds(
        self, query_selection_cluster,
    ):
        client = query_selection_cluster.client
        pac = client.underlying_client
        plan = await explain_plan_async(
            pac, "$.age == 25", set_name=SCOPE_SET_NAME,
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_INT_INDEX

    @requires_query_selection
    async def test_explain_scalar_string_primary_index_no_index_fields(
        self, query_selection_cluster,
    ):
        client = query_selection_cluster.client
        pac = client.underlying_client
        plan = await explain_plan_async(
            pac, "$.country == 'US'", set_name=SCOPE_SET_NAME,
        )

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    async def test_explain_blob_equality_selects_secondary_index(
        self, query_selection_cluster,
    ):
        client = query_selection_cluster.client
        pac = client.underlying_client
        where = f"$.{SCOPE_BLOB_BIN} == x'{blob_hex_literal(SCOPE_BLOB_BYTES)}'"
        plan = await explain_plan_async(pac, where, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_BLOB_INDEX

    @requires_query_selection
    async def test_explain_map_keys_exists_selects_map_keys_index(
        self, query_selection_cluster,
    ):
        client = query_selection_cluster.client
        pac = client.underlying_client
        where = f"$.{SCOPE_MAP_BIN}.{SCOPE_MAP_KEY}.exists() == true"
        plan = await explain_plan_async(pac, where, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_MAP_INDEX

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "index_name"),
        [
            pytest.param(f"$.{SCOPE_TAG_BIN} == '{SCOPE_TAG_MATCH}'", SCOPE_TAG_INDEX, id="string"),
            # The planner unwraps a comparison against ``true``, so both forms share a plan.
            pytest.param(_GEO_MATCH, SCOPE_LOC_INDEX, id="geo_compare"),
            pytest.param(f"{_GEO_MATCH} == true", SCOPE_LOC_INDEX, id="geo_compare_eq_true"),
            pytest.param(_CTX_SCALAR_MATCH, SCOPE_SCORE_LIST_INDEX, id="ctx_path_scalar"),
            pytest.param(_CTX_GEO_MATCH, SCOPE_VENUE_INDEX, id="ctx_path_geo"),
        ],
    )
    async def test_explain_selects_default_secondary_index(
        self, query_selection_cluster, where, index_name,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(pac, where, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == index_name
        assert plan.index_type == CollectionIndexType.DEFAULT

    @requires_query_selection
    async def test_explain_large_geo_region_selects_secondary_index(
        self, query_selection_cluster,
    ):
        """A region literal past the STRING/BLOB bound still plans onto the geo index.

        Geo bounds are capped at the GeoJSON wire limit rather than the
        STRING/BLOB one, so a large region must not fall back to a scan.
        """
        assert len(SCOPE_LARGE_REGION) > STRING_BOUND_MAX
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(pac, _LARGE_REGION_MATCH, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_PT_INDEX
        assert plan.index_type == CollectionIndexType.DEFAULT

    @requires_query_selection
    async def test_explain_expression_arithmetic_selects_expression_index(
        self, query_selection_cluster,
    ):
        """``$.age + 1`` matches the index built over the same arithmetic expression."""
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(pac, _EXP_ARITH_MATCH, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_AGE_PLUS_INDEX

    @requires_query_selection
    async def test_explain_string_path_function_stays_on_primary_index(
        self, query_selection_cluster,
    ):
        """``.upper()`` never matches the uppercase expression index.

        AEL compiles the path function as a CDT string op while the index holds
        a client expression, so the two never compare equal; the predicate still
        evaluates as a residual filter.
        """
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(pac, _UPPER_MATCH, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    async def test_execute_blob_equality_returns_matching_row(
        self, query_selection_cluster,
    ):
        session = query_selection_cluster.session
        where = f"$.{SCOPE_BLOB_BIN} == x'{blob_hex_literal(SCOPE_BLOB_BYTES)}'"

        stream = await (
            session.query(DataSet.of(NS, SCOPE_SET_NAME))
            .bins([SCOPE_BLOB_BIN])
            .where(where)
            .execute()
        )
        assert await count_records_async(stream) == 1

    @requires_query_selection
    async def test_execute_map_keys_exists_returns_matching_rows(
        self, query_selection_cluster,
    ):
        session = query_selection_cluster.session
        where = f"$.{SCOPE_MAP_BIN}.{SCOPE_MAP_KEY}.exists() == true"

        stream = await (
            session.query(DataSet.of(NS, SCOPE_SET_NAME))
            .bins([SCOPE_MAP_BIN])
            .where(where)
            # map-keys-exists is served by SCOPE_MAP_INDEX now, so no scan and
            # no opt-in past the strict allow_scans_with_where default.
            .execute()
        )
        count = 0
        try:
            async for result in stream:
                rec = result.record_or_raise()
                assert SCOPE_MAP_KEY in rec.bins[SCOPE_MAP_BIN]
                count += 1
        finally:
            stream.close()
        assert count > 0

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "bin_name", "needs_scan_opt_in"),
        [
            pytest.param(
                f"$.{SCOPE_TAG_BIN} == '{SCOPE_TAG_MATCH}'", SCOPE_TAG_BIN, False, id="string",
            ),
            pytest.param(_GEO_MATCH, SCOPE_LOC_BIN, False, id="geo_compare"),
            # Replays a multi-kilobyte index range on execute.
            pytest.param(_LARGE_REGION_MATCH, SCOPE_PT_BIN, False, id="large_geo_region"),
            pytest.param(_CTX_SCALAR_MATCH, SCOPE_SCORE_LIST_BIN, False, id="ctx_path_scalar"),
            pytest.param(_CTX_GEO_MATCH, SCOPE_VENUE_BIN, False, id="ctx_path_geo"),
            pytest.param(_EXP_ARITH_MATCH, SCOPE_AGE_BIN, False, id="expression_arithmetic"),
            pytest.param(_UPPER_MATCH, SCOPE_NAME_BIN, True, id="string_path_function"),
        ],
    )
    async def test_execute_returns_matching_row(
        self, query_selection_cluster, where, bin_name, needs_scan_opt_in,
    ):
        """Each shape returns its one matching row; only scan-planned shapes need the opt-in.

        Every indexed shape here runs under the strict default. The
        ``.upper()`` predicate is the exception: it plans onto the primary
        index, so the default refuses it and it reads only with
        ``allow_scans_with_where``.
        """
        count = await count_matches_async(
            query_selection_cluster.session, SCOPE_DS, where, bin_name,
            needs_scan_opt_in=needs_scan_opt_in,
        )
        assert count == 1
