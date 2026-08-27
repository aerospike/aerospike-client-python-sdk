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

"""Field ``44`` explain scope across index shapes (numeric, blob, map-key indexes)."""

from __future__ import annotations

from aerospike_sdk import DataSet

from tests.integration.query_selection_helpers import (
    NS,
    QuerySelection,
    SCOPE_BLOB_BIN,
    SCOPE_BLOB_INDEX,
    SCOPE_INT_INDEX,
    SCOPE_MAP_BIN,
    SCOPE_MAP_INDEX,
    SCOPE_MAP_KEY,
    SCOPE_SET_NAME,
    SCOPE_BLOB_BYTES,
    blob_hex_literal,
    count_records_async,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection



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
