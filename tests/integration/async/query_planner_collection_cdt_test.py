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

"""MAPKEYS / LIST collection CDT ``.exists()`` planner tests."""

from __future__ import annotations

from aerospike_sdk import DataSet, QueryHint

from tests.integration.query_selection_helpers import (
    CDT_LIST_BIN,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_SET_NAME,
    CDT_SIZE,
    NS,
    QuerySelection,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection



class TestQueryPlannerCollectionCdt:
    @requires_query_selection
    async def test_plan_map_keys_exists_selects_map_keys_index(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        plan = await explain_plan_async(pac, where, set_name=CDT_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == CDT_MAP_INDEX

    @requires_query_selection
    async def test_plan_list_exists_primary_index_fallback(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = f"$.{CDT_LIST_BIN}.[0].exists() == true"
        plan = await explain_plan_async(pac, where, set_name=CDT_SET_NAME)

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    async def test_execute_cdt_exists_without_for_bin_returns_matching_rows(
        self, query_selection_cluster,
    ):
        session = query_selection_cluster.session
        ds = DataSet.of(NS, CDT_SET_NAME)
        map_where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        list_where = f"$.{CDT_LIST_BIN}.[0].exists() == true"

        map_stream = await (
            session.query(ds)
            .bins([CDT_MAP_BIN])
            .where(map_where)
            .execute()
        )
        map_count = 0
        try:
            async for result in map_stream:
                rec = result.record_or_raise()
                assert CDT_MAP_KEY in rec.bins[CDT_MAP_BIN]
                map_count += 1
        finally:
            map_stream.close()
        assert map_count == 10

        list_stream = await (
            session.query(ds)
            .bins([CDT_LIST_BIN])
            .where(list_where)
            # A positional list path stays on the primary index, so this leg
            # still needs the opt-in past the strict default.
            .with_hint(QueryHint(allow_scans_with_where=True))
            .execute()
        )
        list_count = 0
        try:
            async for result in list_stream:
                rec = result.record_or_raise()
                assert len(rec.bins[CDT_LIST_BIN]) == 1
                list_count += 1
        finally:
            list_stream.close()
        assert list_count == CDT_SIZE
