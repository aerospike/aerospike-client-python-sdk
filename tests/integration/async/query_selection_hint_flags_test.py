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

"""Tier D integration tests: ``REQUIRE_INDEX`` and ``HARD_HINT`` on field 44 explain."""

from __future__ import annotations

import pytest
from aerospike_async.exceptions import IndexNotFound

from aerospike_sdk import Behavior, QueryHint, ResultCode
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior_settings import Settings

from tests.integration.query_selection_helpers import (
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    NS,
    QuerySelection,
    QueryWhereFlags,
    count_records_async,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection

# Behavior.DEFAULT is strict; this one opens the primary-index fallback so the
# Behavior leg of the precedence chain can be exercised in both directions.
PERMISSIVE_SCANS = Behavior.DEFAULT.derive_with_changes(
    name="permissive_scans",
    reads_query=Settings(allow_scans_with_where=True),
)


class TestQuerySelectionHintFlags:
    @requires_query_selection
    async def test_disallow_scans_on_primary_index_plan_fails_explain(
        self, query_selection_cluster,
    ):
        """The explain-layer counterpart of ``TestQuerySelectionBuilderScanBlocking``.

        The two tests that used to sit alongside this one moved to
        ``query_selection_error_detail_test``, which asserts through the SDK
        builder; this one stays because no test there covers ``REQUIRE_INDEX``
        on a plan that has no index to fall back to.
        """
        pac = query_selection_cluster.client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                pac,
                "$.country == 'US'",
                set_name=HINT_SET_NAME,
                hint=QueryHint(allow_scans_with_where=False),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    async def test_disallow_scans_with_soft_hint_selects_secondary_index(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(allow_scans_with_where=False, index_name=HINT_SCORE_INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME
        assert plan.where_flags == int(
            QueryWhereFlags.EXPLAIN | QueryWhereFlags.REQUIRE_INDEX
        )

    @requires_query_selection
    async def test_hard_hint_with_matching_index_selects_hinted_index(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(index_name=HINT_INDEX_NAME, hard_hint=True),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    @requires_query_selection
    async def test_disallow_scans_and_hard_hint_selects_hinted_index(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(
                index_name=HINT_INDEX_NAME,
                allow_scans_with_where=False,
                hard_hint=True,
            ),
        )

        assert plan.index_name == HINT_INDEX_NAME
        assert plan.where_flags == int(
            QueryWhereFlags.EXPLAIN
            | QueryWhereFlags.REQUIRE_INDEX
            | QueryWhereFlags.HARD_HINT
        )


class TestQuerySelectionBuilderScanBlocking:
    """``allow_scans_with_where`` enforced through the real SDK query builder
    (``session.query().where().execute()``), not the PAC explain helper."""

    @requires_query_selection
    async def test_disallow_scans_via_builder_rejects_scan(self, query_selection_cluster):
        # A per-query hint disallowing scans rejects the primary-index fallback.
        with pytest.raises(AerospikeError) as exc_info:
            await (
                query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    async def test_strict_default_via_builder_rejects_scan(self, query_selection_cluster):
        # No hint: the Behavior.DEFAULT strict setting blocks the fallback.
        with pytest.raises(AerospikeError) as exc_info:
            await (
                query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    async def test_allow_scans_via_builder_permits_scan(self, query_selection_cluster):
        # allow_scans_with_where=True permits the primary-index fallback: no raise.
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
            .where("$.country == 'US'")
            .with_hint(QueryHint(allow_scans_with_where=True))
            .execute()
        )
        await count_records_async(stream)

    @requires_query_selection
    async def test_permissive_behavior_permits_scan(self, query_selection_cluster):
        # The Behavior alone opens the fallback — no hint involved.
        session = query_selection_cluster.client.create_session(PERMISSIVE_SCANS)
        stream = await (
            session.query(namespace=NS, set_name=HINT_SET_NAME)
            .where("$.country == 'US'")
            .execute()
        )
        await count_records_async(stream)

    @requires_query_selection
    async def test_disallow_hint_overrides_permissive_behavior(
        self, query_selection_cluster,
    ):
        # Precedence in the direction the strict default cannot show: a hint
        # rejecting the fallback beats a Behavior that allows it.
        session = query_selection_cluster.client.create_session(PERMISSIVE_SCANS)
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    async def test_strict_behavior_leaves_unfiltered_scan_alone(
        self, query_selection_cluster,
    ):
        # The setting is scoped to where-clause queries: a deliberate bare scan
        # under the strict default still runs.
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
            .execute()
        )
        assert await count_records_async(stream) > 0
