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

"""Sync Tier D integration tests for query-selection hint flags."""

from __future__ import annotations

import pytest
from aerospike_native.exceptions import IndexNotFound

from aerospike_sdk import Behavior, QueryHint, ResultCode
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior_settings import Settings

from tests.integration.query_selection_helpers import (
    HINT_DS,
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    QuerySelection,
    QueryWhereFlags,
    count_records_sync,
    explain_plan_blocking,
)
from tests.pnc_compat import requires_query_selection

INDEX_ONLY = Behavior.DEFAULT.derive_with_changes(
    name="index_only_sync",
    reads_query=Settings(allow_scans_with_where=False),
)


class TestSyncQuerySelectionHintFlags:
    @requires_query_selection
    def test_disallow_scans_on_primary_index_plan_fails_explain(self, query_selection_cluster):
        """The explain-layer counterpart of ``TestSyncQuerySelectionBuilderScanBlocking``.

        The two tests that used to sit alongside this one moved to
        ``query_selection_error_detail_test``, which asserts through the SDK
        builder; this one stays because no test there covers ``REQUIRE_INDEX``
        on a plan that has no index to fall back to.
        """
        pnc = query_selection_cluster.client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            explain_plan_blocking(
                pnc,
                "$.country == 'US'",
                set_name=HINT_SET_NAME,
                hint=QueryHint(allow_scans_with_where=False),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_disallow_scans_with_soft_hint_selects_secondary_index(self, query_selection_cluster):
        pnc = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pnc,
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
    def test_hard_hint_with_matching_index_selects_hinted_index(self, query_selection_cluster):
        pnc = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pnc,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(index_name=HINT_INDEX_NAME, hard_hint=True),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    @requires_query_selection
    def test_disallow_scans_and_hard_hint_selects_hinted_index(self, query_selection_cluster):
        pnc = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pnc,
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


class TestSyncQuerySelectionBuilderScanBlocking:
    """``allow_scans_with_where`` enforced through the real SDK query builder
    (``session.query().where().execute()``), not the PNC explain helper."""

    @requires_query_selection
    def test_default_via_builder_permits_scan(self, query_selection_cluster):
        stream = (
            query_selection_cluster.session.query(HINT_DS)
            .where("$.country == 'US'")
            .execute()
        )
        assert count_records_sync(stream) > 0

    @requires_query_selection
    def test_disallow_hint_overrides_default_behavior(self, query_selection_cluster):
        with pytest.raises(AerospikeError) as exc_info:
            (
                query_selection_cluster.session.query(HINT_DS)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_index_only_behavior_rejects_scan(self, query_selection_cluster):
        # Resolution runs through the sync client's own create_session(behavior).
        session = query_selection_cluster.client.create_session(INDEX_ONLY)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(HINT_DS)
                .where("$.country == 'US'")
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_allow_hint_overrides_index_only_behavior(self, query_selection_cluster):
        session = query_selection_cluster.client.create_session(INDEX_ONLY)
        stream = (
            session.query(HINT_DS)
            .where("$.country == 'US'")
            .with_hint(QueryHint(allow_scans_with_where=True))
            .execute()
        )
        assert count_records_sync(stream) > 0
