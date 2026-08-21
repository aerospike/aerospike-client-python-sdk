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
    count_records_sync,
    explain_plan_blocking,
)
from tests.pac_compat import requires_query_selection



class TestSyncQuerySelectionHintFlags:
    @requires_query_selection
    def test_disallow_scans_with_soft_hint_selects_secondary_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
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
    def test_hard_hint_with_matching_index_selects_hinted_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(index_name=HINT_INDEX_NAME, hard_hint=True),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    @requires_query_selection
    def test_disallow_scans_and_hard_hint_selects_hinted_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
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


class TestSyncQuerySelectionBuilderScanBlocking:
    """``allow_scans_with_where`` enforced through the real SDK query builder
    (``session.query().where().execute()``), not the PAC explain helper."""

    @requires_query_selection
    def test_disallow_scans_via_builder_rejects_scan(self, query_selection_cluster):
        with pytest.raises(AerospikeError) as exc_info:
            (
                query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_strict_default_via_builder_rejects_scan(self, query_selection_cluster):
        with pytest.raises(AerospikeError) as exc_info:
            (
                query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_allow_scans_via_builder_permits_scan(self, query_selection_cluster):
        stream = (
            query_selection_cluster.session.query(namespace=NS, set_name=HINT_SET_NAME)
            .where("$.country == 'US'")
            .with_hint(QueryHint(allow_scans_with_where=True))
            .execute()
        )
        count_records_sync(stream)

    @requires_query_selection
    def test_disallow_hint_overrides_permissive_behavior(self, query_selection_cluster):
        # Resolution runs through the sync client's own create_session(behavior):
        # a hint rejecting the fallback beats a Behavior that allows it.
        permissive = Behavior.DEFAULT.derive_with_changes(
            name="permissive_scans_sync",
            reads_query=Settings(allow_scans_with_where=True),
        )
        session = query_selection_cluster.client.create_session(permissive)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND
