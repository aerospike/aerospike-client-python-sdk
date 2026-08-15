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
from aerospike_async.exceptions import IndexNotFound, InvalidRequest

from aerospike_sdk import QueryHint, ResultCode

from tests.integration.query_selection_helpers import (
    HINT_BOGUS_INDEX_NAME,
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    QuerySelection,
    explain_plan_blocking,
)
from tests.pac_compat import requires_query_selection



class TestSyncQuerySelectionHintFlags:
    @requires_query_selection
    def test_require_index_on_primary_index_plan_fails_explain(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            explain_plan_blocking(
                pac,
                "$.country == 'US'",
                set_name=HINT_SET_NAME,
                hint=QueryHint(require_index=True),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_require_index_with_soft_hint_selects_secondary_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(require_index=True, index_name=HINT_SCORE_INDEX_NAME),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

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
    def test_require_index_and_hard_hint_selects_hinted_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = explain_plan_blocking(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(
                index_name=HINT_INDEX_NAME,
                require_index=True,
                hard_hint=True,
            ),
        )
        assert plan.index_name == HINT_INDEX_NAME

    @requires_query_selection
    def test_hard_hint_with_wrong_index_fails_explain(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            explain_plan_blocking(
                pac,
                "$.age == 25",
                set_name=HINT_SET_NAME,
                hint=QueryHint(
                    index_name=HINT_BOGUS_INDEX_NAME,
                    hard_hint=True,
                ),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    def test_bad_ael_fails_explain_with_parameter(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        with pytest.raises(InvalidRequest) as exc_info:
            explain_plan_blocking(pac, "$.age > 30 and", set_name=HINT_SET_NAME)
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
