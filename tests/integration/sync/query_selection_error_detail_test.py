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

"""Sync mirror: extended error detail on a query-selection rejection."""

from __future__ import annotations

import pytest

from aerospike_sdk import ErrorDetailVerbosity, QueryHint, ResultCode
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings

from tests.integration.query_selection_helpers import HINT_SET_NAME, NS
from tests.pac_compat import requires_query_selection


def _verbose_session(query_selection_cluster, verbosity):
    behavior = Behavior(
        f"qsel-error-detail-{verbosity}",
        {Scope.ALL: Settings(error_detail_verbosity=verbosity)},
    )
    return query_selection_cluster.client.create_session(behavior=behavior)


class TestSyncQuerySelectionErrorDetail:
    @requires_query_selection
    def test_disallow_scans_index_not_found_carries_no_subcode(
        self, query_selection_cluster, supports_error_detail,
    ):
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _verbose_session(query_selection_cluster, ErrorDetailVerbosity.MESSAGE)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND
        # The rejection carries no refining sub_code (JSDK: SubCode.NONE).
        assert exc_info.value.sub_code in (None, 0)
