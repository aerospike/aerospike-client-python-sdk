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

from aerospike_sdk import (
    ErrorDetailVerbosity,
    ExpressionTrace,
    QueryHint,
    ResultCode,
    SubCode,
)
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings

from tests.integration.query_selection_helpers import (
    HINT_BOGUS_INDEX_NAME,
    HINT_SET_NAME,
    NS,
)
from tests.pac_compat import requires_query_selection

# Trailing ``and`` — syntactically invalid AEL.
_BAD_AEL = "$.age > 30 and"
_FILTER_BUILD_MSG = "invalid filter expression in query"
# The parser runs out of input after the dangling ``and``, so it faults at the
# end of the expression rather than at the token itself. Measured on 8.1.3.0,
# where the message agrees: "unexpected end of expression at line 1 col 15".
_BAD_AEL_FAULT_OFFSET = len(_BAD_AEL)
_BAD_AEL_DIAGNOSTIC = "unexpected end of expression"


def _verbose_session(query_selection_cluster, verbosity):
    behavior = Behavior(
        f"qsel-error-detail-{verbosity}",
        {Scope.ALL: Settings(error_detail_verbosity=verbosity)},
    )
    return query_selection_cluster.client.create_session(behavior=behavior)


class TestSyncQuerySelectionErrorDetail:
    """Error detail on the explain path.

    No test here guards on ``supports_error_detail``: that fixture and
    ``@requires_query_selection`` share the 8.1.3.0 threshold, so a cluster
    that reaches these bodies always supplies extended detail.
    """

    @requires_query_selection
    def test_bad_ael_verbosity_none_yields_no_detail(self, query_selection_cluster):
        """Control for the rest of the file: at ``NONE`` the explain path
        attaches nothing, so the detail the other tests assert on is a
        consequence of the requested verbosity rather than something the
        server always sends here."""
        session = _verbose_session(query_selection_cluster, ErrorDetailVerbosity.NONE)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where(_BAD_AEL)
                .execute()
            )
        exc = exc_info.value
        assert exc.result_code == ResultCode.PARAMETER_ERROR
        assert exc.server_message is None
        assert exc.exp_trace is None
        assert exc.sub_code is None

    @requires_query_selection
    def test_bad_ael_verbosity_message_fails_at_explain(self, query_selection_cluster):
        session = _verbose_session(query_selection_cluster, ErrorDetailVerbosity.MESSAGE)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where(_BAD_AEL)
                .execute()
            )
        exc = exc_info.value
        assert exc.result_code == ResultCode.PARAMETER_ERROR
        assert exc.sub_code == SubCode.NONE  # 0, explicitly, not None
        msg = exc.server_message
        assert msg is not None
        assert _FILTER_BUILD_MSG in msg
        assert _BAD_AEL_DIAGNOSTIC in msg
        assert exc.exp_trace is None

    @requires_query_selection
    def test_bad_ael_verbosity_expression_trace_fails_at_explain(self, query_selection_cluster):
        session = _verbose_session(
            query_selection_cluster, ErrorDetailVerbosity.EXPRESSION_TRACE,
        )
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where(_BAD_AEL)
                .execute()
            )
        exc = exc_info.value
        assert exc.result_code == ResultCode.PARAMETER_ERROR
        assert exc.sub_code == SubCode.NONE  # 0, explicitly, not None
        msg = exc.server_message
        assert msg is not None
        assert _FILTER_BUILD_MSG in msg
        trace = exc.exp_trace
        assert trace is not None
        assert trace.phase == ExpressionTrace.PHASE_BUILD
        assert trace.lang == ExpressionTrace.LANG_AEL
        # Pinned, not ``>= 0``: ael_offset is optional, so a trace that failed
        # to localize comes back as None and would raise TypeError rather than
        # fail. The packed path in batch_test leaves it unset for that reason.
        assert trace.ael_offset == _BAD_AEL_FAULT_OFFSET

    @requires_query_selection
    def test_disallow_scans_index_not_found_carries_no_subcode(self, query_selection_cluster):
        session = _verbose_session(query_selection_cluster, ErrorDetailVerbosity.MESSAGE)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.country == 'US'")
                .with_hint(QueryHint(allow_scans_with_where=False))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND
        # No sub_code at all, not SubCode.NONE: a planner rejection carries no
        # detail block, unlike the filter-build failures above which report 0.
        assert exc_info.value.sub_code is None

    @requires_query_selection
    def test_hard_hint_wrong_index_carries_no_subcode(self, query_selection_cluster):
        session = _verbose_session(query_selection_cluster, ErrorDetailVerbosity.MESSAGE)
        with pytest.raises(AerospikeError) as exc_info:
            (
                session.query(namespace=NS, set_name=HINT_SET_NAME)
                .where("$.age == 51")
                .with_hint(QueryHint(index_name=HINT_BOGUS_INDEX_NAME, hard_hint=True))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND
        assert exc_info.value.sub_code is None
