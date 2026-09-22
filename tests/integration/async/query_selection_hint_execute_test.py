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

"""Hints at *execute*, not at explain.

``query_selection_hint_flags_test`` asserts what a hint does to the plan the
server hands back from field 44. This module runs the query and asserts what
comes out the other end: a hard hint that cannot be honored fails, a soft hint
that cannot be honored quietly falls back to the same rows as no hint at all,
and the long-query durations execute rather than merely plan.
"""

from __future__ import annotations

import pytest

from aerospike_sdk import QueryDuration, QueryHint, ResultCode
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.query_selection_helpers import (
    HINT_BOGUS_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    NS,
    count_records_async,
)
from tests.pac_compat import requires_query_selection

# Indexed on `age`, so this predicate can be served by HINT_INDEX_NAME.
INDEXED_PREDICATE = "$.age >= 25"


def _query(state, predicate, hint=None):
    builder = (
        state.session.query(namespace=NS, set_name=HINT_SET_NAME).where(predicate)
    )
    return builder.with_hint(hint) if hint is not None else builder


class TestHardHintAtExecute:
    """A hard hint names the only index the server may use."""

    @requires_query_selection
    async def test_hard_hint_on_the_wrong_index_fails(self, query_selection_cluster):
        """`score` is indexed, but it cannot serve an `age` predicate."""
        with pytest.raises(AerospikeError) as excinfo:
            await _query(
                query_selection_cluster,
                INDEXED_PREDICATE,
                QueryHint(index_name=HINT_SCORE_INDEX_NAME, hard_hint=True),
            ).execute()
        assert excinfo.value.result_code == ResultCode.INDEX_NOT_FOUND

    @requires_query_selection
    async def test_hard_hint_on_a_nonexistent_index_fails(
        self, query_selection_cluster,
    ):
        with pytest.raises(AerospikeError) as excinfo:
            await _query(
                query_selection_cluster,
                INDEXED_PREDICATE,
                QueryHint(index_name=HINT_BOGUS_INDEX_NAME, hard_hint=True),
            ).execute()
        assert excinfo.value.result_code == ResultCode.INDEX_NOT_FOUND


class TestSoftHintAtExecute:
    """A soft hint is a preference: an unusable one falls back silently."""

    @requires_query_selection
    async def test_soft_hint_on_a_nonexistent_index_matches_no_hint(
        self, query_selection_cluster,
    ):
        baseline = await count_records_async(
            await _query(query_selection_cluster, INDEXED_PREDICATE).execute()
        )
        hinted = await count_records_async(
            await _query(
                query_selection_cluster,
                INDEXED_PREDICATE,
                QueryHint(index_name=HINT_BOGUS_INDEX_NAME),
            ).execute()
        )
        assert hinted == baseline > 0

    @requires_query_selection
    async def test_soft_hint_on_the_wrong_index_matches_no_hint(
        self, query_selection_cluster,
    ):
        baseline = await count_records_async(
            await _query(query_selection_cluster, INDEXED_PREDICATE).execute()
        )
        hinted = await count_records_async(
            await _query(
                query_selection_cluster,
                INDEXED_PREDICATE,
                QueryHint(index_name=HINT_SCORE_INDEX_NAME),
            ).execute()
        )
        assert hinted == baseline > 0


class TestRequireIndexAtExecute:
    """``allow_scans_with_where=False`` forbids the primary-index fallback."""

    @requires_query_selection
    async def test_require_index_still_returns_rows_when_an_index_exists(
        self, query_selection_cluster,
    ):
        """The index is found by the server; no index name is supplied."""
        stream = await _query(
            query_selection_cluster,
            INDEXED_PREDICATE,
            QueryHint(allow_scans_with_where=False),
        ).execute()
        assert await count_records_async(stream) > 0


class TestQueryDurationAtExecute:
    """``LONG`` is covered in ``query_hint_test``; this is the relax-AP variant."""

    @requires_query_selection
    async def test_long_relax_ap_returns_rows_on_an_ap_namespace(
        self, query_selection_cluster,
    ):
        stream = await _query(
            query_selection_cluster,
            INDEXED_PREDICATE,
            QueryHint(query_duration=QueryDuration.LONG_RELAX_AP),
        ).execute()
        assert await count_records_async(stream) > 0
