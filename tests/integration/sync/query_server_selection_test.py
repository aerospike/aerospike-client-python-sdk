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

"""Sync integration tests for two-phase server query selection."""

from __future__ import annotations

import pytest

from aerospike_sdk import Exp, QueryDuration, QueryHint, ResultCode, val
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    BOGUS_INDEX_NAME,
    INDEX_NAME,
    NS,
    QuerySelection,
    SCORE_INDEX_NAME,
    SET_NAME,
    SIZE,
    collect_ages_sync,
    collect_scores_sync,
    count_records_sync,
    explain_plan_blocking,
)
from tests.pac_compat import requires_query_selection, requires_server_compiled_ael



class TestSyncQueryExplain:
    @requires_query_selection
    def test_range_selects_secondary_index(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = explain_plan_blocking(pac, "$.age >= 14 and $.age <= 18")

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    @requires_query_selection
    def test_non_indexed_predicate_selects_primary(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = explain_plan_blocking(pac, "$.country == 'US'")

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    def test_contradiction_filtered_out(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = explain_plan_blocking(pac, "$.age > 100 and $.age < 10")
        assert plan.selection == QuerySelection.FILTERED_OUT

    @requires_query_selection
    def test_for_index_hint(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = explain_plan_blocking(
            pac, where, hint=QueryHint(index_name=INDEX_NAME),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    @requires_query_selection
    def test_plan_bytes_stable_across_repeated_probes(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        first = explain_plan_blocking(pac, where)
        second = explain_plan_blocking(pac, where)

        assert first.selection == QuerySelection.SECONDARY_INDEX
        assert first.index_name == INDEX_NAME
        assert second.selection == first.selection
        assert second.index_name == first.index_name
        assert second.ael == first.ael

    @requires_query_selection
    def test_for_index_hint_on_nonexistent_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = explain_plan_blocking(
            pac, where, hint=QueryHint(index_name=BOGUS_INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert plan.index_name != BOGUS_INDEX_NAME

    @requires_query_selection
    def test_for_index_hint_on_wrong_existing_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=SCORE_INDEX_NAME)
        plan = explain_plan_blocking(pac, where, hint=hint)

        ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute(),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert plan.index_name != SCORE_INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]


class TestSyncQueryExecute:
    @requires_server_compiled_ael
    @requires_query_selection
    def test_simple_range(self, qsel_client):
        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where("$.age >= 14 and $.age <= 18")
            .execute()
        )
        assert collect_ages_sync(stream) == [14, 15, 16, 17, 18]

    @requires_server_compiled_ael
    @requires_query_selection
    def test_equality_returns_single_record(self, qsel_client):
        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where("$.age == 25")
            .execute()
        )
        assert collect_ages_sync(stream) == [25]

    @requires_server_compiled_ael
    @requires_query_selection
    def test_primary_index_predicate(self, qsel_client):
        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_COUNTRY])
            .where("$.country == 'US'")
            .execute()
        )
        countries = []
        try:
            for result in stream:
                countries.append(result.record_or_raise().bins[BIN_COUNTRY])
        finally:
            stream.close()
        assert len(countries) == 25
        assert all(c == "US" for c in countries)

    @requires_query_selection
    def test_plan_then_execute_consistency(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = explain_plan_blocking(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        assert collect_ages_sync(stream) == [14, 15, 16, 17, 18]

    @requires_query_selection
    def test_compound_predicate(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age > 30 and $.country == 'US'"
        plan = explain_plan_blocking(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE, BIN_COUNTRY])
            .where(where)
            .execute()
        )
        ages = []
        try:
            for result in stream:
                rec = result.record_or_raise()
                assert rec.bins[BIN_COUNTRY] == "US"
                assert rec.bins[BIN_AGE] > 30
                ages.append(rec.bins[BIN_AGE])
        finally:
            stream.close()
        assert sorted(ages) == [32, 34, 36, 38, 40, 42, 44, 46, 48, 50]

    @requires_query_selection
    def test_reading_only_bins_projects_requested_bins(self, qsel_client):
        where = "$.age >= 14 and $.age <= 18"
        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        ages = []
        try:
            for result in stream:
                rec = result.record_or_raise()
                ages.append(rec.bins[BIN_AGE])
                assert BIN_COUNTRY not in rec.bins
        finally:
            stream.close()
        assert sorted(ages) == [14, 15, 16, 17, 18]

    @requires_server_compiled_ael
    @requires_query_selection
    def test_contradiction_raises_filtered_out(self, qsel_client):
        with pytest.raises(AerospikeError) as exc_info:
            qsel_client.query(NS, SET_NAME).where(
                "$.age > 100 and $.age < 10",
            ).execute()
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    @requires_query_selection
    def test_empty_secondary_index_result(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age == 999"
        plan = explain_plan_blocking(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        count = count_records_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute(),
        )
        assert count == 0


class TestSyncQuerySelectionRouting:
    @requires_query_selection
    def test_for_index_hint_probes_and_executes(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=INDEX_NAME)
        plan = explain_plan_blocking(pac, where, hint=hint)
        ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute(),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    def test_query_duration_only_hint_still_probes_and_executes(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(query_duration=QueryDuration.SHORT)
        plan = explain_plan_blocking(pac, where, hint=hint)
        ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute(),
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    def test_where_exp_uses_non_probe_execute_path(self, qsel_client):
        ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(
                Exp.and_([
                    Exp.ge(Exp.int_bin(BIN_AGE), val(14)),
                    Exp.le(Exp.int_bin(BIN_AGE), val(18)),
                ]),
            )
            .execute(),
        )
        assert ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    def test_multiple_indexes_auto_select(self, qsel_client):
        pac = qsel_client.underlying_client
        age_where = "$.age >= 14 and $.age <= 18"
        score_where = "$.score >= 40 and $.score <= 44"

        age_plan = explain_plan_blocking(pac, age_where)
        score_plan = explain_plan_blocking(pac, score_where)
        ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(age_where)
            .execute(),
        )
        scores = collect_scores_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_SCORE])
            .where(score_where)
            .execute(),
        )

        assert age_plan.index_name == INDEX_NAME
        assert score_plan.index_name == SCORE_INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]
        assert scores == [40, 41, 42, 43, 44]

    @requires_query_selection
    def test_no_where_scan(self, qsel_client):
        count = count_records_sync(qsel_client.query(NS, SET_NAME).execute())
        assert count == SIZE
