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

"""Integration tests for two-phase server query selection (explain → execute).

Requires Aerospike cluster on ``AEROSPIKE_HOST``. Tests are skipped when PAC
reports no query-selection support (``Version.supports_query_selection()``).
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from aerospike_sdk import DataSet, Exp, Filter, QueryDuration, QueryHint, ResultCode, val
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    BOGUS_INDEX_NAME,
    INDEX_NAME,
    NS,
    QuerySelection,
    QuerySelectionClientFacade,
    SCORE_INDEX_NAME,
    SET_NAME,
    SIZE,
    collect_ages_async,
    collect_scores_async,
    count_records_async,
    explain_plan_async,
    key_name,
    requires_pac_query_selection_api,
    skip_unless_query_selection,
)

pytestmark = requires_pac_query_selection_api


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def qsel_client(
    aerospike_host,
    make_cluster_definition,
    supports_query_selection,
    wait_for_index,
    wait_for_set_visible,
):
    skip_unless_query_selection(supports_query_selection)

    cluster_def = make_cluster_definition(aerospike_host)
    cluster_def.with_index_refresh_interval(0.25)
    async with await cluster_def.connect() as cluster:
        client = cluster._sdk_client
        session = cluster.create_session()
        ds = DataSet.of(NS, SET_NAME)

        for i in range(1, SIZE + 1):
            try:
                await session.delete(ds.id(key_name(i))).execute()
            except Exception:
                pass

        for i in range(1, SIZE + 1):
            country = "US" if i % 2 == 0 else "CA"
            await (
                session.upsert(ds.id(key_name(i)))
                .put({BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: country})
                .execute()
            )

        await wait_for_set_visible(session, NS, SET_NAME, SIZE)

        for index_name, bin_name in (
            (INDEX_NAME, BIN_AGE),
            (SCORE_INDEX_NAME, BIN_SCORE),
        ):
            try:
                await (
                    client.index(NS, SET_NAME)
                    .on_bin(bin_name)
                    .named(index_name)
                    .numeric()
                    .create()
                )
            except Exception:
                pass

        await wait_for_index(
            client, NS, SET_NAME, Filter.range(BIN_AGE, 1, SIZE),
        )
        await wait_for_index(
            client, NS, SET_NAME, Filter.range(BIN_SCORE, 1, SIZE),
        )

        yield QuerySelectionClientFacade(client, session)

        for i in range(1, SIZE + 1):
            try:
                await session.delete(ds.id(key_name(i))).execute()
            except Exception:
                pass
        for index_name in (INDEX_NAME, SCORE_INDEX_NAME):
            try:
                await client.index(NS, SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestQueryExplain:
    async def test_range_selects_secondary_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await pac.query_explain(NS, where, set_name=SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.namespace == NS
        assert plan.set_name == SET_NAME
        assert plan.index_name == INDEX_NAME
        assert plan.is_secondary_index

    async def test_non_indexed_predicate_selects_primary(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = await pac.query_explain(
            NS, "$.country == 'US'", set_name=SET_NAME,
        )

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.is_primary_index
        assert plan.index_name is None

    async def test_contradiction_filtered_out(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = await pac.query_explain(
            NS, "$.age > 100 and $.age < 10", set_name=SET_NAME,
        )

        assert plan.selection == QuerySelection.FILTERED_OUT
        assert plan.is_filtered_out

    async def test_for_index_hint(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(
            pac, where, hint=QueryHint(index_name=INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    async def test_plan_bytes_stable_across_repeated_probes(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"

        first = await explain_plan_async(pac, where)
        second = await explain_plan_async(pac, where)

        assert first.selection == QuerySelection.SECONDARY_INDEX
        assert first.index_name == INDEX_NAME
        assert second.selection == first.selection
        assert second.index_name == first.index_name
        assert second.ael == first.ael

    async def test_index_probe_planner_smoke(self, qsel_client):
        """PAC explain path (Python equivalent of Java ``IndexProbePlanner.plan``)."""
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(pac, where)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert plan.ael is not None

    async def test_for_index_hint_on_nonexistent_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(
            pac, where, hint=QueryHint(index_name=BOGUS_INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name != BOGUS_INDEX_NAME
        assert plan.index_name == INDEX_NAME

    async def test_for_index_hint_on_wrong_existing_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=SCORE_INDEX_NAME)
        plan = await explain_plan_async(pac, where, hint=hint)

        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute()
        )
        ages = await collect_ages_async(stream)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name != SCORE_INDEX_NAME
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]


class TestQueryExecute:
    async def test_simple_range_returns_matching_records(self, qsel_client):
        where = "$.age >= 14 and $.age <= 18"
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        ages = await collect_ages_async(stream)
        assert ages == [14, 15, 16, 17, 18]

    async def test_equality_returns_single_record(self, qsel_client):
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where("$.age == 25")
            .execute()
        )
        ages = await collect_ages_async(stream)
        assert ages == [25]

    async def test_primary_index_predicate(self, qsel_client):
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_COUNTRY])
            .where("$.country == 'US'")
            .execute()
        )
        countries = []
        try:
            async for result in stream:
                rec = result.record_or_raise()
                countries.append(rec.bins[BIN_COUNTRY])
        finally:
            stream.close()
        assert len(countries) == 25
        assert all(c == "US" for c in countries)

    async def test_plan_then_execute_consistency_for_secondary_index(
        self, qsel_client,
    ):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"

        plan = await explain_plan_async(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        assert await collect_ages_async(stream) == [14, 15, 16, 17, 18]

    async def test_compound_predicate(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age > 30 and $.country == 'US'"

        plan = await explain_plan_async(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE, BIN_COUNTRY])
            .where(where)
            .execute()
        )
        ages = []
        try:
            async for result in stream:
                rec = result.record_or_raise()
                assert rec.bins[BIN_COUNTRY] == "US"
                assert rec.bins[BIN_AGE] > 30
                ages.append(rec.bins[BIN_AGE])
        finally:
            stream.close()
        assert sorted(ages) == [32, 34, 36, 38, 40, 42, 44, 46, 48, 50]

    async def test_reading_only_bins_projects_requested_bins(self, qsel_client):
        where = "$.age >= 14 and $.age <= 18"
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        ages = []
        try:
            async for result in stream:
                rec = result.record_or_raise()
                ages.append(rec.bins[BIN_AGE])
                assert BIN_COUNTRY not in rec.bins
        finally:
            stream.close()
        assert sorted(ages) == [14, 15, 16, 17, 18]

    async def test_contradiction_raises_filtered_out(self, qsel_client):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                qsel_client.query(NS, SET_NAME)
                .where("$.age > 100 and $.age < 10")
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    async def test_empty_secondary_index_result(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age == 999"
        plan = await pac.query_explain(NS, where, set_name=SET_NAME)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        count = await count_records_async(stream)
        assert count == 0


class TestQuerySelectionRouting:
    async def test_for_bin_hint_uses_legacy_execute_path(self, qsel_client):
        where = "$.age >= 14 and $.age <= 18"

        default_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        for_bin_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(QueryHint(bin_name=BIN_AGE))
            .execute()
        )

        default_ages = await collect_ages_async(default_stream)
        for_bin_ages = await collect_ages_async(for_bin_stream)
        assert default_ages == for_bin_ages == [14, 15, 16, 17, 18]

    async def test_for_index_hint_probes_and_executes(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=INDEX_NAME)

        plan = await explain_plan_async(pac, where, hint=hint)
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute()
        )
        ages = await collect_ages_async(stream)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    async def test_query_duration_only_hint_still_probes_and_executes(
        self, qsel_client,
    ):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(query_duration=QueryDuration.SHORT)

        plan = await explain_plan_async(pac, where, hint=hint)
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute()
        )
        ages = await collect_ages_async(stream)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    async def test_where_exp_uses_non_probe_execute_path(self, qsel_client):
        stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(
                Exp.and_([
                    Exp.ge(Exp.int_bin(BIN_AGE), val(14)),
                    Exp.le(Exp.int_bin(BIN_AGE), val(18)),
                ]),
            )
            .execute()
        )
        assert await collect_ages_async(stream) == [14, 15, 16, 17, 18]

    async def test_server_led_matches_legacy_for_bin(self, qsel_client):
        where = "$.age > 30 and $.country == 'US'"

        server_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        server_ages = await collect_ages_async(server_stream)

        legacy_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(QueryHint(bin_name=BIN_AGE))
            .execute()
        )
        legacy_ages = await collect_ages_async(legacy_stream)

        assert server_ages == legacy_ages
        assert server_ages == [32, 34, 36, 38, 40, 42, 44, 46, 48, 50]

    async def test_multiple_indexes_auto_select(self, qsel_client):
        pac = qsel_client.underlying_client
        age_where = "$.age >= 14 and $.age <= 18"
        score_where = "$.score >= 40 and $.score <= 44"

        age_plan = await explain_plan_async(pac, age_where)
        score_plan = await explain_plan_async(pac, score_where)

        age_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(age_where)
            .execute()
        )
        score_stream = await (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_SCORE])
            .where(score_where)
            .execute()
        )
        ages = await collect_ages_async(age_stream)
        scores = await collect_scores_async(score_stream)

        assert age_plan.selection == QuerySelection.SECONDARY_INDEX
        assert age_plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]
        assert score_plan.selection == QuerySelection.SECONDARY_INDEX
        assert score_plan.index_name == SCORE_INDEX_NAME
        assert scores == [40, 41, 42, 43, 44]

    async def test_no_where_scan_returns_all_records(self, qsel_client):
        stream = await qsel_client.query(NS, SET_NAME).execute()
        count = await count_records_async(stream)
        assert count == SIZE
