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

from aerospike_async import IndexType

from aerospike_sdk import DataSet, Exp, QueryDuration, QueryHint, ResultCode, val
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
    collect_ages_async,
    collect_scores_async,
    count_records_async,
    create_index_quiet_async,
    drop_index_quiet_async,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection, requires_server_compiled_ael

# Long enough that the planner cannot carry it as index range bytes, so the
# plan falls back to the primary index instead of a secondary one.
OVERSIZED_LITERAL = "x" * 2048
# Scans behind a where() are refused by Behavior.DEFAULT, so the tests that
# want to observe a primary-index fallback have to permit them.
PERMIT_SCANS = QueryHint(allow_scans_with_where=True)



class TestQueryExplain:
    @requires_query_selection
    async def test_range_selects_secondary_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await pac.query_explain(NS, where, set_name=SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.namespace == NS
        assert plan.set_name == SET_NAME
        assert plan.index_name == INDEX_NAME
        assert plan.is_secondary_index

    @requires_query_selection
    async def test_non_indexed_predicate_selects_primary(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = await pac.query_explain(
            NS, "$.country == 'US'", set_name=SET_NAME,
        )

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.is_primary_index
        assert plan.index_name is None

    @requires_query_selection
    async def test_contradiction_filtered_out(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        plan = await pac.query_explain(
            NS, "$.age > 100 and $.age < 10", set_name=SET_NAME,
        )

        assert plan.selection == QuerySelection.FILTERED_OUT
        assert plan.is_filtered_out

    @requires_query_selection
    async def test_for_index_hint(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(
            pac, where, hint=QueryHint(index_name=INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    @requires_query_selection
    async def test_plan_bytes_stable_across_repeated_probes(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"

        first = await explain_plan_async(pac, where)
        second = await explain_plan_async(pac, where)

        assert first.selection == QuerySelection.SECONDARY_INDEX
        assert first.index_name == INDEX_NAME
        assert second.selection == first.selection
        assert second.index_name == first.index_name
        assert second.ael == first.ael

    @requires_query_selection
    async def test_index_probe_planner_smoke(self, query_selection_cluster):
        """PAC explain path smoke test via ``query_explain``."""
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(pac, where)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert plan.ael is not None

    @requires_query_selection
    async def test_for_index_hint_on_nonexistent_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = await explain_plan_async(
            pac, where, hint=QueryHint(index_name=BOGUS_INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name != BOGUS_INDEX_NAME
        assert plan.index_name == INDEX_NAME

    @requires_query_selection
    async def test_for_index_hint_on_wrong_existing_index(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=SCORE_INDEX_NAME)
        plan = await explain_plan_async(pac, where, hint=hint)

        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
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

    @requires_query_selection
    async def test_oversized_literal_on_an_unindexed_bin_falls_back_to_primary(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac, f"$.{BIN_COUNTRY} == '{OVERSIZED_LITERAL}'",
        )
        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    async def test_oversized_literal_on_an_indexed_bin_falls_back_to_primary(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac, f"$.{BIN_AGE} == '{OVERSIZED_LITERAL}'",
        )
        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    @requires_query_selection
    async def test_oversized_literal_beside_an_indexable_term_still_selects_the_index(
        self, query_selection_cluster,
    ):
        """One unusable conjunct does not cost the plan its usable one."""
        pac = query_selection_cluster.client.underlying_client
        plan = await explain_plan_async(
            pac,
            f"$.{BIN_AGE} >= 14 and $.{BIN_COUNTRY} == '{OVERSIZED_LITERAL}'",
        )
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    @requires_query_selection
    async def test_or_conjunct_with_an_indexable_sibling_selects_the_index(
        self, query_selection_cluster,
    ):
        """An OR the index cannot serve does not disqualify the AND beside it."""
        pac = query_selection_cluster.client.underlying_client
        for where in (
            "$.age > 10 and ($.age < 50 or $.country == 'US')",
            "($.age < 50 or $.country == 'US') and $.age > 10",
        ):
            plan = await explain_plan_async(pac, where)
            assert plan.selection == QuerySelection.SECONDARY_INDEX, where
            assert plan.index_name == INDEX_NAME, where


class TestQueryExecute:
    @requires_query_selection
    async def test_simple_range_returns_matching_records(self, query_selection_cluster):
        where = "$.age >= 14 and $.age <= 18"
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        ages = await collect_ages_async(stream)
        assert ages == [14, 15, 16, 17, 18]

    @requires_server_compiled_ael
    @requires_query_selection
    async def test_equality_returns_single_record(self, query_selection_cluster):
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where("$.age == 25")
            .execute()
        )
        ages = await collect_ages_async(stream)
        assert ages == [25]

    @requires_server_compiled_ael
    @requires_query_selection
    async def test_primary_index_predicate(self, query_selection_cluster):
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_COUNTRY])
            .where("$.country == 'US'")
            # No secondary index on country -> primary-index scan; opt into it
            # past the strict allow_scans_with_where default.
            .with_hint(QueryHint(allow_scans_with_where=True))
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

    @requires_query_selection
    async def test_plan_then_execute_consistency_for_secondary_index(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"

        plan = await explain_plan_async(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        assert await collect_ages_async(stream) == [14, 15, 16, 17, 18]

    @requires_query_selection
    async def test_compound_predicate(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age > 30 and $.country == 'US'"

        plan = await explain_plan_async(pac, where)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
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

    @requires_query_selection
    async def test_reading_only_bins_projects_requested_bins(self, query_selection_cluster):
        where = "$.age >= 14 and $.age <= 18"
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
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

    @requires_server_compiled_ael
    @requires_query_selection
    async def test_contradiction_raises_filtered_out(self, query_selection_cluster):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
                .where("$.age > 100 and $.age < 10")
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    @requires_query_selection
    async def test_empty_secondary_index_result(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age == 999"
        plan = await pac.query_explain(NS, where, set_name=SET_NAME)
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        count = await count_records_async(stream)
        assert count == 0

    @requires_query_selection
    async def test_or_conjunct_with_an_indexable_sibling_returns_matching_records(
        self, query_selection_cluster,
    ):
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .where("$.age > 10 and ($.age < 14 or $.country == 'US')")
            .execute()
        )
        # The seed sets country US on even ages, so past 13 only those qualify.
        expected = [age for age in range(11, SIZE + 1) if age < 14 or age % 2 == 0]
        assert await collect_ages_async(stream) == expected

    @requires_query_selection
    async def test_integer_range_boundaries_are_inclusive_where_written(
        self, query_selection_cluster,
    ):
        session = query_selection_cluster.session

        async def ages(where):
            stream = await (
                session.query(namespace=NS, set_name=SET_NAME).where(where).execute()
            )
            return await collect_ages_async(stream)

        assert await ages("$.age >= 10 and $.age <= 12") == [10, 11, 12]
        assert await ages("$.age > 10 and $.age < 12") == [11]
        assert await ages("$.age >= 10 and $.age < 12") == [10, 11]
        assert await ages("$.age > 10 and $.age <= 12") == [11, 12]
        assert await ages("$.age >= 1 and $.age <= 1") == [1]
        assert await ages(f"$.age >= {SIZE}") == [SIZE]

    @requires_query_selection
    async def test_integer_range_with_no_representable_value_is_filtered_out(
        self, query_selection_cluster,
    ):
        """No integer lies strictly between 10 and 11."""
        with pytest.raises(AerospikeError) as excinfo:
            await (
                query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
                .where("$.age > 10 and $.age < 11")
                .execute()
            )
        assert excinfo.value.result_code == ResultCode.FILTERED_OUT


class TestQuerySelectionRouting:
    @requires_server_compiled_ael
    async def test_for_bin_hint_uses_legacy_execute_path(self, query_selection_cluster):
        """``bin_name`` skips explain but returns the same rows (Java ``forBin`` parity)."""
        where = "$.age >= 14 and $.age <= 18"
        default_stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute()
        )
        for_bin_stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(QueryHint(bin_name=BIN_AGE))
            .execute()
        )

        default_ages = await collect_ages_async(default_stream)
        for_bin_ages = await collect_ages_async(for_bin_stream)
        assert default_ages == for_bin_ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    async def test_for_index_hint_probes_and_executes(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(index_name=INDEX_NAME)

        plan = await explain_plan_async(pac, where, hint=hint)
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute()
        )
        ages = await collect_ages_async(stream)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    async def test_query_duration_only_hint_still_probes_and_executes(
        self, query_selection_cluster,
    ):
        pac = query_selection_cluster.client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        hint = QueryHint(query_duration=QueryDuration.SHORT)

        plan = await explain_plan_async(pac, where, hint=hint)
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(hint)
            .execute()
        )
        ages = await collect_ages_async(stream)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME
        assert ages == [14, 15, 16, 17, 18]

    @requires_query_selection
    async def test_where_exp_uses_non_probe_execute_path(self, query_selection_cluster):
        stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
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

    @requires_query_selection
    async def test_multiple_indexes_auto_select(self, query_selection_cluster):
        pac = query_selection_cluster.client.underlying_client
        age_where = "$.age >= 14 and $.age <= 18"
        score_where = "$.score >= 40 and $.score <= 44"

        age_plan = await explain_plan_async(pac, age_where)
        score_plan = await explain_plan_async(pac, score_where)

        age_stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
            .bins([BIN_AGE])
            .where(age_where)
            .execute()
        )
        score_stream = await (
            query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME)
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

    @requires_query_selection
    async def test_no_where_scan_returns_all_records(self, query_selection_cluster):
        stream = await query_selection_cluster.session.query(namespace=NS, set_name=SET_NAME).execute()
        count = await count_records_async(stream)
        assert count == SIZE

    @requires_query_selection
    async def test_parameterized_where_matches_the_literal_form(
        self, query_selection_cluster,
    ):
        """A bound ``where()`` takes the same selection path as literal text."""
        session = query_selection_cluster.session
        bound = await count_records_async(
            await session.query(namespace=NS, set_name=SET_NAME)
            .where("$.age >= %s and $.age <= %s", 14, 18)
            .execute()
        )
        literal = await count_records_async(
            await session.query(namespace=NS, set_name=SET_NAME)
            .where("$.age >= 14 and $.age <= 18")
            .execute()
        )
        assert bound == literal > 0


# ---------------------------------------------------------------------------
# The shared seed is a dense 1..SIZE run of ages, so it cannot show what a range
# does with gaps or with records that lack the bin entirely. This set supplies
# both, and owns its own index so the shared one is left alone.
# ---------------------------------------------------------------------------
SPARSE_SET = "qsel_sparse"
SPARSE_INDEX = "qsel_sparse_age_idx"
SPARSE_AGES = (2, 7, 23, 41)
MISSING_AGE_KEYS = ("noage1", "noage2")


@pytest.fixture
async def sparse_set(query_selection_cluster):
    session = query_selection_cluster.session
    client = query_selection_cluster.client
    ds = DataSet.of(NS, SPARSE_SET)

    for age in SPARSE_AGES:
        await session.upsert(ds.id(f"age{age}")).put({BIN_AGE: age}).execute()
    for name in MISSING_AGE_KEYS:
        await session.upsert(ds.id(name)).put({BIN_SCORE: 1}).execute()
    await create_index_quiet_async(
        client.underlying_client,
        set_name=SPARSE_SET,
        bin_name=BIN_AGE,
        index_name=SPARSE_INDEX,
        index_type=IndexType.NUMERIC,
    )
    yield session
    await drop_index_quiet_async(client, NS, SPARSE_SET, SPARSE_INDEX)
    for age in SPARSE_AGES:
        await session.delete(ds.id(f"age{age}")).execute()
    for name in MISSING_AGE_KEYS:
        await session.delete(ds.id(name)).execute()


class TestSparseRanges:
    """Ranges over values that are absent, and over records without the bin."""

    @requires_query_selection
    async def test_range_returns_only_the_ages_that_exist(self, sparse_set):
        stream = await (
            sparse_set.query(namespace=NS, set_name=SPARSE_SET)
            .where("$.age >= 5 and $.age <= 30")
            .execute()
        )
        assert await collect_ages_async(stream) == [7, 23]

    @requires_query_selection
    async def test_range_spanning_a_gap_returns_nothing(self, sparse_set):
        stream = await (
            sparse_set.query(namespace=NS, set_name=SPARSE_SET)
            .where("$.age >= 8 and $.age <= 22")
            .execute()
        )
        assert await collect_ages_async(stream) == []

    @requires_query_selection
    async def test_range_skips_records_without_the_bin(self, sparse_set):
        """The two bin-less records must not appear, and must not error."""
        stream = await (
            sparse_set.query(namespace=NS, set_name=SPARSE_SET)
            .where("$.age >= 1")
            .execute()
        )
        assert await collect_ages_async(stream) == list(SPARSE_AGES)
