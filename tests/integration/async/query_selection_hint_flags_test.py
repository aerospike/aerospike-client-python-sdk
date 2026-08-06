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

"""Tier D integration tests: ``REQUIRE_INDEX`` and ``HARD_HINT`` on field 44 explain.

Port of Java ``QuerySelectionHintFlagsTest``.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from aerospike_async.exceptions import IndexNotFound, InvalidRequest

from aerospike_sdk import DataSet, Filter, QueryHint, ResultCode

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    HINT_BOGUS_INDEX_NAME,
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    NS,
    QuerySelection,
    explain_plan_async,
    hint_key_name,
    requires_pac_query_selection_api,
    skip_unless_query_selection,
)

pytestmark = requires_pac_query_selection_api


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def qselhint_client(
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
        ds = DataSet.of(NS, HINT_SET_NAME)

        for suffix in ("1", "2"):
            try:
                await session.delete(ds.id(hint_key_name(suffix))).execute()
            except Exception:
                pass

        for index_name, bin_name in (
            (HINT_INDEX_NAME, BIN_AGE),
            (HINT_SCORE_INDEX_NAME, BIN_SCORE),
        ):
            try:
                await (
                    client.index(NS, HINT_SET_NAME)
                    .on_bin(bin_name)
                    .named(index_name)
                    .numeric()
                    .create()
                )
            except Exception:
                pass

        await (
            session.upsert(ds.id(hint_key_name("1")))
            .put({BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"})
            .execute()
        )
        await (
            session.upsert(ds.id(hint_key_name("2")))
            .put({BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"})
            .execute()
        )

        await wait_for_set_visible(session, NS, HINT_SET_NAME, 2)
        await wait_for_index(
            client, NS, HINT_SET_NAME, Filter.range(BIN_AGE, 25, 30),
        )
        await wait_for_index(
            client, NS, HINT_SET_NAME, Filter.range(BIN_SCORE, 25, 30),
        )

        yield client

        for suffix in ("1", "2"):
            try:
                await session.delete(ds.id(hint_key_name(suffix))).execute()
            except Exception:
                pass
        for index_name in (HINT_INDEX_NAME, HINT_SCORE_INDEX_NAME):
            try:
                await client.index(NS, HINT_SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestQuerySelectionHintFlags:
    async def test_require_index_on_primary_index_plan_fails_explain(
        self, qselhint_client,
    ):
        pac = qselhint_client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                pac,
                "$.country == 'US'",
                set_name=HINT_SET_NAME,
                hint=QueryHint(require_index=True),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_require_index_with_soft_hint_selects_secondary_index(
        self, qselhint_client,
    ):
        pac = qselhint_client.underlying_client
        plan = await explain_plan_async(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(require_index=True, index_name=HINT_SCORE_INDEX_NAME),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    async def test_hard_hint_with_matching_index_selects_hinted_index(
        self, qselhint_client,
    ):
        pac = qselhint_client.underlying_client
        plan = await explain_plan_async(
            pac,
            "$.age == 25",
            set_name=HINT_SET_NAME,
            hint=QueryHint(index_name=HINT_INDEX_NAME, hard_hint=True),
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == HINT_INDEX_NAME

    async def test_require_index_and_hard_hint_selects_hinted_index(
        self, qselhint_client,
    ):
        pac = qselhint_client.underlying_client
        plan = await explain_plan_async(
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

    async def test_hard_hint_with_wrong_index_fails_explain(self, qselhint_client):
        pac = qselhint_client.underlying_client
        with pytest.raises(IndexNotFound) as exc_info:
            await explain_plan_async(
                pac,
                "$.age == 25",
                set_name=HINT_SET_NAME,
                hint=QueryHint(
                    index_name=HINT_BOGUS_INDEX_NAME,
                    hard_hint=True,
                ),
            )
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND

    async def test_bad_ael_fails_explain_with_parameter(self, qselhint_client):
        pac = qselhint_client.underlying_client
        with pytest.raises(InvalidRequest) as exc_info:
            await explain_plan_async(
                pac, "$.age > 30 and", set_name=HINT_SET_NAME,
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
