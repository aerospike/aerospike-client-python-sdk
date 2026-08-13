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

"""Field ``44`` explain scope across index shapes (Java ``QuerySelectionExplainScopeTest``)."""

from __future__ import annotations

import pytest_asyncio
from aerospike_async import IndexType

from aerospike_sdk import CollectionIndexType, DataSet

from tests.integration.query_selection_helpers import (
    NS,
    QuerySelection,
    SCOPE_AGE_BIN,
    SCOPE_BLOB_BIN,
    SCOPE_BLOB_INDEX,
    SCOPE_COUNTRY_BIN,
    SCOPE_INT_INDEX,
    SCOPE_MAP_BIN,
    SCOPE_MAP_INDEX,
    SCOPE_MAP_KEY,
    SCOPE_SET_NAME,
    blob_hex_literal,
    count_records_async,
    create_index_quiet_async,
    explain_plan_async,
    long_bytes_be,
    requires_pac_query_selection_api,
    skip_unless_query_selection,
)

pytestmark = requires_pac_query_selection_api


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def qscexp_client(
    aerospike_host,
    make_cluster_definition,
    supports_query_selection,
    wait_for_set_visible,
):
    skip_unless_query_selection(supports_query_selection)

    blob_bytes = long_bytes_be(50001)

    cluster_def = make_cluster_definition(aerospike_host)
    async with await cluster_def.connect() as cluster:
        client = cluster._sdk_client
        pac = client.underlying_client
        session = cluster.create_session()
        ds = DataSet.of(NS, SCOPE_SET_NAME)

        for key_id in ("k1", "k2"):
            try:
                await session.delete(ds.id(key_id)).execute()
            except Exception:
                pass

        await create_index_quiet_async(
            pac,
            set_name=SCOPE_SET_NAME,
            bin_name=SCOPE_AGE_BIN,
            index_name=SCOPE_INT_INDEX,
            index_type=IndexType.NUMERIC,
        )
        await create_index_quiet_async(
            pac,
            set_name=SCOPE_SET_NAME,
            bin_name=SCOPE_BLOB_BIN,
            index_name=SCOPE_BLOB_INDEX,
            index_type=IndexType.BLOB,
        )
        await create_index_quiet_async(
            pac,
            set_name=SCOPE_SET_NAME,
            bin_name=SCOPE_MAP_BIN,
            index_name=SCOPE_MAP_INDEX,
            index_type=IndexType.STRING,
            collection_type=CollectionIndexType.MAP_KEYS,
        )

        await (
            session.upsert(ds.id("k1"))
            .put({
                SCOPE_AGE_BIN: 25,
                SCOPE_COUNTRY_BIN: "US",
                SCOPE_BLOB_BIN: blob_bytes,
                SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
            })
            .execute()
        )
        await (
            session.upsert(ds.id("k2"))
            .put({SCOPE_AGE_BIN: 30, SCOPE_COUNTRY_BIN: "CA"})
            .execute()
        )

        await wait_for_set_visible(session, NS, SCOPE_SET_NAME, 2)

        yield client, blob_bytes

        for key_id in ("k1", "k2"):
            try:
                await session.delete(ds.id(key_id)).execute()
            except Exception:
                pass
        for index_name in (SCOPE_INT_INDEX, SCOPE_BLOB_INDEX, SCOPE_MAP_INDEX):
            try:
                await client.index(NS, SCOPE_SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestQuerySelectionExplainScope:
    async def test_explain_scalar_integer_secondary_index_succeeds(
        self, qscexp_client,
    ):
        client, _ = qscexp_client
        pac = client.underlying_client
        plan = await explain_plan_async(
            pac, "$.age == 25", set_name=SCOPE_SET_NAME,
        )

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_INT_INDEX

    async def test_explain_scalar_string_primary_index_no_index_fields(
        self, qscexp_client,
    ):
        client, _ = qscexp_client
        pac = client.underlying_client
        plan = await explain_plan_async(
            pac, "$.country == 'US'", set_name=SCOPE_SET_NAME,
        )

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    async def test_explain_blob_equality_selects_secondary_index(
        self, qscexp_client,
    ):
        client, blob_bytes = qscexp_client
        pac = client.underlying_client
        where = f"$.{SCOPE_BLOB_BIN} == x'{blob_hex_literal(blob_bytes)}'"
        plan = await explain_plan_async(pac, where, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == SCOPE_BLOB_INDEX

    async def test_explain_map_keys_exists_primary_index_fallback(
        self, qscexp_client,
    ):
        client, _ = qscexp_client
        pac = client.underlying_client
        where = f"$.{SCOPE_MAP_BIN}.{SCOPE_MAP_KEY}.exists() == true"
        plan = await explain_plan_async(pac, where, set_name=SCOPE_SET_NAME)

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    async def test_execute_blob_equality_returns_matching_row(
        self, qscexp_client,
    ):
        client, blob_bytes = qscexp_client
        session = client.create_session()
        where = f"$.{SCOPE_BLOB_BIN} == x'{blob_hex_literal(blob_bytes)}'"

        stream = await (
            session.query(DataSet.of(NS, SCOPE_SET_NAME))
            .bins([SCOPE_BLOB_BIN])
            .where(where)
            .execute()
        )
        assert await count_records_async(stream) == 1

    async def test_execute_map_keys_exists_returns_matching_rows(
        self, qscexp_client,
    ):
        client, _ = qscexp_client
        session = client.create_session()
        where = f"$.{SCOPE_MAP_BIN}.{SCOPE_MAP_KEY}.exists() == true"

        stream = await (
            session.query(DataSet.of(NS, SCOPE_SET_NAME))
            .bins([SCOPE_MAP_BIN])
            .where(where)
            .execute()
        )
        count = 0
        try:
            async for result in stream:
                rec = result.record_or_raise()
                assert SCOPE_MAP_KEY in rec.bins[SCOPE_MAP_BIN]
                count += 1
        finally:
            stream.close()
        assert count > 0
