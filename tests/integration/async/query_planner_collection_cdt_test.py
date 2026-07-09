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

"""MAPKEYS / LIST collection CDT ``.exists()`` planner tests (Java ``QueryPlannerCollectionCdtTest``)."""

from __future__ import annotations

import pytest
import pytest_asyncio
from aerospike_async import CollectionIndexType, IndexType, QuerySelection

from aerospike_sdk import Client, DataSet

from tests.integration.query_selection_helpers import (
    CDT_LIST_BIN,
    CDT_LIST_INDEX,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_SET_NAME,
    CDT_SIZE,
    NS,
    cdt_key_name,
    create_index_quiet_async,
    explain_plan_async,
    long_bytes_be,
)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def qp_cdt_client(
    aerospike_host,
    client_policy,
    supports_query_selection,
    wait_for_set_visible,
):
    if not supports_query_selection:
        pytest.skip("cluster does not support query selection (PAC)")

    list_blob_bytes = long_bytes_be(50003)

    async with Client(
        seeds=aerospike_host,
        policy=client_policy,
        index_refresh_interval=0.25,
    ) as client:
        pac = client.underlying_client
        session = client.create_session()
        ds = DataSet.of(NS, CDT_SET_NAME)

        for i in range(1, CDT_SIZE + 1):
            try:
                await session.delete(ds.id(cdt_key_name(i))).execute()
            except Exception:
                pass

        await create_index_quiet_async(
            pac,
            set_name=CDT_SET_NAME,
            bin_name=CDT_MAP_BIN,
            index_name=CDT_MAP_INDEX,
            index_type=IndexType.STRING,
            collection_type=CollectionIndexType.MAP_KEYS,
        )
        await create_index_quiet_async(
            pac,
            set_name=CDT_SET_NAME,
            bin_name=CDT_LIST_BIN,
            index_name=CDT_LIST_INDEX,
            index_type="BLOB",
            collection_type=CollectionIndexType.LIST,
        )

        for i in range(1, CDT_SIZE + 1):
            map_data = {"mkey1": f"v{i}"}
            if i % 2 == 0:
                map_data[CDT_MAP_KEY] = f"v{i}"

            if i == 3:
                list_data = [list_blob_bytes]
            else:
                list_data = [long_bytes_be(50000 + i)]

            await (
                session.upsert(ds.id(cdt_key_name(i)))
                .put({CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data})
                .execute()
            )

        await wait_for_set_visible(session, NS, CDT_SET_NAME, CDT_SIZE)

        yield client

        for i in range(1, CDT_SIZE + 1):
            try:
                await session.delete(ds.id(cdt_key_name(i))).execute()
            except Exception:
                pass
        for index_name in (CDT_MAP_INDEX, CDT_LIST_INDEX):
            try:
                await client.index(NS, CDT_SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestQueryPlannerCollectionCdt:
    async def test_plan_map_keys_exists_primary_index_fallback(
        self, qp_cdt_client,
    ):
        pac = qp_cdt_client.underlying_client
        where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        plan = await explain_plan_async(pac, where, set_name=CDT_SET_NAME)

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    async def test_plan_list_exists_primary_index_fallback(self, qp_cdt_client):
        pac = qp_cdt_client.underlying_client
        where = f"$.{CDT_LIST_BIN}.[0].exists() == true"
        plan = await explain_plan_async(pac, where, set_name=CDT_SET_NAME)

        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    async def test_execute_cdt_exists_without_for_bin_returns_matching_rows(
        self, qp_cdt_client,
    ):
        session = qp_cdt_client.create_session()
        ds = DataSet.of(NS, CDT_SET_NAME)
        map_where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        list_where = f"$.{CDT_LIST_BIN}.[0].exists() == true"

        map_stream = await (
            session.query(ds)
            .bins([CDT_MAP_BIN])
            .where(map_where)
            .execute()
        )
        map_count = 0
        try:
            async for result in map_stream:
                rec = result.record_or_raise()
                assert CDT_MAP_KEY in rec.bins[CDT_MAP_BIN]
                map_count += 1
        finally:
            map_stream.close()
        assert map_count == 10

        list_stream = await (
            session.query(ds)
            .bins([CDT_LIST_BIN])
            .where(list_where)
            .execute()
        )
        list_count = 0
        try:
            async for result in list_stream:
                rec = result.record_or_raise()
                assert len(rec.bins[CDT_LIST_BIN]) == 1
                list_count += 1
        finally:
            list_stream.close()
        assert list_count == CDT_SIZE
