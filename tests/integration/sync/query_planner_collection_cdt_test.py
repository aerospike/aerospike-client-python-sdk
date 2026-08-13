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

"""Sync MAPKEYS / LIST CDT planner tests (Java ``QueryPlannerCollectionCdtTest``)."""

from __future__ import annotations

import pytest
from aerospike_async import IndexType

from aerospike_sdk import CollectionIndexType, DataSet

from tests.integration.query_selection_helpers import (
    CDT_LIST_BIN,
    CDT_LIST_INDEX,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_SET_NAME,
    CDT_SIZE,
    NS,
    QuerySelection,
    cdt_key_name,
    create_index_quiet_blocking,
    explain_plan_blocking,
    long_bytes_be,
    requires_pac_query_selection_api,
    skip_unless_query_selection,
)

pytestmark = requires_pac_query_selection_api


@pytest.fixture(scope="module")
def qp_cdt_client(
    aerospike_host,
    make_cluster_definition,
    supports_query_selection,
):
    skip_unless_query_selection(supports_query_selection)

    cluster_def = make_cluster_definition(aerospike_host, sync=True)
    with cluster_def.connect() as cluster:
        client = cluster._sdk_client
        pac = client.underlying_client
        session = cluster.create_session()
        ds = DataSet.of(NS, CDT_SET_NAME)

        for i in range(1, CDT_SIZE + 1):
            try:
                session.delete(ds.id(cdt_key_name(i))).execute()
            except Exception:
                pass

        create_index_quiet_blocking(
            pac,
            set_name=CDT_SET_NAME,
            bin_name=CDT_MAP_BIN,
            index_name=CDT_MAP_INDEX,
            index_type=IndexType.STRING,
            collection_type=CollectionIndexType.MAP_KEYS,
        )
        create_index_quiet_blocking(
            pac,
            set_name=CDT_SET_NAME,
            bin_name=CDT_LIST_BIN,
            index_name=CDT_LIST_INDEX,
            index_type=IndexType.BLOB,
            collection_type=CollectionIndexType.LIST,
        )

        for i in range(1, CDT_SIZE + 1):
            map_data = {"mkey1": f"v{i}"}
            if i % 2 == 0:
                map_data[CDT_MAP_KEY] = f"v{i}"
            list_data = (
                [long_bytes_be(50003)]
                if i == 3
                else [long_bytes_be(50000 + i)]
            )
            session.upsert(ds.id(cdt_key_name(i))).put(
                {CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data},
            ).execute()

        yield client

        for i in range(1, CDT_SIZE + 1):
            try:
                session.delete(ds.id(cdt_key_name(i))).execute()
            except Exception:
                pass
        for index_name in (CDT_MAP_INDEX, CDT_LIST_INDEX):
            try:
                client.index(NS, CDT_SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestSyncQueryPlannerCollectionCdt:
    def test_plan_map_keys_exists_primary_index_fallback(self, qp_cdt_client):
        where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        plan = explain_plan_blocking(
            qp_cdt_client.underlying_client, where, set_name=CDT_SET_NAME,
        )
        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    def test_plan_list_exists_primary_index_fallback(self, qp_cdt_client):
        where = f"$.{CDT_LIST_BIN}.[0].exists() == true"
        plan = explain_plan_blocking(
            qp_cdt_client.underlying_client, where, set_name=CDT_SET_NAME,
        )
        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None

    def test_execute_cdt_exists_without_for_bin_returns_matching_rows(
        self, qp_cdt_client,
    ):
        session = qp_cdt_client.create_session()
        ds = DataSet.of(NS, CDT_SET_NAME)
        map_where = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists() == true"
        list_where = f"$.{CDT_LIST_BIN}.[0].exists() == true"

        map_stream = (
            session.query(ds).bins([CDT_MAP_BIN]).where(map_where).execute()
        )
        map_count = 0
        try:
            for result in map_stream:
                rec = result.record_or_raise()
                assert CDT_MAP_KEY in rec.bins[CDT_MAP_BIN]
                map_count += 1
        finally:
            map_stream.close()
        assert map_count == 10

        list_stream = (
            session.query(ds).bins([CDT_LIST_BIN]).where(list_where).execute()
        )
        list_count = 0
        try:
            for result in list_stream:
                rec = result.record_or_raise()
                assert len(rec.bins[CDT_LIST_BIN]) == 1
                list_count += 1
        finally:
            list_stream.close()
        assert list_count == CDT_SIZE
