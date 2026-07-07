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

import time

import pytest
from aerospike_async import Filter, QuerySelection, ResultCode

from aerospike_sdk import DataSet, QueryHint, SyncClient
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    INDEX_NAME,
    NS,
    SCORE_INDEX_NAME,
    SET_NAME,
    SIZE,
    collect_ages_sync,
    count_records_sync,
    key_name,
)


def _sync_wait_for_index(client, ns, set_name, sindex_filter, *, timeout=5.0, interval=0.25):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            stream = client.query(ns, set_name).filter(sindex_filter).execute()
            for _ in stream:
                break
            stream.close()
            return
        except Exception as exc:
            if "IndexNotReadable" not in str(exc):
                raise
            last_err = exc
            time.sleep(interval)
    raise last_err  # type: ignore[misc]


@pytest.fixture(scope="module")
def qsel_client(
    aerospike_host,
    client_policy,
    supports_query_selection,
):
    if not supports_query_selection:
        pytest.skip("server does not support query selection (requires 8.1.3+)")

    with SyncClient(
        seeds=aerospike_host,
        policy=client_policy,
        index_refresh_interval=0.25,
    ) as client:
        pac = client.underlying_client
        if not pac.supports_query_selection():
            pytest.skip("cluster nodes do not support query selection")

        session = client.create_session()
        ds = DataSet.of(NS, SET_NAME)

        for i in range(1, SIZE + 1):
            try:
                session.delete(ds.id(key_name(i))).execute()
            except Exception:
                pass

        for i in range(1, SIZE + 1):
            country = "US" if i % 2 == 0 else "CA"
            session.upsert(ds.id(key_name(i))).put(
                {BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: country},
            ).execute()

        for index_name, bin_name in (
            (INDEX_NAME, BIN_AGE),
            (SCORE_INDEX_NAME, BIN_SCORE),
        ):
            try:
                client.index(NS, SET_NAME).on_bin(bin_name).named(
                    index_name,
                ).numeric().create()
            except Exception:
                pass

        _sync_wait_for_index(
            client, NS, SET_NAME, Filter.range(BIN_AGE, 1, SIZE),
        )
        _sync_wait_for_index(
            client, NS, SET_NAME, Filter.range(BIN_SCORE, 1, SIZE),
        )

        yield client

        for i in range(1, SIZE + 1):
            try:
                session.delete(ds.id(key_name(i))).execute()
            except Exception:
                pass
        for index_name in (INDEX_NAME, SCORE_INDEX_NAME):
            try:
                client.index(NS, SET_NAME).named(index_name).drop()
            except Exception:
                pass


class TestSyncQueryExplain:
    def test_range_selects_secondary_index(self, qsel_client):
        pac = qsel_client.underlying_client
        where = "$.age >= 14 and $.age <= 18"
        plan = pac.query_explain_blocking(NS, where, set_name=SET_NAME)

        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == INDEX_NAME

    def test_contradiction_filtered_out(self, qsel_client):
        pac = qsel_client.underlying_client
        plan = pac.query_explain_blocking(
            NS, "$.age > 100 and $.age < 10", set_name=SET_NAME,
        )
        assert plan.selection == QuerySelection.FILTERED_OUT


class TestSyncQueryExecute:
    def test_simple_range(self, qsel_client):
        stream = (
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where("$.age >= 14 and $.age <= 18")
            .execute()
        )
        assert collect_ages_sync(stream) == [14, 15, 16, 17, 18]

    def test_contradiction_raises_filtered_out(self, qsel_client):
        with pytest.raises(AerospikeError) as exc_info:
            qsel_client.query(NS, SET_NAME).where(
                "$.age > 100 and $.age < 10",
            ).execute()
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    def test_server_led_matches_legacy_for_bin(self, qsel_client):
        where = "$.age > 30 and $.country == 'US'"
        server_ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .execute(),
        )
        legacy_ages = collect_ages_sync(
            qsel_client.query(NS, SET_NAME)
            .bins([BIN_AGE])
            .where(where)
            .with_hint(QueryHint(bin_name=BIN_AGE))
            .execute(),
        )
        assert server_ages == legacy_ages

    def test_no_where_scan(self, qsel_client):
        count = count_records_sync(qsel_client.query(NS, SET_NAME).execute())
        assert count == SIZE
