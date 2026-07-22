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

"""Sync integration tests for CDT write options (unique, bounded, no_fail, partial)."""

import pytest

from aerospike_async import Key
from aerospike_sdk.exceptions import AerospikeError


NS = "test"
SET = "cdt_wopt_sync"


@pytest.fixture
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@pytest.fixture
def session(cluster):
    return cluster.create_session()


def _key(suffix: str) -> Key:
    return Key(NS, SET, f"cdt_wopt_sync_{suffix}")


class TestListUniqueFlag:

    def test_list_append_unique_rejects_duplicate(self, session):
        """list_append with unique=True rejects a duplicate value."""
        k = _key("uniq_append")
        session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        with pytest.raises(AerospikeError):
            (
                session.upsert(k)
                .bin("lst").list_append(2, unique=True)
                .execute()
            )

        rs = session.query(key=k).bin("lst").get().execute()
        result = rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2, 3]

    def test_list_append_unique_allows_new(self, session):
        """list_append with unique=True allows a new distinct value."""
        k = _key("uniq_new")
        session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        (
            session.upsert(k)
            .bin("lst").list_append(4, unique=True)
            .execute()
        )

        rs = session.query(key=k).bin("lst").get().execute()
        result = rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2, 3, 4]


class TestListCombinedFlags:

    def test_list_append_unique_no_fail_skips_duplicate(self, session):
        """unique+no_fail: duplicate append is skipped without error."""
        k = _key("uniq_nofail_append")
        session.upsert(k).put({"lst": [1, 2]}).execute()

        (
            session.upsert(k)
            .bin("lst").list_append(1, unique=True, no_fail=True)
            .execute()
        )

        rs = session.query(key=k).bin("lst").get().execute()
        result = rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2]


class TestMapNoFail:

    def test_map_insert_items_no_fail_partial(self, session):
        """map_insert_items with no_fail+partial inserts only new keys."""
        k = _key("map_insert_partial")
        session.upsert(k).put({"m": {"a": 1}}).execute()

        (
            session.upsert(k)
            .bin("m").map_insert_items(
                    {"a": 99, "b": 2},
                    no_fail=True, partial=True,
                )
            .execute()
        )

        rs = session.query(key=k).bin("m").get().execute()
        result = rs.first_or_raise()
        assert result.record.bins["m"]["a"] == 1
        assert result.record.bins["m"]["b"] == 2
