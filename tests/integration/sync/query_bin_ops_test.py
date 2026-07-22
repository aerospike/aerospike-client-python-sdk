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

"""Sync integration tests for query bin-level read operations.

Coverage:
  - Simple bin reads (get, multiple bins)
  - Expression reads (select_from)
  - CDT map reads (key, index range, rank)
  - CDT list reads (index, rank)
  - Batch key queries with bin ops (map and list CDT reads)
  - CDT read edge cases (long range, missing record)
  - Inverted reads
  - Nested CDT read navigation
"""

import pytest
from aerospike_async import Key
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from aerospike_sdk import DataSet


KEY_PREFIX = "qbops_"
NS = "test"
SET = "query_bin_ops_sync"


@pytest.fixture
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        session = cluster.create_session()
        ds = DataSet.of(NS, SET)

        for i in range(1, 4):
            settings = {"theme": "dark", "volume": i * 10, "notifications": True}
            scores = [i * 10, i * 20, i * 30]
            nested = {
                "level1": {"a": i * 100, "b": i * 200},
                "level2": {"x": i, "y": i + 1},
            }
            session.upsert(ds.id(f"{KEY_PREFIX}{i}")).put({
                "name": f"user{i}",
                "age": 20 + i,
                "score": i * 100,
                "settings": settings,
                "scores": scores,
                "nested": nested,
            }).execute()

        yield cluster


def _key(i: int) -> Key:
    return Key(NS, SET, f"{KEY_PREFIX}{i}")


# ===================================================================
# Simple bin reads
# ===================================================================

class TestSimpleBinReads:

    def test_get_single_bin(self, cluster):
        session = cluster.create_session()
        result = session.query(_key(1)).bin("name").get().execute().first_or_raise()
        assert result.record.bins["name"] == "user1"

    def test_get_multiple_bins(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("name").get()
            .bin("age").get()
            .execute()
            .first_or_raise()
        )
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age"] == 21

    def test_map_size(self, cluster):
        session = cluster.create_session()
        result = session.query(_key(1)).bin("settings").map_size().execute().first_or_raise()
        assert result.record.bins["settings"] == 3

    def test_list_size(self, cluster):
        session = cluster.create_session()
        result = session.query(_key(1)).bin("scores").list_size().execute().first_or_raise()
        assert result.record.bins["scores"] == 3

    def test_list_get(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").list_get(0).execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 10

    def test_list_get_range(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").list_get_range(0, 2).execute().first_or_raise()
        )
        assert result.record.bins["scores"] == [10, 20]

    def test_list_get_range_from_index(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").list_get_range(1, None).execute().first_or_raise()
        )
        assert result.record.bins["scores"] == [20, 30]


# ===================================================================
# CDT map reads
# ===================================================================

class TestCdtMapReads:

    def test_map_key_get_values(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_key("theme").get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["settings"] == "dark"

    def test_map_key_count(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_key("theme").count()
            .execute().first_or_raise()
        )
        assert result.record.bins["settings"] == 1

    def test_map_index_range_get_values(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("settings").on_map_index_range(0, 2).get_values()
            .execute().first_or_raise()
        )
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    def test_map_rank_get_values(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(2)).bin("settings").on_map_rank(0).get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["settings"] is not None


# ===================================================================
# CDT list reads
# ===================================================================

class TestCdtListReads:

    def test_list_index_get_values(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").on_list_index(0).get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 10

    def test_list_index_count(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").on_list_index(0).count()
            .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 1

    def test_list_rank_get_values(self, cluster):
        """Rank 0 = lowest value; for key 2 scores=[20,40,60], lowest=20."""
        session = cluster.create_session()
        result = (
            session.query(_key(2)).bin("scores").on_list_rank(0).get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == 20

    def test_nested_list_get(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}nested_lg"
        key = ds.id(kid)
        try:
            session.delete(key).execute()
        except Exception:
            pass
        session.upsert(key).bin("ll").set_to([[10, 20], [30, 40]]).execute()
        result = (
            session.query(key).bin("ll").on_list_index(0).list_get(1).execute().first_or_raise()
        )
        assert result.record.bins["ll"] == 20
        session.delete(key).execute()


# ===================================================================
# Index-based list writes (mutating)
# ===================================================================

class TestIndexBasedListWrites:

    def test_list_insert_then_read(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}idx_mut"
        key = ds.id(kid)
        try:
            session.delete(key).execute()
        except Exception:
            pass
        session.upsert(key).bin("nums").set_to([1, 2, 3]).execute()
        session.upsert(key).bin("nums").list_insert(1, 9).execute()
        result = session.query(key).bin("nums").get().execute().first_or_raise()
        assert result.record.bins["nums"] == [1, 9, 2, 3]
        session.delete(key).execute()

    def test_list_increment_and_nested_insert(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}idx_mut2"
        key = ds.id(kid)
        try:
            session.delete(key).execute()
        except Exception:
            pass
        session.upsert(key).put({"outer": {"items": [10, 20]}}).execute()
        session.upsert(key).bin("outer").on_map_key("items").list_increment(0, 5).execute()
        session.upsert(key).bin("outer").on_map_key("items").list_insert(1, 0).execute()
        result = session.query(key).bin("outer").get().execute().first_or_raise()
        assert result.record.bins["outer"]["items"] == [15, 0, 20]
        session.delete(key).execute()


# ===================================================================
# Batch key queries
# ===================================================================

class TestBatchKeyQueries:

    def test_batch_bin_get(self, cluster):
        session = cluster.create_session()
        results = (
            session.query([_key(1), _key(2)]).bin("name").get()
            .execute().collect()
        )
        assert len(results) == 2
        names = {r.record.bins["name"] for r in results if r.is_ok}
        assert names == {"user1", "user2"}

    def test_batch_cdt_map_read(self, cluster):
        session = cluster.create_session()
        results = (
            session.query([_key(1), _key(2), _key(3)])
            .bin("settings").on_map_key("theme").get_values()
            .execute().collect()
        )
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["settings"] == "dark"

    def test_batch_cdt_list_size(self, cluster):
        session = cluster.create_session()
        results = (
            session.query([_key(1), _key(2), _key(3)])
            .bin("scores").list_size()
            .execute().collect()
        )
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["scores"] == 3

    def test_batch_cdt_list_get(self, cluster):
        session = cluster.create_session()
        results = (
            session.query([_key(1), _key(2), _key(3)])
            .bin("scores").list_get(0)
            .execute().collect()
        )
        assert len(results) == 3
        by_first = {r.record.bins["scores"] for r in results if r.is_ok}
        assert by_first == {10, 20, 30}


class TestCdtReadEdgeCases:
    """OOB-tolerant reads and missing-record error paths."""

    def test_list_get_range_past_end_returns_partial(self, cluster):
        """list_get_range with count past the end returns the full tail."""
        session = cluster.create_session()
        result = (
            session.query(_key(1)).bin("scores").list_get_range(0, 100)
            .execute().first_or_raise()
        )
        assert result.record.bins["scores"] == [10, 20, 30]

    def test_remove_from_nonexistent_key_raises(self, cluster):
        """Map remove_by_key_list on a missing record raises KEY_NOT_FOUND_ERROR."""
        session = cluster.create_session()
        ds = DataSet.of(NS, SET)
        key = ds.id(f"{KEY_PREFIX}missing_rm")
        try:
            session.delete(key).execute()
        except Exception:
            pass
        with pytest.raises(AerospikeError) as exc_info:
            session.update(key).bin("m").on_map_key_list(["a"]).remove().execute()
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


# ===================================================================
# Inverted reads
# ===================================================================

class TestInvertedReads:

    def test_map_key_range_get_all_other_values(self, cluster):
        """Get all map values EXCEPT those in the range."""
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("settings").on_map_key_range("theme", "volume").get_all_other_values()
            .execute().first_or_raise()
        )
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    def test_list_value_get_all_other_values(self, cluster):
        """Get all list elements EXCEPT those matching the value."""
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("scores").on_list_value(10).get_all_other_values()
            .execute().first_or_raise()
        )
        vals = result.record.bins["scores"]
        assert isinstance(vals, list)
        assert 10 not in vals
        assert 20 in vals
        assert 30 in vals


# ===================================================================
# Expression reads (select_from)
# ===================================================================

class TestExpressionReads:

    def test_select_from_simple(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("age_plus_20").select_from("$.age + 20")
            .execute().first_or_raise()
        )
        assert result.record.bins["age_plus_20"] == 41

    def test_select_from_multiple(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(2))
            .bin("double_age").select_from("$.age * 2")
            .bin("triple_score").select_from("$.score * 3")
            .execute().first_or_raise()
        )
        assert result.record.bins["double_age"] == 44   # (20+2)*2
        assert result.record.bins["triple_score"] == 600  # 200*3

    def test_select_from_with_get(self, cluster):
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("name").get()
            .bin("age_in_10").select_from("$.age + 10")
            .execute().first_or_raise()
        )
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age_in_10"] == 31


# ===================================================================
# Nested CDT read navigation
# ===================================================================

class TestNestedCdtReads:

    def test_nested_map_key_get_values(self, cluster):
        """Read a value 2 levels deep: nested.level1.a"""
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("nested").on_map_key("level1").on_map_key("a").get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 100

    def test_nested_map_key_count(self, cluster):
        """Count at a nested path should be 1 for a scalar."""
        session = cluster.create_session()
        result = (
            session.query(_key(1))
            .bin("nested").on_map_key("level1").on_map_key("b").count()
            .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 1

    def test_nested_map_key_different_branches(self, cluster):
        """Read from two different nested branches in separate queries."""
        session = cluster.create_session()
        r1 = (
            session.query(_key(2))
            .bin("nested").on_map_key("level1").on_map_key("a").get_values()
            .execute().first_or_raise()
        )
        assert r1.record.bins["nested"] == 200

        r2 = (
            session.query(_key(2))
            .bin("nested").on_map_key("level2").on_map_key("x").get_values()
            .execute().first_or_raise()
        )
        assert r2.record.bins["nested"] == 2

    def test_nested_map_key_with_flat_bin(self, cluster):
        """Combine a nested CDT read with a flat bin read."""
        session = cluster.create_session()
        result = (
            session.query(_key(3))
            .bin("nested").on_map_key("level1").on_map_key("a").get_values()
            .bin("name").get()
            .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 300
        assert result.record.bins["name"] == "user3"

    def test_nested_map_key_get_values_key3(self, cluster):
        """Read nested value for a different key to verify data independence."""
        session = cluster.create_session()
        result = (
            session.query(_key(3))
            .bin("nested").on_map_key("level2").on_map_key("y").get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["nested"] == 4
