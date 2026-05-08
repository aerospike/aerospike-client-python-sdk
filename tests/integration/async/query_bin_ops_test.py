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

"""Integration tests for query bin-level read operations and stacking.

Coverage:
  - Simple bin reads (get, multiple bins)
  - CDT map reads (key, index range, rank)
  - CDT list reads (index, rank)
  - Batch key queries with bin ops (map and list CDT reads)
  - CDT read edge cases (long range, missing record)
  - Dataset query with bin ops -> OP_NOT_APPLICABLE
  - Query stacking
"""

import asyncio

import pytest
import pytest_asyncio

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode
from aerospike_sdk import DataSet, Client
from aerospike_sdk.exceptions import AerospikeError


KEY_PREFIX = "qbops_"
NS = "test"
SET = "query_bin_ops"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    """Setup SDK client, seed test data, yield the client."""
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of(NS, SET)

        for i in range(1, 4):
            settings = {"theme": "dark", "volume": i * 10, "notifications": True}
            scores = [i * 10, i * 20, i * 30]
            nested = {
                "level1": {"a": i * 100, "b": i * 200},
                "level2": {"x": i, "y": i + 1},
            }
            await session.upsert(ds.id(f"{KEY_PREFIX}{i}")).put({
                "name": f"user{i}",
                "age": 20 + i,
                "score": i * 100,
                "settings": settings,
                "scores": scores,
                "nested": nested,
            }).execute()

        # Brief pause so the query scan index reflects the committed writes under CI load
        await asyncio.sleep(0.1)

        yield client


def _key(i: int) -> Key:
    return Key(NS, SET, f"{KEY_PREFIX}{i}")


# ===================================================================
# Simple bin reads
# ===================================================================

class TestSimpleBinReads:

    async def test_get_single_bin(self, client):
        rs = await client.query(key=_key(1)).bin("name").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["name"] == "user1"

    async def test_get_multiple_bins(self, client):
        rs = await (
            client.query(key=_key(1))
                .bin("name").get()
                .bin("age").get()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age"] == 21

    async def test_map_size(self, client):
        rs = await client.query(key=_key(1)).bin("settings").map_size().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == 3

    async def test_list_size(self, client):
        rs = await client.query(key=_key(1)).bin("scores").list_size().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 3

    async def test_list_get(self, client):
        rs = await client.query(key=_key(1)).bin("scores").list_get(0).execute()
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 10

    async def test_list_get_range(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").list_get_range(0, 2).execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == [10, 20]

    async def test_list_get_range_from_index(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").list_get_range(1, None).execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == [20, 30]


# ===================================================================
# CDT map reads
# ===================================================================

class TestCdtMapReads:

    async def test_map_key_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_key("theme").get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == "dark"

    async def test_map_key_count(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_key("theme").count()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] == 1

    async def test_map_index_range_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("settings").on_map_index_range(0, 2).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        assert len(vals) == 2

    async def test_map_rank_get_values(self, client):
        rs = await (
            client.query(key=_key(2)).bin("settings").on_map_rank(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["settings"] is not None


# ===================================================================
# CDT list reads
# ===================================================================

class TestCdtListReads:

    async def test_list_index_get_values(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").on_list_index(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 10

    async def test_list_index_count(self, client):
        rs = await (
            client.query(key=_key(1)).bin("scores").on_list_index(0).count()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 1

    async def test_list_rank_get_values(self, client):
        """Rank 0 = lowest value; for key 2 scores=[20,40,60], lowest=20."""
        rs = await (
            client.query(key=_key(2)).bin("scores").on_list_rank(0).get_values()
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == 20

    async def test_nested_list_get(self, client):
        session = client.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}nested_lg"
        key = ds.id(kid)
        try:
            await session.delete(key).execute()
        except Exception:
            pass
        await session.upsert(key).bin("ll").set_to([[10, 20], [30, 40]]).execute()
        rs = await (
            client.query(key=key).bin("ll").on_list_index(0).list_get(1).execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["ll"] == 20
        await session.delete(key).execute()


# ===================================================================
# Index-based list writes (mutating)
# ===================================================================

class TestIndexBasedListWrites:

    async def test_list_insert_then_read(self, client):
        session = client.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}idx_mut"
        key = ds.id(kid)
        try:
            await session.delete(key).execute()
        except Exception:
            pass
        await session.upsert(key).bin("nums").set_to([1, 2, 3]).execute()
        await session.upsert(key).bin("nums").list_insert(1, 9).execute()
        rs = await client.query(key=key).bin("nums").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["nums"] == [1, 9, 2, 3]
        await session.delete(key).execute()

    async def test_list_increment_and_nested_insert(self, client):
        session = client.create_session()
        ds = DataSet.of(NS, SET)
        kid = f"{KEY_PREFIX}idx_mut2"
        key = ds.id(kid)
        try:
            await session.delete(key).execute()
        except Exception:
            pass
        await session.upsert(key).put({"outer": {"items": [10, 20]}}).execute()
        await (
            session.upsert(key).bin("outer").on_map_key("items").list_increment(0, 5).execute()
        )
        await (
            session.upsert(key).bin("outer").on_map_key("items").list_insert(1, 0).execute()
        )
        rs = await client.query(key=key).bin("outer").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["outer"]["items"] == [15, 0, 20]
        await session.delete(key).execute()


# ===================================================================
# Batch key queries
# ===================================================================

class TestBatchKeyQueries:

    async def test_batch_bin_get(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2)]).bin("name").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        names = {r.record.bins["name"] for r in results if r.is_ok}
        assert names == {"user1", "user2"}

    async def test_batch_cdt_map_read(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2), _key(3)])
                .bin("settings").on_map_key("theme").get_values()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["settings"] == "dark"

    async def test_batch_cdt_list_size(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2), _key(3)])
                .bin("scores").list_size()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3
        for r in results:
            assert r.is_ok
            assert r.record.bins["scores"] == 3

    async def test_batch_cdt_list_get(self, client):
        rs = await (
            client.query(keys=[_key(1), _key(2), _key(3)])
                .bin("scores").list_get(0)
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3
        by_first = {r.record.bins["scores"] for r in results if r.is_ok}
        assert by_first == {10, 20, 30}


class TestCdtReadEdgeCases:
    """OOB-tolerant reads and missing-record error paths."""

    async def test_list_get_range_past_end_returns_partial(self, client):
        """list_get_range with count past the end returns the full tail."""
        rs = await (
            client.query(key=_key(1)).bin("scores").list_get_range(0, 100).execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30]

    async def test_remove_from_nonexistent_key_raises(self, client):
        """Map remove_by_key_list on a missing record raises KEY_NOT_FOUND_ERROR."""
        session = client.create_session()
        ds = DataSet.of(NS, SET)
        key = ds.id(f"{KEY_PREFIX}missing_rm")
        try:
            await session.delete(key).execute()
        except Exception:
            pass
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.update(key).bin("m").on_map_key_list(["a"]).remove().execute()
            )
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


# ===================================================================
# Dataset query with bin ops -> OP_NOT_APPLICABLE
# ===================================================================

class TestDatasetQueryGuard:

    async def test_dataset_query_with_bin_ops_raises(self, client):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                client.query(NS, SET).bin("settings").on_map_key("theme").get_values()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE

    async def test_dataset_query_with_get_raises(self, client):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                client.query(NS, SET).bin("name").get()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


# ===================================================================
# Query stacking
# ===================================================================

class TestQueryStacking:

    async def test_stack_two_point_queries(self, client):
        rs = await (
            client
                .query(key=_key(1)).bin("name").get()
                .query(_key(2)).bin("age").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        bins_by_key = {}
        for r in results:
            assert r.is_ok
            digest = r.key.digest
            bins_by_key[digest] = r.record.bins
        digests = list(bins_by_key.keys())
        assert len(digests) == 2

    async def test_stack_batch_queries(self, client):
        rs = await (
            client
                .query(keys=[_key(1), _key(2)]).bin("name").get()
                .query([_key(3)]).bin("age").get()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3

    async def test_dataset_query_cannot_stack(self, client):
        with pytest.raises(ValueError, match="cannot be stacked"):
            client.query(NS, SET).query(_key(1))

    async def test_stacked_read_complex(self, client):
        """Stacked query mixing specific bins, all bins, no bins,
        select_from, missing bin, and missing key."""
        rs = await (
            client
                .query(key=_key(1)).bins(["name"])
                .query(_key(2))
                .query(_key(3)).with_no_bins()
                .query(_key(1)).bin("score").select_from("$.score * 8")
                .query(_key(2)).bins(["binnotfound"])
                .query(Key(NS, SET, f"{KEY_PREFIX}999")).bins(["name"])
                .execute()
        )
        results = await rs.collect()
        # Missing key (key999) is excluded from stream by default
        assert len(results) == 5

        # key1 — specific bin
        r = results[0]
        assert r.is_ok
        assert r.record.bins["name"] == "user1"
        assert "age" not in r.record.bins

        # key2 — all bins
        r = results[1]
        assert r.is_ok
        assert "name" in r.record.bins
        assert "age" in r.record.bins

        # key3 — no bins (header only)
        r = results[2]
        assert r.is_ok
        assert r.record.bins == {}

        # key1 — select_from expression
        r = results[3]
        assert r.is_ok
        assert r.record.bins["score"] == 800

        # key2 — missing bin returns empty
        r = results[4]
        assert r.is_ok
        assert "binnotfound" not in r.record.bins


# ===================================================================
# Inverted reads
# ===================================================================

class TestInvertedReads:

    async def test_map_key_range_get_all_other_values(self, client):
        """Get all map values EXCEPT those in the range."""
        rs = await (
            client.query(key=_key(1))
                .bin("settings").on_map_key_range("theme", "volume").get_all_other_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["settings"]
        assert isinstance(vals, list)
        # Key range ["theme", "volume") is exclusive on upper bound, so
        # "theme" is in the range but "volume" is NOT.  Inverted returns
        # values for "notifications" and "volume".
        assert len(vals) == 2

    async def test_list_value_get_all_other_values(self, client):
        """Get all list elements EXCEPT those matching the value."""
        rs = await (
            client.query(key=_key(1))
                .bin("scores").on_list_value(10).get_all_other_values()
                .execute()
        )
        result = await rs.first_or_raise()
        vals = result.record.bins["scores"]
        assert isinstance(vals, list)
        assert 10 not in vals
        assert 20 in vals
        assert 30 in vals


# ===================================================================
# Expression reads (select_from)
# ===================================================================

class TestExpressionReads:

    async def test_select_from_simple(self, client):
        rs = await (
            client.query(key=_key(1))
                .bin("age_plus_20").select_from("$.age + 20")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["age_plus_20"] == 41

    async def test_select_from_multiple(self, client):
        rs = await (
            client.query(key=_key(2))
                .bin("double_age").select_from("$.age * 2")
                .bin("triple_score").select_from("$.score * 3")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["double_age"] == 44   # (20+2)*2
        assert result.record.bins["triple_score"] == 600  # 200*3

    async def test_select_from_with_get(self, client):
        rs = await (
            client.query(key=_key(1))
                .bin("name").get()
                .bin("age_in_10").select_from("$.age + 10")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["name"] == "user1"
        assert result.record.bins["age_in_10"] == 31


# ===================================================================
# Nested CDT read navigation
# ===================================================================

class TestNestedCdtReads:

    async def test_nested_map_key_get_values(self, client):
        """Read a value 2 levels deep: nested.level1.a"""
        session = client.create_session()
        rs = await (
            await session.query(_key(1))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["nested"] == 100

    async def test_nested_map_key_count(self, client):
        """Count at a nested path should be 1 for a scalar."""
        session = client.create_session()
        rs = await (
            await session.query(_key(1))
                .bin("nested").on_map_key("level1").on_map_key("b").count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["nested"] == 1

    async def test_nested_map_key_different_branches(self, client):
        """Read from two different nested branches in separate queries."""
        session = client.create_session()
        rs1 = await (
            await session.query(_key(2))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .execute()
        ).first_or_raise()
        assert rs1.record.bins["nested"] == 200

        rs2 = await (
            await session.query(_key(2))
                .bin("nested").on_map_key("level2").on_map_key("x").get_values()
                .execute()
        ).first_or_raise()
        assert rs2.record.bins["nested"] == 2

    async def test_nested_map_key_with_flat_bin(self, client):
        """Combine a nested CDT read with a flat bin read."""
        session = client.create_session()
        rs = await (
            await session.query(_key(3))
                .bin("nested").on_map_key("level1").on_map_key("a").get_values()
                .bin("name").get()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["nested"] == 300
        assert rs.record.bins["name"] == "user3"

    async def test_nested_map_key_get_values_key3(self, client):
        """Read nested value for a different key to verify data independence."""
        session = client.create_session()
        rs = await (
            await session.query(_key(3))
                .bin("nested").on_map_key("level2").on_map_key("y").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["nested"] == 4
