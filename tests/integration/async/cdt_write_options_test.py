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

"""Async integration tests for CDT write options (unique, bounded, no_fail, partial)."""

import pytest

from aerospike_async import Key
from aerospike_sdk import Client
from aerospike_sdk.exceptions import AerospikeError


NS = "test"
SET = "cdt_wopt"


def _key(suffix: str) -> Key:
    return Key(NS, SET, f"cdt_wopt_{suffix}")


class TestListUniqueFlag:

    async def test_list_append_unique_rejects_duplicate(self, client):
        """list_append with unique=True rejects a duplicate value."""
        session = client.create_session()
        k = _key("uniq_append")
        await session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        with pytest.raises(AerospikeError):
            await (
                session.upsert(k)
                    .bin("lst").list_append(2, unique=True)
                    .execute()
            )

        rs = await client.query(key=k).bin("lst").get().execute()
        result = await rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2, 3]

    async def test_list_add_unique_rejects_duplicate(self, client):
        """list_add with unique=True rejects a duplicate value."""
        session = client.create_session()
        k = _key("uniq_add")
        await session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        with pytest.raises(AerospikeError):
            await (
                session.upsert(k)
                    .bin("lst").list_add(2, unique=True)
                    .execute()
            )

    async def test_list_append_unique_allows_new(self, client):
        """list_append with unique=True allows a new distinct value."""
        session = client.create_session()
        k = _key("uniq_new")
        await session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        await (
            session.upsert(k)
                .bin("lst").list_append(4, unique=True)
                .execute()
        )

        rs = await client.query(key=k).bin("lst").get().execute()
        result = await rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2, 3, 4]


class TestListCombinedFlags:

    async def test_list_append_unique_no_fail_skips_duplicate(self, client):
        """unique+no_fail: duplicate append is skipped without error."""
        session = client.create_session()
        k = _key("uniq_nofail_append")
        await session.upsert(k).put({"lst": [1, 2]}).execute()

        await (
            session.upsert(k)
                .bin("lst").list_append(1, unique=True, no_fail=True)
                .execute()
        )

        rs = await client.query(key=k).bin("lst").get().execute()
        result = await rs.first_or_raise()
        assert sorted(result.record.bins["lst"]) == [1, 2]


class TestListBoundedFlag:

    async def test_list_insert_bounded_rejects_out_of_bounds(self, client):
        """list_insert with bounded=True rejects an out-of-bounds index."""
        session = client.create_session()
        k = _key("bounded_insert")
        await session.upsert(k).put({"lst": [10, 20]}).execute()

        with pytest.raises(AerospikeError):
            await (
                session.upsert(k)
                    .bin("lst").list_insert(99, "oob", bounded=True)
                    .execute()
            )


class TestMapNoFail:

    async def test_map_upsert_items_no_fail(self, client):
        """map_upsert_items with no_fail=True succeeds."""
        session = client.create_session()
        k = _key("map_nofail")
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        await (
            session.upsert(k)
                .bin("m").map_upsert_items({"a": 2, "b": 3}, no_fail=True)
                .execute()
        )

        rs = await client.query(key=k).bin("m").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["m"]["a"] == 2
        assert result.record.bins["m"]["b"] == 3

    async def test_map_insert_items_no_fail_partial(self, client):
        """map_insert_items with no_fail+partial inserts only new keys."""
        session = client.create_session()
        k = _key("map_insert_partial")
        await session.upsert(k).put({"m": {"a": 1}}).execute()

        await (
            session.upsert(k)
                .bin("m").map_insert_items(
                    {"a": 99, "b": 2},
                    no_fail=True, partial=True,
                )
                .execute()
        )

        rs = await client.query(key=k).bin("m").get().execute()
        result = await rs.first_or_raise()
        assert result.record.bins["m"]["a"] == 1
        assert result.record.bins["m"]["b"] == 2
