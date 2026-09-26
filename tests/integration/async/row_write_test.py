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

"""Tabular writes on a live cluster: ``session.upsert(dataset).bins(...).rows(...)``.

The rows must land as one batch with per-row outcomes, and the per-row TTL
and generation verbs must reach the server for that row alone.
"""

from __future__ import annotations

from datetime import timedelta

from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import ResultCode
from tests.integration.namespace import general_namespace

SET_NAME = "row_write_test"
PEOPLE = [(1, "Tim", 312), (2, "Bob", 25), (3, "Jane", 46)]


def _users() -> DataSet:
    return DataSet.of(general_namespace(), SET_NAME)


async def _bins_by_id(session, users: DataSet, ids):
    results = await (await session.query(users.ids(list(ids))).execute()).collect()
    return {r.key.value: r.record.bins for r in results if r.is_ok}


async def _clear(session, users: DataSet, ids):
    await session.delete(users.ids(list(ids))).execute()


class TestRowWrites:
    async def test_rows_land_as_one_batch(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, [1, 2, 3])

        stream = await session.upsert(users).bins("name", "age").rows(PEOPLE).execute()
        results = await stream.collect()
        assert [r.is_ok for r in results] == [True, True, True]

        assert await _bins_by_id(session, users, [1, 2, 3]) == {
            1: {"name": "Tim", "age": 312},
            2: {"name": "Bob", "age": 25},
            3: {"name": "Jane", "age": 46},
        }

    async def test_inline_rows_with_string_ids_and_mixed_values(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, ["alice", "bob"])

        await (
            session.upsert(users).bins("name", "email", "age", "active")
            .row("alice", "Alice", "alice@example.com", 30, True)
            .row("bob", "Bob", "bob@example.com", 25, False)
            .execute()
        )
        assert await _bins_by_id(session, users, ["alice", "bob"]) == {
            "alice": {"name": "Alice", "email": "alice@example.com", "age": 30, "active": True},
            "bob": {"name": "Bob", "email": "bob@example.com", "age": 25, "active": False},
        }

    async def test_insert_reports_an_existing_record_per_row(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, [10, 11])
        await session.upsert(users.id(10)).put({"name": "taken"}).execute()

        results = await (
            await session.insert(users).bins("name").row(10, "Tim").row(11, "Bob").execute()
        ).collect()
        by_id = {r.key.value: r for r in results}
        assert not by_id[10].is_ok
        assert by_id[10].result_code == ResultCode.KEY_EXISTS_ERROR
        assert by_id[11].is_ok
        assert (await _bins_by_id(session, users, [10]))[10] == {"name": "taken"}

    async def test_update_reports_a_missing_record_per_row(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, [20, 21])
        await session.upsert(users.id(20)).put({"name": "old"}).execute()

        results = await (
            await session.update(users).bins("name").row(20, "new").row(21, "ghost").execute()
        ).collect()
        by_id = {r.key.value: r for r in results}
        assert by_id[20].is_ok
        assert by_id[21].result_code == ResultCode.KEY_NOT_FOUND_ERROR
        assert (await _bins_by_id(session, users, [20]))[20] == {"name": "new"}

    async def test_per_row_ttl_reaches_only_that_row(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, [30, 31])

        await (
            session.upsert(users).bins("v")
            .row(30, 1)
            .row(31, 2).expire_record_after(timedelta(hours=1))
            .execute()
        )
        results = await (await session.query(users.ids(30, 31)).execute()).collect()
        ttl = {r.key.value: r.record.ttl for r in results}
        assert ttl[31] is not None and 3500 < ttl[31] <= 3600
        assert ttl[30] != ttl[31]

    async def test_per_row_generation_check_reaches_only_that_row(self, cluster):
        session = cluster.create_session()
        users = _users()
        await _clear(session, users, [40, 41])
        await session.upsert(users.ids(40, 41)).put({"v": 0}).execute()

        results = await (
            await session.update(users).bins("v")
            .row(40, 1).ensure_generation_is(99)
            .row(41, 1)
            .execute()
        ).collect()
        by_id = {r.key.value: r for r in results}
        assert by_id[40].result_code == ResultCode.GENERATION_ERROR
        assert by_id[41].is_ok
