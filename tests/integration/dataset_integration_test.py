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

"""Tests for DataSet integration through the Cluster entry point."""

from aerospike_sdk import DataSet
from tests.integration.namespace import general_namespace


async def test_key_value_with_dataset(aerospike_host, make_cluster_definition):
    """Test key_value operation using DataSet."""
    users = DataSet.of(general_namespace(), "users")

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        key = users.id("user1")
        # Put a record using DataSet
        await session.upsert(key).put({"name": "John", "age": 30}).execute()

        # Get the record using DataSet
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        assert record.bins["name"] == "John"
        assert record.bins["age"] == 30

        # Clean up
        await session.delete(key).execute()

async def test_key_value_with_key_object(aerospike_host, make_cluster_definition):
    """Test key_value operation using Key object."""
    users = DataSet.of(general_namespace(), "users")
    key = users.id("user2")

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        # Put a record using Key object
        await session.upsert(key).put({"name": "Jane", "age": 25}).execute()

        # Get the record using Key object
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        assert record.bins["name"] == "Jane"
        assert record.bins["age"] == 25

        # Clean up
        await session.delete(key).execute()

async def test_query_with_dataset(aerospike_host, make_cluster_definition):
    """Test query operation using DataSet."""
    users = DataSet.of(general_namespace(), "query_test")

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        # Put some test data
        await session.upsert(users.id("q1")).put({"id": "q1", "value": 10}).execute()
        await session.upsert(users.id("q2")).put({"id": "q2", "value": 20}).execute()

        # Query using DataSet
        stream = await session.query(dataset=users).execute()
        count = 0
        async for result in stream:
            record = result.record
            assert record is not None
            assert "id" in record.bins
            count += 1
            if count >= 2:
                break
        stream.close()

        # Clean up
        await session.delete(users.id("q1")).execute()
        await session.delete(users.id("q2")).execute()

async def test_query_with_single_key(aerospike_host, make_cluster_definition):
    """Test query operation using a single Key."""
    users = DataSet.of(general_namespace(), "users")
    key = users.id("user3")

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        # Put a record
        await session.upsert(key).put({"name": "Bob", "age": 35}).execute()

        # Query using single Key
        stream = await session.query(key=key).execute()
        count = 0
        async for result in stream:
            record = result.record
            assert record is not None
            assert record.bins["name"] == "Bob"
            count += 1
        stream.close()
        assert count == 1

        # Clean up
        await session.delete(key).execute()

async def test_query_with_multiple_keys(aerospike_host, make_cluster_definition):
    """Test query operation using multiple Keys."""
    users = DataSet.of(general_namespace(), "users")
    keys = users.ids("user4", "user5")

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        # Put records
        await session.upsert(keys[0]).put({"name": "Alice", "age": 28}).execute()
        await session.upsert(keys[1]).put({"name": "Charlie", "age": 32}).execute()

        # Query using multiple Keys (positional argument)
        stream = await session.query(keys).execute()
        count = 0
        names = []
        async for result in stream:
            record = result.record
            assert record is not None
            names.append(record.bins["name"])
            count += 1
        stream.close()
        assert count == 2
        assert "Alice" in names
        assert "Charlie" in names

        # Clean up
        await session.delete(keys[0]).execute()
        await session.delete(keys[1]).execute()
