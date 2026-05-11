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

"""Tests for replace and replace_if_exists operations."""

import pytest
from aerospike_async.exceptions import ResultCode
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError


@pytest.fixture
def users():
    """DataSet fixture for replace tests."""
    return DataSet.of("test", "replace_test")


class TestReplaceOperations:
    """Test replace and replace_if_exists operations."""

    async def test_replace(self, client: Client, users: DataSet):
        """replace() completely replaces an existing record's bins."""
        session = client.create_session()
        key = users.id("replace_key")

        await session.upsert(key).put({"bin1": "value1", "bin2": "value2"}).execute()

        await session.replace(key).put({"bin3": "value3"}).execute()

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins.get("bin1") is None
        assert record.record.bins.get("bin2") is None
        assert record.record.bins["bin3"] == "value3"

    async def test_replace_only(self, client: Client, users: DataSet):
        """replace_only() on a non-existent key should fail with KEY_NOT_FOUND_ERROR."""
        session = client.create_session()
        key = users.id("replace_only_key")

        await session.delete(key).execute()

        with pytest.raises(AerospikeError) as exc_info:
            stream = await (
                session.upsert(key)
                    .replace_only()
                    .put({"bin": "value"})
                    .execute()
            )
            await stream.first_or_raise()

        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_replace_only_modifies_op_type(self, client: Client, users: DataSet):
        """replace_only() dynamically changes upsert to replace-if-exists semantics."""
        session = client.create_session()
        key = users.id("replace_only_modifies_key")

        await session.upsert(key).put({"bin1": "value1", "bin2": "value2"}).execute()

        await (
            session.upsert(key)
                .replace_only()
                .put({"bin3": "value3"})
                .execute()
        )

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins.get("bin1") is None
        assert record.record.bins.get("bin2") is None
        assert record.record.bins["bin3"] == "value3"

    async def test_chained_operations_with_different_op_types(
        self, client: Client, users: DataSet,
    ):
        """Per-operation errors are isolated when chaining different op types."""
        session = client.create_session()

        key1 = users.id("chain_key1")
        key2 = users.id("chain_key2")
        key3 = users.id("chain_key3")

        await session.upsert(key1).put({"value": "original1"}).execute()
        await session.upsert(key2).put({"value": "original2"}).execute()
        await session.delete(key3).execute()

        await (
            session.update(key1)
                .set_to("value", "updated1")
            .insert(key2)
                .set_to("newbin", "inserted2")
            .replace(key3)
                .set_to("value", "replaced3")
            .execute()
        )

        rec1 = await (await session.query(key1).execute()).first_or_raise()
        assert rec1.record.bins["value"] == "updated1"

        rec2 = await (await session.query(key2).execute()).first_or_raise()
        assert rec2.record.bins["value"] == "original2"
        assert rec2.record.bins.get("newbin") is None

        rec3 = await (await session.query(key3).execute()).first_or_raise()
        assert rec3.record.bins["value"] == "replaced3"

    async def test_replace_creates_new_record(self, client: Client, users: DataSet):
        """replace() creates a new record if it doesn't exist."""
        session = client.create_session()
        key = users.id("replace_new_record")

        await session.delete(key).execute()

        await session.replace(key).put({"name": "New User", "status": "active"}).execute()

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins["name"] == "New User"
        assert record.record.bins["status"] == "active"

    async def test_replace_if_exists_fails_on_missing_record(self, client: Client, users: DataSet):
        """replace_if_exists() fails if record doesn't exist."""
        session = client.create_session()
        key = users.id("replace_if_exists_missing")

        await session.delete(key).execute()

        with pytest.raises(AerospikeError):
            stream = await (
                session.replace_if_exists(key)
                    .put({"name": "Should Fail"})
                    .execute()
            )
            await stream.first_or_raise()

    async def test_replace_if_exists_replaces_existing_record(self, client: Client, users: DataSet):
        """replace_if_exists() replaces an existing record."""
        session = client.create_session()
        key = users.id("replace_if_exists_existing")

        await session.upsert(key).put({
            "name": "Original",
            "extra": "should be deleted"
        }).execute()

        await session.replace_if_exists(key).put({"name": "Replaced", "status": "updated"}).execute()

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins["name"] == "Replaced"
        assert record.record.bins["status"] == "updated"
        assert "extra" not in record.record.bins

    async def test_batch_replace_if_exists(self, client: Client, users: DataSet):
        """replace_if_exists works in batch operations."""
        session = client.create_session()

        key1 = users.id("batch_replace_exists_1")
        key2 = users.id("batch_replace_exists_2")

        await session.upsert(key1).put({"value": "original1"}).execute()
        await session.upsert(key2).put({"value": "original2"}).execute()

        await (
            session.batch()
                .replace_if_exists(key1).bin("value").set_to("replaced1")
                .replace_if_exists(key2).bin("value").set_to("replaced2")
                .execute()
        )

        record1 = await (await session.query(key1).execute()).first_or_raise()
        assert record1.record.bins["value"] == "replaced1"

        record2 = await (await session.query(key2).execute()).first_or_raise()
        assert record2.record.bins["value"] == "replaced2"
