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

"""Tests for append and prepend operations."""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet


@pytest.fixture
def test_set():
    """DataSet fixture for append tests."""
    return DataSet.of("test", "append_test")


class TestAppend:
    """Test string append operations."""

    async def test_append(self, client: Client, test_set: DataSet):
        """Test appending strings to a bin."""
        session = client.create_session()
        key = test_set.id("append")
        bin_name = "appendbin"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).append("Hello").execute()
        await session.upsert(key).bin(bin_name).append(" World").execute()

        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.is_ok
        assert first.record_or_raise().bins[bin_name] == "Hello World"

        await session.upsert(key).bin(bin_name).append("!").execute()
        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.record_or_raise().bins[bin_name] == "Hello World!"

        await session.delete(key).execute()

    async def test_prepend(self, client: Client, test_set: DataSet):
        """Test prepending strings to a bin."""
        session = client.create_session()
        key = test_set.id("prepend")
        bin_name = "prependbin"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).prepend("!").execute()
        await session.upsert(key).bin(bin_name).prepend("World").execute()

        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.is_ok
        assert first.record_or_raise().bins[bin_name] == "World!"

        await session.upsert(key).bin(bin_name).prepend("Hello ").execute()
        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.record_or_raise().bins[bin_name] == "Hello World!"

        await session.delete(key).execute()

    async def test_append_to_multiple_keys(self, client: Client, test_set: DataSet):
        """Test appending to multiple keys."""
        session = client.create_session()
        bin_name = "appendbin"

        key1 = test_set.id("append_multi_1")
        key2 = test_set.id("append_multi_2")

        try:
            await session.delete(key1).execute()
        except Exception:
            pass
        try:
            await session.delete(key2).execute()
        except Exception:
            pass

        await session.upsert(key1).bin(bin_name).append("First").execute()
        await session.upsert(key2).bin(bin_name).append("Second").execute()

        await session.upsert(key1).bin(bin_name).append("_1").execute()
        await session.upsert(key2).bin(bin_name).append("_2").execute()

        result1 = await session.query(key1).execute()
        result2 = await session.query(key2).execute()
        first1 = await result1.first_or_raise()
        first2 = await result2.first_or_raise()

        assert first1.record_or_raise().bins[bin_name] == "First_1"
        assert first2.record_or_raise().bins[bin_name] == "Second_2"

        await session.delete(key1).execute()
        await session.delete(key2).execute()
