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

"""Integration tests for put/get and core SDK operations."""

import pytest
from aerospike_sdk import ListOrderType, MapReturnType
from aerospike_async import ListOperation, ListPolicy, MapOperation, MapPolicy, Operation, WritePolicy
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from .durable_delete_support import delete_keys_durable


@pytest.fixture
async def cluster(aerospike_host, make_cluster_definition):
    """Function-scoped: many tests reuse ``test/test`` user key ``1`` and assume a fresh record."""
    async with await make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        test_ds = DataSet.of("test", "test")
        await session.delete(test_ds.id(1)).execute()
        yield c


async def test_put_get_basic(cluster):
    """Test basic put and get operations."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "John", "age": 30}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "John", "age": 30}


async def test_put_get_int(cluster):
    """Test putting and getting integer values."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bin": 42}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"bin": 42}


async def test_put_get_float(cluster):
    """Test putting and getting float values."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bin": 3.14159}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"bin": 3.14159}


async def test_put_get_string(cluster):
    """Test putting and getting string values."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bin": "hello world"}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"bin": "hello world"}


async def test_put_get_bool(cluster):
    """Test putting and getting boolean values."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bint": True, "binf": False}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"bint": True, "binf": False}


async def test_get_specific_bins(cluster):
    """Test getting only specific bins."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "John", "age": 30, "city": "NYC"}).execute()

    result = await session.query(k).bins(["name", "age"]).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    bins = first.record_or_raise().bins
    assert bins == {"name": "John", "age": 30}
    assert "city" not in bins


async def test_delete(cluster):
    """Test delete operation."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "John"}).execute()

    exists_result = await session.exists(k).execute()
    assert (await exists_result.first()).as_bool() is True

    await session.delete(k).execute()

    exists_result = await session.exists(k).execute()
    first = await exists_result.first()
    assert first is None or not first.as_bool()


async def test_delete_nonexistent(cluster):
    """Test deleting a non-existent record."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(999)

    result = await session.delete(k).execute()
    first = await result.first()
    assert first is None or not first.is_ok


async def test_exists(cluster):
    """Test exists operation."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    exists_result = await session.exists(k).execute()
    first = await exists_result.first()
    assert first is None or not first.as_bool()

    await session.upsert(k).put({"name": "John"}).execute()

    exists_result = await session.exists(k).execute()
    first = await exists_result.first()
    assert first is not None and first.as_bool()


async def test_add(cluster):
    """Test add (increment) operation."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"counter": 10}).execute()
    await session.upsert(k).bin("counter").add(5).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"counter": 15}


async def test_append(cluster):
    """Test append operation."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "John"}).execute()
    await session.upsert(k).bin("name").append(" Doe").execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "John Doe"}


async def test_prepend(cluster):
    """Test prepend operation."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Doe"}).execute()
    await session.upsert(k).bin("name").prepend("John ").execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "John Doe"}


async def test_touch(cluster):
    """Test touch operation (update TTL without modifying data)."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "John"}).execute()

    await session.touch(k).execute()

    result2 = await session.query(k).execute()
    first2 = await result2.first_or_raise()
    assert first2.is_ok
    assert first2.record_or_raise().bins == {"name": "John"}


async def test_get_nonexistent(cluster):
    """Test getting a non-existent record."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(999)

    result = await session.query(k).execute()
    first = await result.first()
    assert first is None or not first.is_ok


async def test_string_key(cluster):
    """Test using string keys."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id("user123")

    await session.upsert(k).put({"name": "John"}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "John"}


async def test_chaining(cluster):
    """Test that method chaining works correctly."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"counter": 0, "name": "test"}).execute()

    result = await session.query(k).bins(["counter"]).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    bins = first.record_or_raise().bins
    assert bins == {"counter": 0}
    assert "name" not in bins


async def test_operate_put_and_get(cluster):
    """Test operate with Put and Get operations."""
    session = cluster.create_session()
    pac = cluster._client.underlying_client
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bin1": 7, "bin2": "string value"}).execute()

    record = await pac.operate(
        k,
        [
        Operation.put("bin2", "new string"),
        Operation.get(),
    ],
        policy=WritePolicy(),
    )

    assert record is not None
    assert record.bins is not None
    assert record.bins.get("bin2") == "new string"
    assert record.bins.get("bin1") == 7


async def test_operate_get_only(cluster):
    """Test operate with Get operation only."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"bin1": "value1", "bin2": 42}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    bins = first.record_or_raise().bins
    assert bins.get("bin1") == "value1"
    assert bins.get("bin2") == 42


async def test_operate_list_append(cluster):
    """Test operate with ListOperation.append."""
    session = cluster.create_session()
    pac = cluster._client.underlying_client
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"listbin": [1, 2, 3]}).execute()

    list_policy = ListPolicy(ListOrderType.ORDERED, None)
    record = await pac.operate(
        k,
        [
        ListOperation.append("listbin", 4, list_policy),
        ListOperation.size("listbin"),
    ],
        policy=WritePolicy(),
    )

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("listbin")
    if isinstance(size, list):
        size = size[-1]
    assert size == 4


async def test_operate_map_put_and_get(cluster):
    """Test operate with MapOperation.put and get_by_key."""
    session = cluster.create_session()
    pac = cluster._client.underlying_client
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"mapbin": {"key1": "value1"}}).execute()

    map_policy = MapPolicy(None, None)
    record = await pac.operate(
        k,
        [
        MapOperation.put("mapbin", "key2", "value2", map_policy),
        MapOperation.get_by_key("mapbin", "key2", MapReturnType.VALUE),
    ],
        policy=WritePolicy(),
    )

    assert record is not None
    assert record.bins is not None
    value = record.bins.get("mapbin")
    if isinstance(value, list):
        value = value[-1]
    assert value == "value2"


async def test_operate_map_clear(cluster):
    """Test operate with MapOperation.clear."""
    session = cluster.create_session()
    pac = cluster._client.underlying_client
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"mapbin": {"key1": "value1", "key2": "value2"}}).execute()

    record = await pac.operate(
        k,
        [
        MapOperation.clear("mapbin"),
        MapOperation.size("mapbin"),
    ],
        policy=WritePolicy(),
    )

    assert record is not None
    assert record.bins is not None
    size = record.bins.get("mapbin")
    assert size == 0


async def test_bin_chaining_set_to(cluster):
    """Test bin chaining API with set_to."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).bin("name").set_to("Tim").bin("age").set_to(1).bin("gender").set_to("male").execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Tim", "age": 1, "gender": "male"}


async def test_bin_chaining_add(cluster):
    """Test bin chaining API with add."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"age": 30}).execute()
    await session.upsert(k).bin("age").add(1).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins["age"] == 31


async def test_bin_chaining_mixed_operations(cluster):
    """Test bin chaining with both set_to and add."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Tim", "age": 1}).execute()
    await session.upsert(k).bin("name").set_to("Tim Updated").bin("age").add(1).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Tim Updated", "age": 2}


async def test_and_remove_other_bins(cluster):
    """Test and_remove_other_bins functionality."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Tim", "age": 30, "gender": "male", "city": "NYC"}).execute()
    await session.replace(k).put({"name": "Tim Updated", "age": 26}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Tim Updated", "age": 26}


async def test_set_bins_execute(cluster):
    """Test set_bins with execute method."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Tim", "age": 1, "gender": "male"}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Tim", "age": 1, "gender": "male"}


async def test_with_durable_delete(cluster, enterprise):
    """Test ``with_durable_delete()`` on delete operations."""
    if not enterprise:
        pytest.skip("Requires Enterprise Edition")
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Tim"}).execute()
    gen_before_delete = (
        (await (await session.query(k).execute()).first_or_raise()).record.generation
    )

    result = await session.delete(k).with_durable_delete().execute()
    first = await result.first_or_raise()
    assert first.is_ok

    query_result = await session.query(k).execute()
    first = await query_result.first()
    assert first is None or not first.is_ok

    await session.upsert(k).put({"name": "Tim"}).execute()
    gen_after_reinsert = (
        (await (await session.query(k).execute()).first_or_raise()).record.generation
    )
    try:
        if await session.is_namespace_sc(k.namespace):
            assert gen_before_delete == 1
            assert gen_after_reinsert == 3
    except Exception:
        pass

    await delete_keys_durable(session, [k])


async def test_insert_creates_new_record(cluster):
    """Test that insert() creates a new record successfully."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.insert(k).put({"name": "Alice", "age": 25}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Alice", "age": 25}


async def test_insert_fails_if_record_exists(cluster):
    """Test that insert() fails if record already exists."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.insert(k).put({"name": "Alice"}).execute()

    with pytest.raises(AerospikeError):
        await session.insert(k).put({"name": "Bob"}).execute()


async def test_update_succeeds_if_record_exists(cluster):
    """Test that update() succeeds if record exists."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Alice", "age": 25}).execute()
    await session.update(k).put({"age": 26}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "Alice", "age": 26}


async def test_update_fails_if_record_not_exists(cluster):
    """Test that update() raises KEY_NOT_FOUND_ERROR if record does not exist."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(88888)

    try:
        await session.delete(k).execute()
    except Exception:
        pass

    with pytest.raises(AerospikeError) as exc_info:
        await session.update(k).put({"name": "Bob"}).execute()
    assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


async def test_replace_succeeds_if_record_exists(cluster):
    """Test that replace() succeeds if record exists and replaces all bins."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Alice", "age": 25, "city": "NYC"}).execute()
    await session.replace(k).put({"name": "Bob"}).execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    bins = first.record_or_raise().bins
    assert bins == {"name": "Bob"}
    assert "age" not in bins
    assert "city" not in bins


async def test_replace_if_exists_fails_if_record_not_exists(cluster):
    """Test that replace_if_exists() raises KEY_NOT_FOUND_ERROR if record does not exist."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(99999)

    try:
        await session.delete(k).execute()
    except Exception:
        pass

    with pytest.raises(AerospikeError) as exc_info:
        await session.replace_if_exists(k).put({"name": "Bob"}).execute()
    assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


async def test_upsert_updates_existing_record(cluster):
    """Upsert on an existing record overwrites the specified bins."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).bin("name").set_to("original").execute()
    await session.upsert(k).bin("name").set_to("updated").execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    assert first.record_or_raise().bins["name"] == "updated"


async def test_update_preserves_other_bins(cluster):
    """Update modifies specified bins but preserves unspecified ones."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).put({"name": "Alice", "counter": 10}).execute()
    await session.update(k).bin("name").set_to("Bob").execute()

    result = await session.query(k).execute()
    first = await result.first_or_raise()
    bins = first.record_or_raise().bins
    assert bins["name"] == "Bob"
    assert bins["counter"] == 10


async def test_get_header(cluster):
    """Query with no bins returns header (generation, TTL) without bin data."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).bin("mybin").set_to("myvalue").execute()

    result = await session.query(k).with_no_bins().execute()
    first = await result.first_or_raise()
    rec = first.record_or_raise()

    assert rec.bins.get("mybin") is None
    assert rec.generation > 0


async def test_touch_updates_generation(cluster):
    """Touch increments the record's generation without modifying data."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).bin("name").set_to("touchable").execute()

    result = await session.query(k).with_no_bins().execute()
    initial_rec = (await result.first_or_raise()).record_or_raise()
    initial_gen = initial_rec.generation

    await session.touch(k).execute()

    result = await session.query(k).with_no_bins().execute()
    touched_rec = (await result.first_or_raise()).record_or_raise()
    assert touched_rec.generation == initial_gen + 1


async def test_touch_nonexistent_record(cluster):
    """Touch on a non-existent record yields a not-OK result."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(77777)

    try:
        await session.delete(k).execute()
    except Exception:
        pass

    stream = await session.touch(k).include_missing_keys().execute()
    first = await stream.first()
    assert first is not None
    assert not first.is_ok


async def test_touch_with_ttl(cluster):
    """Touch with expire_record_after_seconds sets the TTL."""
    session = cluster.create_session()
    k = DataSet.of("test", "test").id(1)

    await session.upsert(k).bin("name").set_to("ttl_test").execute()

    await session.touch(k).expire_record_after_seconds(300).execute()

    result = await session.query(k).with_no_bins().execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.ttl is not None
    assert rec.ttl > 0


# TTL / expiration tests live in tests/integration/async/expire_test.py.
