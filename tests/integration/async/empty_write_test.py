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

"""Integration tests for write verbs that carry no operations."""

import pytest
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from tests.integration.namespace import general_namespace

_WRITE_VERBS = ("upsert", "insert", "update", "replace", "replace_if_exists")


@pytest.fixture(scope="module")
async def session(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as c:
        yield c.create_session()


@pytest.fixture
async def keys(session):
    """Three absent keys, removed again after the test."""
    ds = DataSet.of(general_namespace(), "empty_write_async")
    ks = [ds.id(i) for i in (1, 2, 3)]
    await session.delete(*ks).execute()
    yield ks
    await session.delete(*ks).execute()


def _assert_parameter_error(excinfo, verb="upsert"):
    assert excinfo.value.result_code == ResultCode.PARAMETER_ERROR
    assert verb in str(excinfo.value)


@pytest.mark.parametrize("verb", _WRITE_VERBS)
async def test_single_key_empty_put_raises_parameter_error(session, keys, verb):
    with pytest.raises(AerospikeError) as excinfo:
        await getattr(session, verb)(keys[0]).put({}).execute()
    _assert_parameter_error(excinfo, verb)


async def test_single_key_without_any_operation_raises_parameter_error(session, keys):
    with pytest.raises(AerospikeError) as excinfo:
        await session.upsert(keys[0]).execute()
    _assert_parameter_error(excinfo)


async def test_single_key_empty_put_with_ttl_raises_parameter_error(session, keys):
    with pytest.raises(AerospikeError) as excinfo:
        await session.upsert(keys[0]).put({}).expire_record_after_seconds(20).execute()
    _assert_parameter_error(excinfo)


async def test_empty_write_raises_even_with_error_handler(session, keys):
    seen = []
    with pytest.raises(AerospikeError) as excinfo:
        await session.upsert(keys[0]).put({}).execute(
            on_error=lambda key, index, exc: seen.append(exc))
    _assert_parameter_error(excinfo)
    assert seen == []


async def test_multi_key_empty_put_raises_parameter_error(session, keys):
    await session.upsert(keys[0]).put({"name": "Ada"}).execute()
    with pytest.raises(AerospikeError) as excinfo:
        await session.upsert(keys[0], keys[1]).put({}).execute()
    _assert_parameter_error(excinfo)


async def test_chained_empty_segment_raises_and_writes_nothing(session, keys):
    with pytest.raises(AerospikeError) as excinfo:
        await session.upsert(keys[0]).put({}).upsert(keys[2]).put({"name": "Ada"}).execute()
    _assert_parameter_error(excinfo)
    assert await (await session.query(keys[2]).execute()).first() is None


async def test_empty_put_followed_by_bins_still_writes(session, keys):
    await session.upsert(keys[0]).put({}).put({"name": "Ada"}).execute()
    record = (await (await session.query(keys[0]).execute()).first_or_raise()).record
    assert record.bins == {"name": "Ada"}
