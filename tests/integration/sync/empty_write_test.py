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

"""Integration tests for synchronous write verbs that carry no operations."""

import pytest
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from tests.integration.namespace import general_namespace

_WRITE_VERBS = ("upsert", "insert", "update", "replace", "replace_if_exists")


@pytest.fixture(scope="module")
def session(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c.create_session()


@pytest.fixture
def keys(session):
    """Three absent keys, removed again after the test."""
    ds = DataSet.of(general_namespace(), "empty_write_sync")
    ks = [ds.id(i) for i in (1, 2, 3)]
    session.delete(*ks).execute()
    yield ks
    session.delete(*ks).execute()


def _assert_parameter_error(excinfo, verb="upsert"):
    assert excinfo.value.result_code == ResultCode.PARAMETER_ERROR
    assert verb in str(excinfo.value)


@pytest.mark.parametrize("verb", _WRITE_VERBS)
def test_single_key_empty_put_raises_parameter_error(session, keys, verb):
    with pytest.raises(AerospikeError) as excinfo:
        getattr(session, verb)(keys[0]).put({}).execute()
    _assert_parameter_error(excinfo, verb)


def test_empty_write_raises_even_with_error_handler(session, keys):
    seen = []
    with pytest.raises(AerospikeError) as excinfo:
        session.upsert(keys[0]).put({}).execute(
            on_error=lambda key, index, exc: seen.append(exc))
    _assert_parameter_error(excinfo)
    assert seen == []


def test_chained_empty_segment_raises_and_writes_nothing(session, keys):
    with pytest.raises(AerospikeError) as excinfo:
        session.upsert(keys[0]).put({}).upsert(keys[2]).put({"name": "Ada"}).execute()
    _assert_parameter_error(excinfo)
    assert session.query(keys[2]).execute().first() is None
