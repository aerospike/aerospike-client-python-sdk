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

"""Session verbs take a DataSet, one Key, keys as varargs, or a list of keys."""

import time
from unittest.mock import MagicMock

import pytest
from aerospike_native import ClientPolicy

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.session import Session
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync.session import Session as SyncSession

USERS = DataSet.of("test", "users")
K1 = USERS.id(1)
K2 = USERS.id(2)

WRITE_VERBS = [
    "upsert", "insert", "update", "replace", "replace_if_exists", "delete", "touch", "exists",
]


def _offline_session(session_cls, client_cls):
    client = client_cls("127.0.0.1:3000", policy=ClientPolicy())
    client._client = MagicMock()
    client._connected = True
    client._cached_supports_query_selection = True
    client._cached_supports_server_compiled_ael = True
    client._routing_capability_stamp = time.monotonic()
    return session_cls(client=client, behavior=Behavior.DEFAULT)


@pytest.fixture(params=[(Session, Client), (SyncSession, SyncClient)], ids=["async", "sync"])
def session(request):
    return _offline_session(*request.param)


@pytest.mark.parametrize("call, keys", [
    (lambda s: s.query(USERS), None),
    (lambda s: s.query(K1), None),
    (lambda s: s.query(K1, K2), 2),
    (lambda s: s.query([K1, K2]), 2),
], ids=["dataset", "key", "varargs", "list"])
def test_query_accepts_the_four_shapes(session, call, keys):
    builder = call(session)
    assert (len(builder._keys) if builder._keys else None) == keys


@pytest.mark.parametrize("verb", WRITE_VERBS)
def test_write_verbs_accept_a_key_varargs_and_a_list(session, verb):
    for args in ((K1,), (K1, K2), ([K1, K2],)):
        getattr(session, verb)(*args)


def test_query_rejects_namespace_and_set_strings(session):
    with pytest.raises(TypeError, match=r"DataSet\.of"):
        session.query("test", "users")


def test_index_rejects_a_namespace_string(session):
    with pytest.raises(TypeError, match=r"DataSet\.of"):
        session.index("test")


@pytest.mark.parametrize("call", [
    lambda s: s.query(namespace="test", set_name="users"),
    lambda s: s.query(dataset=USERS),
    lambda s: s.query(key=K1),
    lambda s: s.query(keys=[K1, K2]),
    lambda s: s.query(K1, behavior=Behavior.DEFAULT),
    lambda s: s.query(arg1=K1),
    lambda s: s.upsert(key=K1),
    lambda s: s.upsert(dataset=USERS, key_value=1),
    lambda s: s.upsert(namespace="test", set_name="users", key_value=1),
    lambda s: s.delete(arg1=K1),
    lambda s: s.index("test", "users"),
    lambda s: s.index(dataset=USERS),
    lambda s: s.upsert(USERS, K1),
    lambda s: s.insert(USERS, K1),
    lambda s: s.update(USERS, K1),
    lambda s: s.replace(USERS, K1),
], ids=[
    "query-namespace-set", "query-dataset-kw", "query-key-kw", "query-keys-kw",
    "query-behavior", "query-arg1-kw", "upsert-key-kw", "upsert-dataset-key-value",
    "upsert-namespace-set-key-value", "delete-arg1-kw", "index-namespace-set", "index-dataset-kw",
    "upsert-dataset-trailing-key", "insert-dataset-trailing-key",
    "update-dataset-trailing-key", "replace-dataset-trailing-key",
])
def test_removed_forms_raise_type_error(session, call):
    with pytest.raises(TypeError):
        call(session)
