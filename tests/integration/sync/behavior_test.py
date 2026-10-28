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

"""Behavior resolution through the sync session and builders on a live cluster."""

import pytest

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy.behavior_settings import Settings
from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


_RETRYABLE_MARKED = Behavior.DEFAULT.derive_with_changes(
    "retryable_marked", writes_retryable=Settings(send_key=True),
)
_NON_RETRYABLE_MARKED = Behavior.DEFAULT.derive_with_changes(
    "non_retryable_marked", writes_non_retryable=Settings(send_key=True),
)


def _stored_user_keys(cluster, dataset) -> set:
    return {
        result.record.key.value
        for result in cluster.create_session().query(dataset).execute()
        if result.record.key.value is not None
    }


@pytest.mark.parametrize("marked, expect_marked_idempotent", [
    pytest.param(_RETRYABLE_MARKED, True, id="send_key-on-retryable"),
    pytest.param(_NON_RETRYABLE_MARKED, False, id="send_key-on-non-retryable"),
])
def test_write_resolves_scope_from_its_operations(cluster, marked, expect_marked_idempotent):
    """``send_key`` set on one write scope reaches exactly the writes of that kind."""
    dataset = DataSet.of(general_namespace(), f"bhv_retry_s_{marked.name}")
    session = cluster.create_session(marked)
    keys = dataset.ids("u1", "i1", "b1", "b2", "a1", "ba1", "ba2")
    for key in keys:
        cluster.create_session().delete(key).execute()
    try:
        session.upsert(dataset.id("u1")).bin("v").set_to(1).execute()
        session.insert(dataset.id("i1")).put({"v": 1}).execute()
        session.upsert(*dataset.ids("b1", "b2")).bin("v").set_to(1).execute()
        session.upsert(dataset.id("a1")).bin("v").add(1).execute()
        session.upsert(*dataset.ids("ba1", "ba2")).bin("v").add(1).execute()

        idempotent = {"u1", "i1", "b1", "b2"}
        non_idempotent = {"a1", "ba1", "ba2"}
        expected = idempotent if expect_marked_idempotent else non_idempotent
        assert _stored_user_keys(cluster, dataset) == expected
    finally:
        for key in keys:
            cluster.create_session().delete(key).execute()
