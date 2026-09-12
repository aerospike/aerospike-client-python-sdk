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
# License for the specific language governing permissions and limitations
# under the License.

"""Integration tests for the IndexBuilder SDK API (sync)."""

import pytest

from aerospike_async import FilterExpression
from aerospike_sdk import DataSet, Filter
from aerospike_sdk.exceptions import AerospikeError
from tests.integration.namespace import general_namespace
from tests.pac_compat import requires_server_compiled_ael

NS = general_namespace()
SET = "ael_idx_set_sync"
DS = DataSet.of(NS, SET)


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    """Module-scoped sync connection: one handshake for the whole file."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@requires_server_compiled_ael
def test_sync_create_index_from_ael_string_and_query(cluster):
    """Create an expression index from an AEL string, list it, query through it, drop it."""
    index_name = "psdk_ael_age_idx_sync"
    ael = "$.age + 1"
    session = cluster.create_session()

    try:
        session.index(NS, SET).named(index_name).drop()
    except Exception:
        pass

    keys = [DS.id(f"ael_u{i}") for i in range(5)]
    for i, k in enumerate(keys):
        session.upsert(k).put({"age": 30 + i}).execute()

    try:
        index_task = (
            session.index(NS, SET)
            .on_expression(ael)
            .named(index_name)
            .numeric()
            .create()
        )
        index_task.wait_till_complete_blocking()

        listed = [i for i in session.list_indexes() if i["name"] == index_name]
        assert listed, "AEL-string expression index not visible in list_indexes"

        flt = Filter.range("age", 32, 34).expression(
            FilterExpression.from_server_compiled_ael(ael),
        )
        stream = session.query(NS, SET).filter(flt).bins(["age"]).execute()
        ages = sorted(
            [r.record.bins["age"] for r in stream if r.is_ok and r.record],
        )
        assert ages == [31, 32, 33]
    finally:
        session.delete(keys).execute()
        try:
            session.index(NS, SET).named(index_name).drop()
        except Exception:
            pass


@requires_server_compiled_ael
def test_sync_create_index_from_boolean_ael_rejected(cluster):
    """The index basis must produce a value — a boolean AEL predicate is rejected."""
    session = cluster.create_session()
    with pytest.raises(AerospikeError):
        (
            session.index(NS, SET)
            .on_expression("$.age > 31")
            .named("psdk_ael_bool_idx_sync")
            .numeric()
            .create()
        )
