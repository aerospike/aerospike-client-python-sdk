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

"""Query builder tweaks combined with a Behavior's query settings, end to end (sync)."""

from datetime import timedelta

import pytest

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import Settings
from tests.integration.namespace import general_namespace

_SET = "query_policy_fields_sync"
_BEHAVIOR = Behavior.DEFAULT.derive_with_changes(
    "query-policy-fields-sync",
    reads_query=Settings(total_timeout=timedelta(seconds=7), max_retries=4),
)


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        yield cluster


def test_limit_and_rate_run_under_a_behavior(cluster):
    session = cluster.create_session(_BEHAVIOR)
    ds = DataSet.of(general_namespace(), _SET)
    for i in range(10):
        session.upsert(ds.id(i)).put({"v": i}).execute()

    rows = session.query(ds).limit(3).records_per_second(1000).execute().collect()

    assert 0 < len(rows) <= 3
