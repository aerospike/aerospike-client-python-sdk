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

"""Query builder tweaks layer onto the Behavior's query policy, never replace it.

``limit()`` and ``records_per_second()`` are builder fields applied when the
policy is built at execute time, so the Behavior's timeouts and retries still
reach PAC.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from aerospike_sdk import Behavior
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.policy import Settings
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder

_TIMEOUT_MS = 7000
_RETRIES = 4

_BEHAVIOR = Behavior.DEFAULT.derive_with_changes(
    "query-policy-build",
    reads_query=Settings(
        total_timeout=timedelta(milliseconds=_TIMEOUT_MS), max_retries=_RETRIES,
    ),
)

_RUNTIMES = pytest.mark.parametrize("qb_cls", [QueryBuilder, SyncQueryBuilder], ids=["async", "sync"])


def _builder(qb_cls):
    qb = qb_cls(
        client=MagicMock(),
        namespace="test",
        set_name="t",
        behavior=_BEHAVIOR,
    )
    qb._resolved_namespace_mode = lambda: Mode.AP
    return qb


async def _run_dataset_query(qb):
    """Execute *qb* as a dataset query; return (policy sent, overall cap)."""
    sent = {}

    async def fake_async(self, policy, *args, **kwargs):
        sent["policy"] = policy
        return MagicMock(), None

    def fake_blocking(self, policy, *args, **kwargs):
        sent["policy"] = policy
        return MagicMock(), None

    if isinstance(qb, SyncQueryBuilder):
        with patch.object(SyncQueryBuilder, "_run_dataset_query_blocking", fake_blocking):
            _recordset, _reexecute, cap = qb._execute_dataset_query_blocking()
    else:
        with patch.object(QueryBuilder, "_run_dataset_query_async", fake_async):
            stream = await qb._execute_dataset_query()
        cap = stream._chunk_limit if qb._chunk_size else 0
    return sent["policy"], cap


@_RUNTIMES
@pytest.mark.parametrize("configure", [
    pytest.param(lambda qb: qb, id="no-tweaks"),
    pytest.param(lambda qb: qb.limit(10), id="limit"),
    pytest.param(lambda qb: qb.records_per_second(100), id="records-per-second"),
])
async def test_tweaks_keep_the_behavior_query_settings(qb_cls, configure):
    policy, _cap = await _run_dataset_query(configure(_builder(qb_cls)))
    assert policy.total_timeout == _TIMEOUT_MS
    assert policy.max_retries == _RETRIES


@_RUNTIMES
async def test_tweaks_reach_the_policy(qb_cls):
    policy, _cap = await _run_dataset_query(
        _builder(qb_cls).limit(10).records_per_second(100),
    )
    assert policy.max_records == 10
    assert policy.records_per_second == 100


@_RUNTIMES
async def test_chunked_query_fetches_per_chunk_and_caps_the_total(qb_cls):
    policy, cap = await _run_dataset_query(_builder(qb_cls).chunk_size(3).limit(10))
    assert policy.max_records == 3
    assert cap == 10


@_RUNTIMES
@pytest.mark.parametrize(
    "name", ["with_policy", "with_read_policy", "base_policy", "replica", "expected_duration"],
)
def test_raw_policy_methods_are_not_on_the_builder(qb_cls, name):
    """A Behavior scope or a QueryHint covers each one without bypassing the Behavior."""
    assert not hasattr(_builder(qb_cls), name)
