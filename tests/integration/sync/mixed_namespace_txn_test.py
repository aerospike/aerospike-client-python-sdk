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

"""One case per sync dispatch shape for the one-transaction-one-namespace rule.

The rule lives in the shared batch error handler, so this is not a mirror of
the async suite. What is *not* shared is the wiring: ``sync/operations/
query_dispatch.py`` is an independent implementation with routes the async tier
does not have -- ``_execute_blocking_fast_path`` sends a multi-key single-spec
batch straight to ``_execute_batch_*_blocking``, skipping
``_execute_spec_blocking`` -- and each route must still deliver the raise.
"""

from __future__ import annotations

import uuid

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode

try:
    from integration.sc_namespace_resolve import (
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        resolve_sc_namespace_sync,
        skip_reason_no_sc_namespace,
    )
except ImportError:  # pragma: no cover - path shim, as in the sibling suites
    from tests.integration.sc_namespace_resolve import (  # type: ignore[no-redef]
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        resolve_sc_namespace_sync,
        skip_reason_no_sc_namespace,
    )

SET = "mixed_ns_txn_sync"
OTHER_NS = "no_such_namespace_errspec"
BIN = "v"


@pytest.fixture(scope="module")
def cluster_sc(aerospike_host_sc, make_cluster_definition):
    with make_cluster_definition(aerospike_host_sc, sync=True, auth=True).connect() as c:
        yield c


@pytest.fixture(scope="module")
def sc_namespace(cluster_sc):
    try:
        return resolve_sc_namespace_sync(cluster_sc.create_session())
    except MultipleScNamespacesError as e:
        pytest.skip(f"Set AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}")
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
def session(cluster_sc):
    return cluster_sc.create_session()


@pytest.fixture
def keys(sc_namespace, request):
    """Unique per test and per run so nothing observes another's writes."""
    tag = f"{request.node.name}_{uuid.uuid4().hex[:8]}"
    return [
        DataSet.of(sc_namespace, SET).id(f"present_{tag}"),
        DataSet.of(OTHER_NS, SET).id(f"elsewhere_{tag}"),
    ]


def test_multi_key_single_spec_batch_is_rejected_in_a_transaction(session, keys):
    """The fast path: one spec, several keys, dispatched without _execute_spec_blocking."""
    def op(tx):
        tx.upsert(keys).put({BIN: 1}).execute()

    with pytest.raises(AerospikeError) as excinfo:
        session.do_in_transaction(op)
    assert "namespace must be the same" in str(excinfo.value).lower()
    assert excinfo.value.result_code == ResultCode.CLIENT_ERROR

    # Nothing was written where atomicity was requested.
    assert [r for r in session.query(keys[0]).bins([BIN]).execute() if r.is_ok] == []


def test_chained_multi_spec_batch_is_rejected_in_a_transaction(session, keys):
    """The other sync route: several segments, folded by _execute_multispec_blocking."""
    def op(tx):
        tx.upsert(keys[0]).bin(BIN).set_to(1).upsert(keys[1]).bin(BIN).set_to(2).execute()

    with pytest.raises(AerospikeError) as excinfo:
        session.do_in_transaction(op)
    assert "namespace must be the same" in str(excinfo.value).lower()
    assert excinfo.value.result_code == ResultCode.CLIENT_ERROR


def test_a_single_namespace_batch_still_runs_in_a_transaction(session, sc_namespace):
    """The guard must not fire on the ordinary case."""
    ds = DataSet.of(sc_namespace, SET)
    same = [ds.id("same_a"), ds.id("same_b")]

    def op(tx):
        tx.upsert(same).put({BIN: 9}).execute()

    session.do_in_transaction(op)
    rows = [r for r in session.query(same[0]).bins([BIN]).execute() if r.is_ok]
    assert rows and rows[0].record.bins[BIN] == 9
