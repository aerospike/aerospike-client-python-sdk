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

"""A batch write that spans two namespaces, with and without a transaction.

A transaction is scoped to one namespace, so a batch that spans two cannot be
atomic. What the client should do about that differs by shape, and this file
pins each one.

The second namespace deliberately does not exist: the decision is the client's
and is taken before anything reaches a node, so a real namespace is not needed
to observe it.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    resolve_sc_namespace,
    skip_reason_no_sc_namespace,
)

SET = "mixed_ns_txn"
OTHER_NS = "no_such_namespace_errspec"
BIN = "v"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster_sc(aerospike_host_sc, make_cluster_definition):
    async with make_cluster_definition(aerospike_host_sc, auth=True).connect() as c:
        yield c


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def sc_namespace(cluster_sc):
    sess = cluster_sc.create_session()
    try:
        return await resolve_sc_namespace(sess)
    except MultipleScNamespacesError as e:
        pytest.skip(f"Set AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}")
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
def session(cluster_sc):
    return cluster_sc.create_session()


@pytest.fixture
def keys(sc_namespace, request):
    """One key in the SC namespace, one in a namespace that does not exist.

    Unique per test and per run so nothing observes another's writes -- the
    explicit-txn cases assert a key is *absent*, which a reused key would mask.
    """
    tag = f"{request.node.name}_{uuid.uuid4().hex[:8]}"
    return [
        DataSet.of(sc_namespace, SET).id(f"present_{tag}"),
        DataSet.of(OTHER_NS, SET).id(f"elsewhere_{tag}"),
    ]


async def test_mixed_namespaces_without_a_transaction_reports_per_key(session, keys):
    """No transaction was asked for, so the spanning batch is not an error.

    Each key is judged on its own; the unknown namespace fails its row rather
    than the call.
    """
    stream = await session.upsert(keys).put({BIN: 1}).with_txn(None).execute()
    rows = await stream.collect()
    assert len(rows) == 2
    assert sum(1 for r in rows if r.is_ok) == 1
    assert sum(1 for r in rows if not r.is_ok) == 1


async def test_mixed_namespaces_under_an_implicit_transaction(session, keys):
    """An implicit MRT cannot span namespaces, and none is applied.

    The caller did not ask for a transaction here, so the batch proceeds and
    each key is judged on its own -- the same outcome as with no transaction.
    """
    stream = await session.upsert(keys).put({BIN: 1}).execute()
    rows = await stream.collect()
    assert len(rows) == 2
    assert sum(1 for r in rows if r.is_ok) == 1


async def test_mixed_namespaces_under_an_explicit_transaction(session, keys):
    """An explicit transaction is scoped to one namespace: the whole call fails.

    The caller asked for atomicity, so a batch that cannot be atomic is rejected
    outright rather than reported row by row -- the constraint belongs to the
    submitted call, not to any one record. The rejection is client-side, before
    a per-key command is sent, so the unknown namespace never reaches a node.
    """
    async def op(tx):
        await tx.upsert(keys).put({BIN: 1}).execute()

    with pytest.raises(AerospikeError) as excinfo:
        await session.do_in_transaction(op)

    assert "namespace must be the same" in str(excinfo.value).lower()
    assert excinfo.value.result_code == ResultCode.CLIENT_ERROR

    # Nothing was written: no partial commit where atomicity was requested.
    stream = await session.query(keys[0]).bins([BIN]).execute()
    assert [r async for r in stream if r.is_ok] == []


async def test_switching_namespaces_between_calls_in_a_transaction(session, keys):
    """The first command binds the transaction to its namespace.

    A later batch whose keys all share some other namespace is just as
    non-atomic as a mixed one, so it fails the call too -- and the abort that
    follows takes the first write with it.
    """
    async def op(tx):
        await tx.upsert(keys[0]).put({BIN: 1}).execute()
        await tx.upsert(DataSet.of(OTHER_NS, SET).ids("a", "b")).put({BIN: 2}).execute()

    with pytest.raises(AerospikeError) as excinfo:
        await session.do_in_transaction(op)

    assert "namespace must be the same" in str(excinfo.value).lower()
    assert excinfo.value.result_code == ResultCode.CLIENT_ERROR

    stream = await session.query(keys[0]).bins([BIN]).execute()
    assert [r async for r in stream if r.is_ok] == []
