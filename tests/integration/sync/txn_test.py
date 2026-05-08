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

"""Sync MRT test subset: exercises :class:`SyncTransactionalSession`
against a live strong-consistency cluster.

This file is intentionally a focused subset of ``tests/integration/async/
txn_test.py`` — the async suite is the authoritative surface; this
suite verifies the sync wrapper correctly drives the underlying async
path for the critical MRT behaviors (commit, abort, conflict,
outside-txn block, batch).

All tests skip cleanly when the configured namespace is not strong-
consistency, matching the async suite's gate. Namespace selection follows
``integration.sc_namespace_resolve`` (auto-pick the sole SC namespace when env is unset).
Optional ``AEROSPIKE_HOST_SC`` targets an SC cluster while ``AEROSPIKE_HOST`` may point elsewhere.

Provenance (per repo rules):
    reference: client/src/test/java/com/aerospike/client/sdk/TxnTest.java
"""

from __future__ import annotations

import pytest

from aerospike_async import ResultCode
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync import SyncClient

from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    pinned_namespace_env_hint,
    resolve_sc_namespace_sync,
    skip_reason_no_sc_namespace,
)


BIN_NAME = "bin"


def _namespaces_on_cluster_hint_sync(session) -> str:
    try:
        names = sorted(session.info().namespaces())
    except Exception:
        return ""
    if not names:
        return ""
    return f" Namespaces on this cluster: {', '.join(names)}."


@pytest.fixture(scope="module")
def sc_namespace(aerospike_host_sc, client_policy_sc):
    client = SyncClient(seeds=aerospike_host_sc, policy=client_policy_sc)
    try:
        client.connect()
    except Exception as exc:
        pytest.skip(f"cluster unreachable at {aerospike_host_sc!r}: {exc}")
    try:
        sess = client.create_session()
        try:
            return resolve_sc_namespace_sync(sess)
        except MultipleScNamespacesError as e:
            pytest.skip(
                "Several namespaces have strong-consistency enabled; set "
                f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
            )
        except NoStrongConsistencyNamespace as e:
            pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))
    finally:
        client.close()


@pytest.fixture
def sync_client(aerospike_host_sc, client_policy_sc):
    """SyncClient against the SC test seed (``AEROSPIKE_HOST_SC`` or ``AEROSPIKE_HOST``)."""
    client = SyncClient(seeds=aerospike_host_sc, policy=client_policy_sc)
    try:
        client.connect()
    except Exception as exc:
        pytest.skip(f"SC cluster unreachable at {aerospike_host_sc!r}: {exc}")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def session(sync_client, sc_namespace):
    """Top-level (non-transactional) session; skips if the target namespace
    isn't strong-consistency — matches the async suite's ``assumeTrue`` gate.
    """
    sess = sync_client.create_session()
    try:
        status = sess.namespace_sc_status(sc_namespace)
    except Exception as exc:
        pytest.skip(
            f"SC namespace {sc_namespace!r} unreachable "
            f"({exc}); set AEROSPIKE_HOST_SC / AEROSPIKE_HOST / AEROSPIKE_SC_NAMESPACE to an SC cluster"
        )
    if not status.is_sc:
        ns_hint = _namespaces_on_cluster_hint_sync(sess)
        pin = pinned_namespace_env_hint()
        pytest.skip(f"{status.detail}{ns_hint}{pin} MRT tests require SC.")
    return sess


@pytest.fixture
def mrt_set(sc_namespace):
    """DataSet scoped to a dedicated set under the SC namespace."""
    return DataSet.of(sc_namespace, "mrt_sync")


def _fetch_bin(session, key):
    """Return the BIN_NAME value for ``key``, or ``None`` when missing."""
    stream = session.exists(key).execute()
    first = stream.first()
    if first is None or not first.as_bool():
        return None
    stream = session.query(key).execute()
    result = stream.first_or_raise().record_or_raise()
    return result.bins.get(BIN_NAME)


def _reset(session, key) -> None:
    """Delete ``key`` if present so each test starts clean."""
    try:
        session.delete(key).execute()
    except AerospikeError:
        pass


# ---------------------------------------------------------------------------
# 1. txnWrite: write inside a txn is visible after commit.
# ---------------------------------------------------------------------------
def test_txn_write(session, mrt_set):
    key = mrt_set.id("syncTxnWrite")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    def op(tx):
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    session.do_in_transaction(op)
    assert _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 2. txnAbortRollsBack: abort on context exit leaves the original value.
# ---------------------------------------------------------------------------
def test_txn_abort_rolls_back(session, mrt_set):
    key = mrt_set.id("syncTxnAbort")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    with session.begin_transaction() as tx:
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        tx.abort()

    assert _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 3. txnWriteConflict: nested txn writing the same key gets MRT_BLOCKED.
# ---------------------------------------------------------------------------
def test_txn_write_conflict(session, mrt_set):
    key = mrt_set.id("syncTxnConflict")
    _reset(session, key)

    def outer(tx1):
        tx1.upsert(key).put({BIN_NAME: "val1"}).execute()

        with session.begin_transaction() as tx2:
            with pytest.raises(AerospikeError) as excinfo:
                tx2.upsert(key).put({BIN_NAME: "val2"}).execute()
            assert excinfo.value.result_code == ResultCode.MRT_BLOCKED
            tx2.abort()

    session.do_in_transaction(outer)
    assert _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 4. txnWriteBlock: outside-txn write while a txn holds the key is blocked.
# ---------------------------------------------------------------------------
def test_txn_write_block(session, mrt_set):
    key = mrt_set.id("syncTxnBlock")
    _reset(session, key)
    session.upsert(key).put({BIN_NAME: "val1"}).execute()

    def op(tx):
        tx.upsert(key).put({BIN_NAME: "val2"}).execute()

        with pytest.raises(AerospikeError) as excinfo:
            session.upsert(key).put({BIN_NAME: "val3"}).execute()
        assert excinfo.value.result_code == ResultCode.MRT_BLOCKED

    session.do_in_transaction(op)
    assert _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 5. txnBatch: 10-key batch upsert under a txn commits all together.
# ---------------------------------------------------------------------------
def test_txn_batch(session, mrt_set):
    keys = [mrt_set.id(100 + i) for i in range(10)]
    for k in keys:
        _reset(session, k)
        session.upsert(k).put({BIN_NAME: 1}).execute()

    def op(tx):
        stream = tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        count = 0
        for result in stream:
            result.record_or_raise()
            count += 1
        assert count == len(keys)

    session.do_in_transaction(op)
    for k in keys:
        assert _fetch_bin(session, k) == 2


# ---------------------------------------------------------------------------
# 6. txnBatchAbort: 10-key batch upsert rolled back.
# ---------------------------------------------------------------------------
def test_txn_batch_abort(session, mrt_set):
    keys = [mrt_set.id(200 + i) for i in range(10)]
    for k in keys:
        _reset(session, k)
        session.upsert(k).put({BIN_NAME: 1}).execute()

    with session.begin_transaction() as tx:
        stream = tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        for result in stream:
            result.record_or_raise()
        tx.abort()

    for k in keys:
        assert _fetch_bin(session, k) == 1
