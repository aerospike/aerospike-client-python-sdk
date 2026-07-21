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

"""Implicit batch-write transactions on the sync surface (curated subset).

The sync path is an independent implementation — blocking batch
dispatchers plus ``run_in_implicit_txn_blocking`` (PAC ``commit_blocking``
/ ``abort_blocking``) — so the wrap is smoke-tested here in its own
right; the full gate matrix lives in the async suite and the unit tests.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.sync import SyncClient

from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    resolve_sc_namespace_sync,
    skip_reason_no_sc_namespace,
)


@pytest.fixture(scope="module")
def sync_client(aerospike_host_sc, client_policy_sc):
    client = SyncClient(seeds=aerospike_host_sc, policy=client_policy_sc)
    try:
        client.connect()
    except Exception as exc:
        pytest.skip(f"SC cluster unreachable at {aerospike_host_sc!r}: {exc}")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="module")
def sc_namespace(sync_client):
    sess = sync_client.create_session()
    try:
        return resolve_sc_namespace_sync(sess)
    except MultipleScNamespacesError as e:
        pytest.skip(
            "Several namespaces have strong-consistency enabled; set "
            f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
        )
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
def session(sync_client, sc_namespace):
    if not sync_client._supports_mrt_blocking():
        pytest.skip("cluster does not support multi-record transactions")
    return sync_client.create_session()


@pytest.fixture
def ds(sc_namespace):
    return DataSet.of(sc_namespace, "implicit_txn_sync")


@pytest.fixture
def txn_spy(monkeypatch):
    """Record every blocking implicit-transaction wrap, delegating to the real runner."""
    import aerospike_sdk.implicit_txn as impl
    from aerospike_sdk.aio.operations import batch as batch_mod
    from aerospike_sdk.aio.operations import query as query_mod

    calls: list = []
    real = impl.run_in_implicit_txn_blocking

    def spy(pac_client, transactions, attempt_fn):
        calls.append(transactions)
        return real(pac_client, transactions, attempt_fn)

    monkeypatch.setattr(batch_mod, "run_in_implicit_txn_blocking", spy)
    monkeypatch.setattr(query_mod, "run_in_implicit_txn_blocking", spy)
    return calls


def _reset(session, keys):
    for key in keys:
        try:
            session.delete(key).execute()
        except Exception:
            pass


def _bin_values(session, keys, bin_name):
    rows = session.query(keys).execute().collect()
    return {
        row.key.value: row.record.bins.get(bin_name)
        for row in rows
        if row.record is not None
    }


def test_multi_key_upsert_is_wrapped(session, ds, txn_spy):
    keys = ds.ids(1, 2, 3)
    _reset(session, keys)

    session.upsert(keys).put({"fixed": True, "version": 1}).execute()

    assert len(txn_spy) == 1
    assert _bin_values(session, keys, "version") == {1: 1, 2: 1, 3: 1}


def test_batch_builder_write_is_wrapped(session, ds, txn_spy):
    keys = ds.ids(10, 11)
    _reset(session, keys)

    batch = session.batch()
    batch.upsert(keys[0]).bin("n").set_to(1)
    batch.upsert(keys[1]).bin("n").set_to(2)
    batch.execute()

    assert len(txn_spy) == 1
    assert _bin_values(session, keys, "n") == {10: 1, 11: 2}


def test_setting_disabled_suppresses_wrap(sync_client, session, ds, txn_spy, monkeypatch):
    keys = ds.ids(20, 21)
    _reset(session, keys)

    settings = sync_client._sdk_settings
    monkeypatch.setattr(
        sync_client, "_sdk_settings",
        replace(
            settings,
            transactions=replace(
                settings.transactions, implicit_batch_write_transactions=False,
            ),
        ),
    )
    session.upsert(keys).put({"n": 1}).execute()

    assert txn_spy == []
    assert _bin_values(session, keys, "n") == {20: 1, 21: 1}


def test_explicit_transaction_is_not_double_wrapped(session, ds, txn_spy):
    keys = ds.ids(30, 31)
    _reset(session, keys)

    with session.transaction() as tx:
        tx.upsert(keys).put({"n": 7}).execute()

    assert txn_spy == []
    assert _bin_values(session, keys, "n") == {30: 7, 31: 7}
