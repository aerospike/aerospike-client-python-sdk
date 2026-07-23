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

"""Implicit batch-write transactions against a real SC cluster (async).

A qualifying multi-key write batch (SC namespace, MRT-capable server, no
explicit transaction, ``implicit_batch_write_transactions`` enabled) is
wrapped in an implicit multi-record transaction. These tests observe the
wrap through a spy on the implicit-transaction runner (the wrap itself,
plus the commit, run against the live cluster) and verify every gate
condition that must suppress it.

Requires ``AEROSPIKE_HOST_SC`` (or an SC namespace on the default seed);
skips cleanly otherwise.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
import pytest_asyncio

from aerospike_sdk import DataSet

from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    resolve_sc_namespace,
    skip_reason_no_sc_namespace,
)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def sc_namespace(cluster_sc):
    sess = cluster_sc.create_session()
    try:
        return await resolve_sc_namespace(sess)
    except MultipleScNamespacesError as e:
        pytest.skip(
            "Several namespaces have strong-consistency enabled; set "
            f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
        )
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest_asyncio.fixture(loop_scope="session")
async def session(cluster_sc, sc_namespace):
    """Session on the SC cluster; skips when the cluster cannot run MRTs."""
    if not await cluster_sc._client._supports_mrt():
        pytest.skip("cluster does not support multi-record transactions")
    return cluster_sc.create_session()


@pytest.fixture
def ds(sc_namespace):
    return DataSet.of(sc_namespace, "implicit_txn_async")


@pytest.fixture
def txn_spy(monkeypatch):
    """Record every implicit-transaction wrap while delegating to the real runner."""
    import aerospike_sdk.implicit_txn as impl
    from aerospike_sdk.aio.operations import query as query_mod

    calls: list = []
    real = impl.run_in_implicit_txn

    async def spy(pac_client, transactions, attempt_fn):
        calls.append(transactions)
        return await real(pac_client, transactions, attempt_fn)

    monkeypatch.setattr(query_mod, "run_in_implicit_txn", spy)
    return calls


async def _reset(session, keys):
    for key in keys:
        try:
            await session.delete(key).execute()
        except Exception:
            pass


async def _bin_values(session, keys, bin_name):
    stream = await session.query(keys).execute()
    rows = await stream.collect()
    return {row.key.value: row.record.bins.get(bin_name) for row in rows if row.record is not None}


async def test_multi_key_upsert_is_wrapped(session, ds, txn_spy):
    """The ticket's canonical shape: session.upsert(ds.ids(...)) on SC."""
    keys = ds.ids(1, 2, 3)
    await _reset(session, keys)

    await session.upsert(keys).put({"fixed": True, "version": 1}).execute()

    assert len(txn_spy) == 1
    values = await _bin_values(session, keys, "version")
    assert values == {1: 1, 2: 1, 3: 1}


async def test_multi_segment_write_chain_is_wrapped(session, ds, txn_spy):
    # The kwarg form routes through the full builder; a chain grown from the
    # positional single-key fast segment does not carry the SDK-client handle
    # the implicit-transaction gate needs, so it would not wrap.
    keys = ds.ids(10, 11)
    await _reset(session, keys)

    await (
        session.upsert(key=keys[0]).bin("n").set_to(1)
        .upsert(keys[1]).bin("n").set_to(2)
        .execute()
    )

    assert len(txn_spy) == 1
    values = await _bin_values(session, keys, "n")
    assert values == {10: 1, 11: 2}


async def test_multi_key_delete_is_wrapped(session, ds, txn_spy):
    keys = ds.ids(20, 21)
    await session.upsert(keys).put({"n": 1}).execute()
    txn_spy.clear()

    await session.delete(keys).execute()

    assert len(txn_spy) == 1
    assert await _bin_values(session, keys, "n") == {}


async def test_setting_disabled_suppresses_wrap(cluster_sc, session, ds, txn_spy, monkeypatch):
    keys = ds.ids(30, 31)
    await _reset(session, keys)

    settings = cluster_sc._client._sdk_settings
    monkeypatch.setattr(
        cluster_sc._client, "_sdk_settings",
        replace(
            settings,
            transactions=replace(
                settings.transactions, implicit_batch_write_transactions=False,
            ),
        ),
    )
    await session.upsert(keys).put({"n": 1}).execute()

    assert txn_spy == []
    assert await _bin_values(session, keys, "n") == {30: 1, 31: 1}


async def test_explicit_transaction_is_not_double_wrapped(session, ds, txn_spy):
    keys = ds.ids(40, 41)
    await _reset(session, keys)

    async with session.transaction() as tx:
        await tx.upsert(keys).put({"n": 7}).execute()

    assert txn_spy == []
    assert await _bin_values(session, keys, "n") == {40: 7, 41: 7}


async def test_with_txn_none_opts_out(session, ds, txn_spy):
    keys = ds.ids(50, 51)
    await _reset(session, keys)

    await session.upsert(keys).put({"n": 3}).with_txn(None).execute()

    assert txn_spy == []
    assert await _bin_values(session, keys, "n") == {50: 3, 51: 3}


async def test_read_only_multi_key_is_not_wrapped(session, ds, txn_spy):
    keys = ds.ids(60, 61)
    await session.upsert(keys).put({"n": 1}).execute()
    txn_spy.clear()

    stream = await session.query(keys).execute()
    rows = await stream.collect()

    assert txn_spy == []
    assert len(rows) == 2


async def test_single_key_write_is_not_wrapped(session, ds, txn_spy):
    key = ds.id(70)
    await _reset(session, [key])

    await session.upsert(key).put({"n": 1}).execute()

    assert txn_spy == []
