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

"""MRT test suite: covers PSDK's multi-record transaction surface
against a fixed catalog of scenarios so behavior doesn't silently
regress.

Optional ``AEROSPIKE_HOST_SC`` routes this suite (and durable-delete SC tests) to a
dedicated SC cluster seed while ``AEROSPIKE_HOST`` stays on another box.

Every test here needs a strong-consistency namespace. If
``AEROSPIKE_SC_NAMESPACE`` is unset and the cluster has exactly one SC namespace,
that name is used automatically; otherwise set the env var (required when several
namespaces are SC). When no SC namespace is reachable each test skips with a clear reason.

Provenance (per repo rules):
    reference: client/src/test/java/com/aerospike/client/sdk/TxnTest.java
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from aerospike_async import ResultCode
from aerospike_sdk import Client, DataSet
from aerospike_sdk.exceptions import AerospikeError

from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    pinned_namespace_env_hint,
    resolve_sc_namespace,
    skip_reason_no_sc_namespace,
)


BIN_NAME = "bin"


async def _namespaces_on_cluster_hint(session) -> str:
    try:
        names = sorted(await session.info().namespaces())
    except Exception:
        return ""
    if not names:
        return ""
    return f" Namespaces on this cluster: {', '.join(names)}."


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def sc_namespace(aerospike_host_sc, client_policy_sc):
    async with Client(seeds=aerospike_host_sc, policy=client_policy_sc) as client:
        sess = client.create_session()
        try:
            return await resolve_sc_namespace(sess)
        except MultipleScNamespacesError as e:
            pytest.skip(
                "Several namespaces have strong-consistency enabled; set "
                f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
            )
        except NoStrongConsistencyNamespace as e:
            pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
async def session(client_sc, sc_namespace):
    """Top-level (non-transactional) session used outside ``doInTransaction``.

    Skips the test if the configured SC namespace isn't available on the
    cluster.
    """
    sess = client_sc.create_session()
    try:
        status = await sess.namespace_sc_status(sc_namespace)
    except Exception as exc:
        pytest.skip(
            f"SC namespace {sc_namespace!r} unreachable "
            f"({exc}); set AEROSPIKE_HOST_SC / AEROSPIKE_SC_NAMESPACE or stand up Phase 3e.3"
        )
    if not status.is_sc:
        ns_hint = await _namespaces_on_cluster_hint(sess)
        pin = pinned_namespace_env_hint()
        pytest.skip(f"{status.detail}{ns_hint}{pin} MRT tests require SC.")
    return sess


@pytest.fixture
def mrt_set(sc_namespace):
    """DataSet scoped to a dedicated set under the SC namespace."""
    return DataSet.of(sc_namespace, "mrt_async")


async def _fetch_bin(session, key) -> object | None:
    """Return the BIN_NAME value for ``key``, or ``None`` when missing."""
    stream = await session.exists(key).execute()
    first = await stream.first()
    if first is None or not first.as_bool():
        return None
    stream = await session.query(key).execute()
    result = (await stream.first_or_raise()).record_or_raise()
    return result.bins.get(BIN_NAME)


async def _reset(session, key) -> None:
    """Delete ``key`` if present so each test starts clean."""
    try:
        await session.delete(key).execute()
    except AerospikeError:
        pass


# ---------------------------------------------------------------------------
# 1. txnWrite: write inside a txn is visible after commit.
# ---------------------------------------------------------------------------
async def test_txn_write(session, mrt_set):
    key = mrt_set.id("txnWrite")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 2. txnWriteTwice: last write inside a txn wins.
# ---------------------------------------------------------------------------
async def test_txn_write_twice(session, mrt_set):
    key = mrt_set.id("txnWriteTwice")
    await _reset(session, key)

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val1"}).execute()
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 3. txnWriteConflict: another txn trying to write the same key while a
#    txn holds it gets MRT_BLOCKED. We use begin_transaction (rather than
#    do_in_transaction) for the inner txn so its retry loop doesn't mask
#    the MRT_BLOCKED we're asserting on.
# ---------------------------------------------------------------------------
async def test_txn_write_conflict(session, mrt_set):
    key = mrt_set.id("txnWriteConflict")
    await _reset(session, key)

    async def outer(tx1):
        await tx1.upsert(key).put({BIN_NAME: "val1"}).execute()

        async with session.begin_transaction() as tx2:
            with pytest.raises(AerospikeError) as excinfo:
                await tx2.upsert(key).put({BIN_NAME: "val2"}).execute()
            assert excinfo.value.result_code == ResultCode.MRT_BLOCKED
            # Let the inner txn abort cleanly on context exit.
            await tx2.abort()

    await session.do_in_transaction(outer)
    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 4. txnReadFailsForAllStatesExceptOpen: issuing any command against a non-
#    OPEN txn must raise client-side (reference test does the same via
#    ``txn.setState(...)`` — PAC now exposes an equivalent setter).
# ---------------------------------------------------------------------------
async def test_txn_read_fails_for_all_states_except_open(session, client_sc, mrt_set):
    # ``session`` dep triggers the shared SC-namespace skip.
    del session
    from aerospike_async import Txn, TxnState

    key = mrt_set.id("txnReadFailsForAllStatesExceptOpen")

    # OPEN must not raise on a fresh txn — the query may return zero rows
    # for a missing key, and that's fine; we only care that no forbidden-
    # state error fires.
    for state, should_raise in (
        (TxnState.OPEN, False),
        (TxnState.COMMITTED, True),
        (TxnState.ABORTED, True),
        (TxnState.VERIFIED, True),
    ):
        tx_session = client_sc.transaction_session()
        # Allocate a txn without going through __aenter__, then force
        # the state to exercise the non-OPEN state-machine guard.
        tx_session._txn = Txn()
        tx_session._finalized = False
        tx_session._txn.state = state

        try:
            stream = await tx_session.query(key).execute()
            async for _ in stream:
                break
            if should_raise:
                pytest.fail(f"Expected AerospikeError for state {state}")
        except AerospikeError as exc:
            if not should_raise:
                pytest.fail(f"Unexpected error for state {state}: {exc}")
            msg = str(exc).lower()
            # Core reports a single generic "forbidden" message for all
            # non-OPEN states; tolerate that and any future state-specific
            # variants the reference uses.
            assert (
                "forbidden" in msg
                or "commit" in msg
                or "abort" in msg
                or "committed" in msg
                or "aborted" in msg
            ), f"Unexpected error text for state {state}: {exc}"
        finally:
            # Short-circuit finalization to avoid trying to commit/abort
            # a faked-state txn at teardown.
            tx_session._finalized = True
            tx_session._txn = None


# ---------------------------------------------------------------------------
# 5. txnWriteBlock: outside-txn write is blocked while a txn holds the key.
# ---------------------------------------------------------------------------
async def test_txn_write_block(session, mrt_set):
    key = mrt_set.id("txnWriteBlock")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

        with pytest.raises(AerospikeError) as excinfo:
            await session.upsert(key).put({BIN_NAME: "val3"}).execute()
        assert excinfo.value.result_code == ResultCode.MRT_BLOCKED

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 6. txnWriteRead: outside-txn read sees pre-txn value until commit.
# ---------------------------------------------------------------------------
async def test_txn_write_read(session, mrt_set):
    key = mrt_set.id("txnWriteRead")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        # Non-transactional reader must still see val1 pre-commit.
        assert await _fetch_bin(session, key) == "val1"

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 7. txnWriteAbort: explicit abort rolls back writes.
# ---------------------------------------------------------------------------
async def test_txn_write_abort(session, mrt_set):
    key = mrt_set.id("txnWriteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()
        # Read-your-own-writes inside the txn:
        assert await _fetch_bin(tx, key) == "val2"
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 8. txnDelete: delete inside a txn commits.
# ---------------------------------------------------------------------------
async def test_txn_delete(session, mrt_set):
    key = mrt_set.id("txnDelete")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.delete(key).durably_delete().execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) is None


# ---------------------------------------------------------------------------
# 9. txnDeleteAbort: delete rolled back restores the record.
# ---------------------------------------------------------------------------
async def test_txn_delete_abort(session, mrt_set):
    key = mrt_set.id("txnDeleteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.delete(key).durably_delete().execute()
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 10. txnDeleteTwice: second delete of the same key within one txn is a
#     no-op (idempotent) rather than raising.
# ---------------------------------------------------------------------------
async def test_txn_delete_twice(session, mrt_set):
    key = mrt_set.id("txnDeleteTwice")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.delete(key).durably_delete().execute()
        # The second delete must not blow up: we only assert it doesn't
        # raise an unexpected error — a KEY_NOT_FOUND_ERROR is acceptable.
        try:
            await tx.delete(key).durably_delete().execute()
        except AerospikeError as exc:
            assert exc.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) is None


# ---------------------------------------------------------------------------
# 11. txnTouch: touch under a txn keeps the record and commits.
# ---------------------------------------------------------------------------
async def test_txn_touch(session, mrt_set):
    key = mrt_set.id("txnTouch")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async def op(tx):
        await tx.touch(key).execute()

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 12. txnTouchAbort: touch rolled back leaves record intact.
# ---------------------------------------------------------------------------
async def test_txn_touch_abort(session, mrt_set):
    key = mrt_set.id("txnTouchAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1"}).execute()

    async with session.begin_transaction() as tx:
        await tx.touch(key).execute()
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 13. txnOperateWrite: combined set + read in one operate under txn.
# ---------------------------------------------------------------------------
async def test_txn_operate_write(session, mrt_set):
    key = mrt_set.id("txnOperateWrite")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1", "bin2": "bal1"}).execute()

    async def op(tx):
        stream = await (
            tx.upsert(key).set_to(BIN_NAME, "val2").get("bin2").execute()
        )
        rec = (await stream.first_or_raise()).record_or_raise()
        assert rec.bins.get("bin2") == "bal1"

    await session.do_in_transaction(op)
    assert await _fetch_bin(session, key) == "val2"


# ---------------------------------------------------------------------------
# 14. txnOperateWriteAbort: combined op rolled back.
# ---------------------------------------------------------------------------
async def test_txn_operate_write_abort(session, mrt_set):
    key = mrt_set.id("txnOperateWriteAbort")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val1", "bin2": "bal1"}).execute()

    async with session.begin_transaction() as tx:
        stream = await (
            tx.upsert(key).set_to(BIN_NAME, "val2").get("bin2").execute()
        )
        rec = (await stream.first_or_raise()).record_or_raise()
        assert rec.bins.get("bin2") == "bal1"
        await tx.abort()

    assert await _fetch_bin(session, key) == "val1"


# ---------------------------------------------------------------------------
# 15. txnBatch: 10-key batch upsert under a txn commits all together.
# ---------------------------------------------------------------------------
async def test_txn_batch(session, mrt_set):
    keys = [mrt_set.id(i) for i in range(0, 10)]
    for k in keys:
        await _reset(session, k)
        await session.upsert(k).put({BIN_NAME: 1}).execute()

    async def op(tx):
        stream = await tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        count = 0
        async for result in stream:
            # Only ensure the per-key op succeeded: writes don't return
            # the new bin value. The final check below queries
            # post-commit to confirm values landed.
            result.record_or_raise()
            count += 1
        assert count == len(keys)

    await session.do_in_transaction(op)
    for k in keys:
        assert await _fetch_bin(session, k) == 2


# ---------------------------------------------------------------------------
# 16. txnBatchAbort: 10-key batch upsert rolled back.
# ---------------------------------------------------------------------------
async def test_txn_batch_abort(session, mrt_set):
    keys = [mrt_set.id(i) for i in range(10, 20)]
    for k in keys:
        await _reset(session, k)
        await session.upsert(k).put({BIN_NAME: 1}).execute()

    async with session.begin_transaction() as tx:
        stream = await tx.upsert(keys).set_to(BIN_NAME, 2).execute()
        async for result in stream:
            result.record_or_raise()
        await tx.abort()

    for k in keys:
        assert await _fetch_bin(session, k) == 1


# ---------------------------------------------------------------------------
# 17. txnMrtExpiredAfterDeadline: a write issued after the txn deadline must
#     raise MRT_EXPIRED. JSDK parity (TxnTest.txnMrtExpiredAfterDeadline).
# ---------------------------------------------------------------------------
async def test_txn_mrt_expired_after_deadline(session, mrt_set):
    """Write after the txn deadline must raise MRT_EXPIRED.

    Mirrors :class:`TxnTest`.\\ ``txnMrtExpiredAfterDeadline``: pin a 2-second
    deadline on the active transaction, do one write, sleep past the deadline,
    then attempt a second write — the server must reject it with
    ``MRT_EXPIRED``. ``MRT_EXPIRED`` is non-retryable so
    :meth:`do_in_transaction` propagates the failure on the first attempt.
    """
    import asyncio

    key = mrt_set.id("txnMrtExpired")
    await _reset(session, key)
    await session.upsert(key).put({BIN_NAME: "val0"}).execute()

    async def op(tx):
        # Must be set before the first execute() under this txn — once a
        # builder captures the underlying Arc<Txn> the timeout is frozen.
        tx.txn.timeout = 2

        await tx.upsert(key).put({BIN_NAME: "val1"}).execute()
        await asyncio.sleep(3)
        await tx.upsert(key).put({BIN_NAME: "val2"}).execute()

    with pytest.raises(AerospikeError) as excinfo:
        await session.do_in_transaction(op)
    assert excinfo.value.result_code == ResultCode.MRT_EXPIRED

    # Pre-txn value must remain visible to non-transactional reads.
    assert await _fetch_bin(session, key) == "val0"
