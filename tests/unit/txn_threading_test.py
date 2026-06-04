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

"""Unit tests for implicit MRT threading: Session -> builder -> policy.

These verify that every builder spun up off a Session captures the
session's current ``Txn`` and that the builder's ``_apply_txn`` helper
stamps it on every outer policy it hands to the PAC. The tests use
real PAC types (``WritePolicy``, ``ReadPolicy``, ``QueryPolicy``,
``BatchPolicy``, ``Txn``) so we exercise the real setters; there is no
network I/O.
"""

import pytest

from aerospike_async import (
    BatchPolicy,
    Key,
    QueryPolicy,
    ReadPolicy,
    ResultCode,
    Txn,
    WritePolicy,
)

from aerospike_sdk import AbortStatus, CommitStatus, TransactionalSession
from aerospike_sdk.aio.operations.batch import BatchOperationBuilder
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.aio.session import Session
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior import Behavior


class _FakePac:
    """Stand-in for the PAC client; builders accept any object here."""


class _FakeSdkClient:
    def __init__(self) -> None:
        self._async_client = _FakePac()
        self._client = self._async_client
        self._indexes_monitor = None


# -- QueryBuilder ------------------------------------------------------------

def test_query_builder_captures_txn_at_construction() -> None:
    txn = Txn()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s", txn=txn,
    )
    assert qb._txn is txn


def test_query_builder_default_txn_is_none() -> None:
    qb = QueryBuilder(client=_FakePac(), namespace="test", set_name="s")
    assert qb._txn is None


def test_query_builder_apply_txn_stamps_each_policy_type() -> None:
    txn = Txn()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s", txn=txn,
    )
    for policy in (WritePolicy(), ReadPolicy(), QueryPolicy(), BatchPolicy()):
        stamped = qb._apply_txn(policy)
        assert stamped is policy
        assert stamped.txn is not None
        assert stamped.txn.id == txn.id


def test_query_builder_apply_txn_noop_outside_mrt() -> None:
    qb = QueryBuilder(client=_FakePac(), namespace="test", set_name="s")
    wp = WritePolicy()
    assert qb._apply_txn(wp) is wp
    assert wp.txn is None


def test_query_builder_apply_txn_tolerates_none() -> None:
    txn = Txn()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s", txn=txn,
    )
    assert qb._apply_txn(None) is None


def test_query_builder_with_txn_overrides_captured() -> None:
    t1 = Txn()
    t2 = Txn()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s", txn=t1,
    )
    assert qb._txn is t1
    result = qb.with_txn(t2)
    assert result is qb
    assert qb._txn is t2
    assert qb._apply_txn(WritePolicy()).txn.id == t2.id


def test_query_builder_with_txn_none_clears_ambient() -> None:
    txn = Txn()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s", txn=txn,
    )
    qb.with_txn(None)
    assert qb._txn is None
    assert qb._apply_txn(WritePolicy()).txn is None


def test_query_builder_with_txn_drops_cached_base_policies() -> None:
    """After .with_txn() the cached base policies must be re-derived so
    they reflect the (new) txn."""
    rp = ReadPolicy()
    wp = WritePolicy()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s",
        cached_read_policy=rp, cached_write_policy=wp,
    )
    assert qb._base_read_policy is rp
    assert qb._base_write_policy is wp
    qb.with_txn(Txn())
    assert qb._base_read_policy is None
    assert qb._base_write_policy is None


def test_query_builder_under_txn_skips_cached_policies() -> None:
    """Constructing under an active txn must ignore cached policies (which
    were built without the txn) to force re-derivation at execute time."""
    rp = ReadPolicy()
    wp = WritePolicy()
    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s",
        cached_read_policy=rp, cached_write_policy=wp, txn=Txn(),
    )
    assert qb._base_read_policy is None
    assert qb._base_write_policy is None


# -- WriteSegmentBuilder delegation ------------------------------------------

def test_write_segment_with_txn_delegates_to_query_builder() -> None:
    qb = QueryBuilder(client=_FakePac(), namespace="test", set_name="s")
    seg = WriteSegmentBuilder(qb)
    txn = Txn()
    result = seg.with_txn(txn)
    assert result is seg
    assert qb._txn is txn


# -- _SingleKeyWriteSegment --------------------------------------------------

def test_single_key_segment_captures_txn() -> None:
    key = Key("test", "s", 1)
    txn = Txn()
    seg = _SingleKeyWriteSegment(
        client=_FakePac(), key=key, op_type="upsert",
        behavior=None, write_policy=None, read_policy=None, txn=txn,
    )
    assert seg._txn is txn


def test_single_key_segment_apply_txn_stamps() -> None:
    key = Key("test", "s", 1)
    txn = Txn()
    seg = _SingleKeyWriteSegment(
        client=_FakePac(), key=key, op_type="upsert",
        behavior=None, write_policy=None, read_policy=None, txn=txn,
    )
    wp = WritePolicy()
    stamped = seg._apply_txn(wp)
    assert stamped.txn is not None
    assert stamped.txn.id == txn.id


def test_single_key_segment_under_txn_drops_cached_policies() -> None:
    """Cached session-level policies were built without a txn; they must
    not leak into an MRT's operations."""
    key = Key("test", "s", 1)
    seg = _SingleKeyWriteSegment(
        client=_FakePac(), key=key, op_type="upsert",
        behavior=None, write_policy=WritePolicy(),
        read_policy=ReadPolicy(), txn=Txn(),
    )
    assert seg._write_policy is None
    assert seg._read_policy is None


def test_single_key_segment_with_txn_threads_through_promotion() -> None:
    key = Key("test", "s", 1)
    seg = _SingleKeyWriteSegment(
        client=_FakePac(), key=key, op_type="upsert",
        behavior=None, write_policy=None, read_policy=None,
    )
    txn = Txn()
    seg.with_txn(txn)
    seg._promote()
    assert seg._qb is not None
    assert seg._qb._txn is txn


# -- BatchOperationBuilder ---------------------------------------------------

def test_batch_builder_captures_txn() -> None:
    txn = Txn()
    bb = BatchOperationBuilder(client=_FakePac(), txn=txn)
    assert bb._txn is txn


def test_batch_builder_apply_txn_stamps_batch_policy() -> None:
    txn = Txn()
    bb = BatchOperationBuilder(client=_FakePac(), txn=txn)
    bp = BatchPolicy()
    stamped = bb._apply_txn(bp)
    assert stamped.txn is not None
    assert stamped.txn.id == txn.id


def test_batch_builder_with_txn_roundtrip() -> None:
    bb = BatchOperationBuilder(client=_FakePac())
    assert bb._txn is None
    txn = Txn()
    assert bb.with_txn(txn) is bb
    assert bb._txn is txn
    assert bb.with_txn(None) is bb
    assert bb._txn is None


# -- Session -> builder propagation ------------------------------------------

def test_session_bind_txn_helper_applies_to_builder() -> None:

    session = Session.__new__(Session)
    session._txn = Txn()

    qb = QueryBuilder(client=_FakePac(), namespace="test", set_name="s")
    returned = session._bind_txn(qb)
    assert returned is qb
    assert qb._txn is session._txn


def test_session_bind_txn_noop_when_no_active_txn() -> None:

    session = Session.__new__(Session)
    session._txn = None

    qb = QueryBuilder(
        client=_FakePac(), namespace="test", set_name="s",
        txn=None,  # builder starts with no txn
    )
    session._bind_txn(qb)
    assert qb._txn is None


# -- Session.do_in_transaction retry loop ------------------------------------

class _FakePacClient:
    """Minimal PAC stand-in for do_in_transaction tests: just enough to
    satisfy TransactionalSession.commit / abort."""

    def __init__(self) -> None:
        self.commit_calls: list = []
        self.abort_calls: list = []
        self._commit_ok = CommitStatus.OK
        self._abort_ok = AbortStatus.OK

    async def commit(self, txn):
        self.commit_calls.append(txn)
        return self._commit_ok

    async def abort(self, txn):
        self.abort_calls.append(txn)
        return self._abort_ok


class _FakeSdkClientForRetry:
    """Stand-in for aerospike_sdk.aio.client.Client used in retry tests."""

    def __init__(self) -> None:
        self._async_client = _FakePacClient()
        self._client = self._async_client
        self._indexes_monitor = None

    def transaction_session(self, behavior=None):
        return TransactionalSession(client=self, behavior=behavior)


def _make_session_for_retry() -> "object":

    client = _FakeSdkClientForRetry()
    return Session(client=client, behavior=Behavior.DEFAULT)  # type: ignore[arg-type]


async def test_do_in_transaction_returns_value_on_success() -> None:
    session = _make_session_for_retry()
    calls: list = []

    async def op(tx):
        calls.append(tx)
        return "ok"

    result = await session.do_in_transaction(op)
    assert result == "ok"
    assert len(calls) == 1
    # Must have committed, not aborted.
    assert len(session._client._async_client.commit_calls) == 1
    assert len(session._client._async_client.abort_calls) == 0


async def test_do_in_transaction_retries_on_transient() -> None:

    session = _make_session_for_retry()
    attempts = {"n": 0}

    async def flaky(tx):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise AerospikeError(
                "blocked", result_code=ResultCode.MRT_BLOCKED,
            )
        return "won"

    result = await session.do_in_transaction(flaky, max_attempts=5)
    assert result == "won"
    assert attempts["n"] == 3
    pac = session._client._async_client
    # Two failed attempts got aborted; the third was committed.
    assert len(pac.abort_calls) == 2
    assert len(pac.commit_calls) == 1


async def test_do_in_transaction_gives_up_after_max_attempts() -> None:

    session = _make_session_for_retry()
    attempts = {"n": 0}

    async def always_blocked(tx):
        attempts["n"] += 1
        raise AerospikeError(
            "still blocked", result_code=ResultCode.MRT_VERSION_MISMATCH,
        )

    with pytest.raises(AerospikeError) as excinfo:
        await session.do_in_transaction(always_blocked, max_attempts=3)
    assert excinfo.value.result_code == ResultCode.MRT_VERSION_MISMATCH
    assert attempts["n"] == 3
    pac = session._client._async_client
    assert len(pac.abort_calls) == 3
    assert len(pac.commit_calls) == 0


async def test_do_in_transaction_does_not_retry_on_non_transient() -> None:

    session = _make_session_for_retry()
    attempts = {"n": 0}

    async def hard_fail(tx):
        attempts["n"] += 1
        raise AerospikeError(
            "bad key", result_code=ResultCode.KEY_NOT_FOUND_ERROR,
        )

    with pytest.raises(AerospikeError):
        await session.do_in_transaction(hard_fail, max_attempts=5)
    assert attempts["n"] == 1


async def test_do_in_transaction_rejects_zero_max_attempts() -> None:
    session = _make_session_for_retry()

    async def op(tx):
        return "unused"

    with pytest.raises(ValueError, match="max_attempts"):
        await session.do_in_transaction(op, max_attempts=0)


async def test_do_in_transaction_propagates_non_aerospike_errors() -> None:
    session = _make_session_for_retry()

    class _AppError(RuntimeError):
        pass

    async def op(tx):
        raise _AppError("boom")

    with pytest.raises(_AppError):
        await session.do_in_transaction(op, max_attempts=5)
    pac = session._client._async_client
    assert len(pac.abort_calls) == 1
    assert len(pac.commit_calls) == 0
