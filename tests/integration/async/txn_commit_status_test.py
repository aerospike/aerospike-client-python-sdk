# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Commit outcomes that only appear when a specific commit command fails.

A commit is three steps: mark-roll-forward, the roll-forward batch, then
closing the transaction monitor. The interesting outcomes are defined by which
one did not land, and no server setting makes a chosen one fail -- so the
client runs through a :class:`TcpGate` that forwards a set number of commands
and then severs the connection.

The two outcomes are not the same severity, and that is the point of the pair:

* an **abandoned roll-forward** leaves the writes provisional. Reporting it as
  success would present invisible writes as committed, so it raises.
* an **abandoned monitor close** leaves durable writes behind and only the
  server-side cleanup unfinished. That is success.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from aerospike_async import CommitErrorType, CommitStatus, TxnState
from aerospike_sdk import ClusterDefinition, DataSet, Host
from aerospike_sdk.exceptions import CommitError, ResultCode, TransactionError

from integration.tcp_gate import TcpGate

BIN_NAME = "bin"
NAMESPACE = "test_sc"

# Cut before mark-roll-forward, so the commit's first command is the one that
# fails and the transaction's outcome is left in doubt.
BEFORE_MARK_ROLL_FORWARD = 0
# Let mark-roll-forward through, so the roll-forward batch is the command that
# fails.
THROUGH_MARK_ROLL_FORWARD = 1
# Also let the roll-forward through, so the monitor close is what fails.
THROUGH_ROLL_FORWARD = 2


async def _gated_cluster(gate: TcpGate):
    """A cluster whose only route to the server is the gate."""
    definition = ClusterDefinition(
        hosts=Host.parse_hosts(f"127.0.0.1:{gate.port}", 3000)
    ).force_single_node()
    return await definition.connect()


class TestAbandonedRollForward:
    """The writes are still provisional, so the caller has to be told."""

    async def test_abandoned_roll_forward_raises(self, aerospike_host_sc_single):
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                key = DataSet.of(NAMESPACE, "gate_commit").id("abandoned")
                await session.upsert(key).put({BIN_NAME: 1}).execute()

                async def operation(tx):
                    await tx.upsert(key).put({BIN_NAME: 2}).execute()
                    # From here the next data message is the roll-forward.
                    gate.refuse_after_client_messages(THROUGH_MARK_ROLL_FORWARD)
                    return "unreachable"

                with pytest.raises(CommitError) as excinfo:
                    await session.do_in_transaction(operation, max_attempts=1)

                exc = excinfo.value
                assert exc.commit_error_type is CommitErrorType.ROLL_FORWARD_ABANDONED
            finally:
                await cluster.close()

    async def test_abandoned_roll_forward_is_not_retried(
        self, aerospike_host_sc_single
    ):
        """Retrying would open a second transaction on keys the server will commit."""
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                key = DataSet.of(NAMESPACE, "gate_commit").id("noretry")
                await session.upsert(key).put({BIN_NAME: 1}).execute()
                attempts = 0

                async def operation(tx):
                    nonlocal attempts
                    attempts += 1
                    await tx.upsert(key).put({BIN_NAME: 2}).execute()
                    gate.refuse_after_client_messages(THROUGH_MARK_ROLL_FORWARD)
                    return "unreachable"

                with pytest.raises(CommitError):
                    await session.do_in_transaction(
                        operation, max_attempts=3, sleep_between_retries=0.0
                    )
                assert attempts == 1
            finally:
                await cluster.close()


class TestAbandonedMonitorClose:
    """Durable writes with server-side cleanup outstanding: still success."""

    async def test_abandoned_close_is_success(self, aerospike_host_sc_single):
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                key = DataSet.of(NAMESPACE, "gate_commit").id("closed")
                await session.upsert(key).put({BIN_NAME: 1}).execute()

                async with session.transaction() as tx:
                    await tx.upsert(key).put({BIN_NAME: 2}).execute()
                    # Roll-forward lands; only the monitor close is cut off.
                    gate.refuse_after_client_messages(THROUGH_ROLL_FORWARD)
                    status = await tx.commit()

                assert status in (
                    CommitStatus.OK,
                    CommitStatus.CLOSE_ABANDONED,
                )
            finally:
                await cluster.close()


class TestInDoubtMarkRollForward:
    """The commit may still advance, so the block must not be run a second time."""

    async def test_in_doubt_mark_roll_forward_is_not_retried(
        self, aerospike_host_sc_single
    ):
        """Re-running could apply the same writes twice; the caller decides instead."""
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                # An in-doubt commit leaves the transaction unresolved, so the
                # record stays locked; a fresh key keeps the test re-runnable.
                key = DataSet.of(NAMESPACE, "gate_commit").id(
                    f"indoubt_{time.monotonic_ns()}"
                )
                await session.upsert(key).put({BIN_NAME: 1}).execute()
                attempts = 0

                async def operation(tx):
                    nonlocal attempts
                    attempts += 1
                    await tx.upsert(key).put({BIN_NAME: 2}).execute()
                    # Cut before mark-roll-forward, so the commit's outcome is
                    # unknown rather than cleanly failed.
                    gate.refuse_after_client_messages(BEFORE_MARK_ROLL_FORWARD)
                    return "unreachable"

                with pytest.raises(CommitError) as excinfo:
                    await session.do_in_transaction(
                        operation, max_attempts=3, sleep_between_retries=0.0
                    )

                exc = excinfo.value
                assert exc.commit_error_type is CommitErrorType.MARK_ROLL_FORWARD_ABANDONED
                assert exc.in_doubt is True
                assert attempts == 1
            finally:
                await cluster.close()


class TestCommitFailedIsRetryable:
    """An in-doubt commit leaves the only resolution path open: another commit."""

    async def test_the_transaction_survives_an_in_doubt_commit(
        self, aerospike_host_sc_single
    ):
        """Finalizing here would strand the transaction with no way to resolve it."""
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                key = DataSet.of(NAMESPACE, "gate_commit").id(
                    f"retryable_{time.monotonic_ns()}"
                )
                await session.upsert(key).put({BIN_NAME: 1}).execute()

                tx = session.transaction()
                await tx.__aenter__()
                await tx.upsert(key).put({BIN_NAME: 2}).execute()
                gate.refuse_after_client_messages(BEFORE_MARK_ROLL_FORWARD)

                with pytest.raises(CommitError) as excinfo:
                    await tx.commit()
                assert excinfo.value.in_doubt is True

                # The client core marks the state; the session keeps the
                # transaction because of it.
                assert tx._txn is not None
                assert tx._txn.state == TxnState.COMMIT_FAILED
                assert tx._finalized is False
            finally:
                await cluster.close()

    async def test_abort_is_refused_and_leaves_the_commit_path_open(
        self, aerospike_host_sc_single
    ):
        """Rolling back could discard writes the server is committing."""
        host, _, port = aerospike_host_sc_single.rpartition(":")
        async with await TcpGate.open(host, int(port)) as gate:
            cluster = await _gated_cluster(gate)
            try:
                session = cluster.create_session()
                key = DataSet.of(NAMESPACE, "gate_commit").id(
                    f"refused_{time.monotonic_ns()}"
                )
                await session.upsert(key).put({BIN_NAME: 1}).execute()

                tx = session.transaction()
                await tx.__aenter__()
                await tx.upsert(key).put({BIN_NAME: 2}).execute()
                gate.refuse_after_client_messages(BEFORE_MARK_ROLL_FORWARD)
                with pytest.raises(CommitError):
                    await tx.commit()

                # Refused client-side, so it does not need the connection back.
                with pytest.raises(TransactionError) as excinfo:
                    await tx.abort()
                assert excinfo.value.result_code == ResultCode.TXN_FAILED

                # Refusing an abort must not finalize what it refused to end.
                assert tx._txn is not None
                assert tx._finalized is False

                # A second commit gets past the "no active transaction" guard
                # and out to the network, where the cut connection keeps it
                # retrying. Reaching the timeout is the proof; waiting for the
                # retries to exhaust would only cost time.
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(tx.commit(), timeout=2.0)
            finally:
                await cluster.close()
