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

import time

import pytest

from aerospike_async import CommitErrorType, CommitStatus
from aerospike_sdk import ClusterDefinition, DataSet, Host
from aerospike_sdk.exceptions import CommitError

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
