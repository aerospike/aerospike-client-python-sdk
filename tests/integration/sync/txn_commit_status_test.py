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

"""In-doubt commit outcomes, reached through the sync runtime.

Deliberately narrower than the async suite. What it protects is the part that
is genuinely written twice: ``sync/transactional_session.py`` has its own
``commit``, ``abort`` and ``__exit__``, so the rule about which failures keep
the transaction open is implemented separately there and would not be caught
by any amount of async coverage.
"""

from __future__ import annotations

import time

import pytest

from aerospike_async import TxnState
from aerospike_sdk import DataSet
from aerospike_sdk.sync import ClusterDefinition
from aerospike_sdk.exceptions import AerospikeError, CommitError

from integration.tcp_gate import threaded_gate

BIN_NAME = "bin"
NAMESPACE = "test_sc"

# Cut before the mark-roll-forward reaches the server: the commit's outcome is
# then unknown rather than cleanly failed.
BEFORE_MARK_ROLL_FORWARD = 0


def _gated_cluster(gate):
    return ClusterDefinition("127.0.0.1", gate.port).force_single_node().connect()


class TestSyncCommitFailedIsRetryable:
    """An in-doubt commit leaves the only resolution path open: another commit."""

    def test_the_transaction_survives_an_in_doubt_commit(self, aerospike_host_sc_single):
        host, _, port = aerospike_host_sc_single.rpartition(":")
        with threaded_gate(host, int(port)) as gate, _gated_cluster(gate) as cluster:
            session = cluster.create_session()
            key = DataSet.of(NAMESPACE, "gate_commit").id(
                f"sync_retryable_{time.monotonic_ns()}"
            )
            session.upsert(key).put({BIN_NAME: 1}).execute()

            tx = session.transaction()
            tx.__enter__()
            tx.upsert(key).put({BIN_NAME: 2}).execute()
            gate.refuse_after_client_messages(BEFORE_MARK_ROLL_FORWARD)

            with pytest.raises(CommitError) as excinfo:
                tx.commit()
            assert excinfo.value.in_doubt is True

            assert tx._txn is not None
            assert tx._txn.state == TxnState.COMMIT_FAILED
            assert tx._finalized is False

            # Refused client-side, so the cut connection is not in the way.
            with pytest.raises(AerospikeError) as abort_exc:
                tx.abort()
            assert "commit already failed" in str(abort_exc.value).lower()
            # Refusing an abort must not finalize what it refused to end.
            assert tx._txn is not None
            assert tx._finalized is False
