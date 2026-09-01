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

"""Unit tests for SyncTransactionalSession API shape and lifecycle.

The underlying PAC client is mocked so these tests don't need an SC cluster.
"""

from dataclasses import replace
from datetime import timedelta

import pytest

from aerospike_sdk import ResultCode

from aerospike_sdk import (
    AbortStatus,
    CommitStatus,
    SyncTransactionalSession,
    Txn,
)
from aerospike_async.exceptions import CommitFailedError
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.exceptions import AerospikeError, CommitError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.system_settings import TransactionSettings
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.session import SyncSession


class _FakePacClient:
    """Minimal stand-in for the PAC Client with commit/abort_blocking stubs."""

    def __init__(self) -> None:
        self.commit_calls: list = []
        self.abort_calls: list = []
        self.commit_return: CommitStatus = CommitStatus.OK
        self.abort_return: AbortStatus = AbortStatus.OK

    def commit_blocking(self, txn):
        self.commit_calls.append(txn)
        return self.commit_return

    def abort_blocking(self, txn):
        self.abort_calls.append(txn)
        return self.abort_return


class _FakeSyncClient:
    """Stand-in for :class:`aerospike_sdk.sync.client.SyncClient`."""

    def __init__(self) -> None:
        self._pac = _FakePacClient()
        self._indexes_monitor = None
        self._namespace_mode_cache: dict = {}
        # Mirrors the real client: the retry plan is resolved from here.
        self._sdk_settings = fill_hard_defaults(None)

    @property
    def underlying_client(self):
        return self._pac

    def _resolve_namespace_mode_blocking(self, namespace):
        return Mode.AP


@pytest.fixture
def sync_client() -> _FakeSyncClient:
    return _FakeSyncClient()


@pytest.fixture
def sync_tx(sync_client: _FakeSyncClient) -> SyncTransactionalSession:
    return SyncTransactionalSession(
        client=sync_client, behavior=Behavior.DEFAULT,
    )  # type: ignore[arg-type]


def test_txn_attribute_raises_before_enter(
    sync_tx: SyncTransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="not active"):
        _ = sync_tx.txn
    assert sync_tx.active is False


def test_enter_allocates_txn(sync_tx: SyncTransactionalSession) -> None:
    with sync_tx as tx:
        assert tx is sync_tx
        assert isinstance(tx.txn, Txn)
        assert tx.active is True


def test_clean_exit_commits(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    with sync_tx as tx:
        txn_ref = tx.txn
    assert len(sync_client._pac.commit_calls) == 1
    assert sync_client._pac.commit_calls[0] is txn_ref
    assert len(sync_client._pac.abort_calls) == 0
    assert sync_tx.active is False


def test_exception_exit_aborts(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom):
        with sync_tx as tx:
            _ = tx.txn
            raise _Boom("oops")
    assert len(sync_client._pac.abort_calls) == 1
    assert len(sync_client._pac.commit_calls) == 0
    assert sync_tx.active is False


def test_explicit_commit_returns_status(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    with sync_tx as tx:
        status = tx.commit()
        assert status == CommitStatus.OK
        assert tx.active is False
    # __exit__ must not double-commit after an explicit commit:
    assert len(sync_client._pac.commit_calls) == 1


def test_ops_after_explicit_commit_run_txn_free(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    # Twin of the aio drift guard: explicit commit drops the txn reference,
    # so later builders run transaction-free.
    with sync_tx as tx:
        tx.commit()
        assert tx.get_current_transaction() is None


def test_explicit_abort_returns_status(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    with sync_tx as tx:
        status = tx.abort()
        assert status == AbortStatus.OK
        assert tx.active is False
    assert len(sync_client._pac.abort_calls) == 1
    assert len(sync_client._pac.commit_calls) == 0


def test_rollback_is_alias_for_abort(
    sync_tx: SyncTransactionalSession,
    sync_client: _FakeSyncClient,
) -> None:
    with sync_tx as tx:
        status = tx.rollback()
        assert status == AbortStatus.OK
    assert len(sync_client._pac.abort_calls) == 1


def test_commit_without_active_txn_raises(
    sync_tx: SyncTransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="No active transaction"):
        sync_tx.commit()


def test_abort_without_active_txn_raises(
    sync_tx: SyncTransactionalSession,
) -> None:
    with pytest.raises(RuntimeError, match="No active transaction"):
        sync_tx.abort()


def test_double_enter_raises(sync_tx: SyncTransactionalSession) -> None:
    with sync_tx:
        with pytest.raises(RuntimeError, match="already active"):
            sync_tx.__enter__()


def test_subclasses_sync_session() -> None:
    """SyncTransactionalSession must be a proper SyncSession subclass so
    session APIs (query, upsert, batch, ...) work inside the ``with`` block.
    """
    assert issubclass(SyncTransactionalSession, SyncSession)


def test_default_behavior_applied(
    sync_client: _FakeSyncClient,
) -> None:
    sync_tx = SyncTransactionalSession(
        client=sync_client, behavior=Behavior.DEFAULT,
    )  # type: ignore[arg-type]
    assert sync_tx.behavior is Behavior.DEFAULT


def test_explicit_behavior_honored(
    sync_client: _FakeSyncClient,
) -> None:
    custom = Behavior.DEFAULT.derive_with_changes(name="custom_sync_mrt")
    sync_tx = SyncTransactionalSession(
        client=sync_client, behavior=custom,
    )  # type: ignore[arg-type]
    assert sync_tx.behavior is custom


def test_session_transaction_forwards_behavior(
    sync_client: _FakeSyncClient,
) -> None:
    """``SyncSession.transaction()`` must carry the session's own behavior
    through to the returned :class:`SyncTransactionalSession`.

    Mirrors the async forwarding guard: pins the ``transaction()`` forwarding
    path (the behavior-drop regression surface) so the Phase-3 hoist of
    ``transaction()`` onto a shared session base lands against green coverage
    on both trees.
    """
    custom = Behavior.DEFAULT.derive_with_changes(name="sync_sess_fwd_mrt")
    sync_session = SyncSession(
        client=sync_client, behavior=custom,
    )  # type: ignore[arg-type]
    tx = sync_session.transaction()
    assert isinstance(tx, SyncTransactionalSession)
    assert tx.behavior is sync_session.behavior
    assert tx.behavior is custom


# -- do_in_transaction retry logic -------------------------------------------


def _make_sync_session():
    """Construct a SyncSession bound to a fake client (no IO)."""
    client = _FakeSyncClient()
    sync_session = SyncSession(
        client=client, behavior=Behavior.DEFAULT,
    )  # type: ignore[arg-type]
    return sync_session, client


def test_do_in_transaction_commits_on_success() -> None:
    sync_session, client = _make_sync_session()

    def op(tx):
        assert isinstance(tx, SyncTransactionalSession)
        assert tx.active is True
        return "ok"

    result = sync_session.do_in_transaction(op)
    assert result == "ok"
    assert len(client._pac.commit_calls) == 1
    assert len(client._pac.abort_calls) == 0


def test_do_in_transaction_aborts_on_non_retryable() -> None:
    sync_session, client = _make_sync_session()

    def op(tx):
        raise AerospikeError("boom", result_code=ResultCode.PARAMETER_ERROR)

    with pytest.raises(AerospikeError):
        sync_session.do_in_transaction(op)
    assert len(client._pac.commit_calls) == 0
    assert len(client._pac.abort_calls) == 1


def test_do_in_transaction_retries_then_succeeds() -> None:
    sync_session, client = _make_sync_session()
    calls = 0

    def op(tx):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise AerospikeError(
                "conflict", result_code=ResultCode.MRT_BLOCKED,
            )
        return "eventually"

    result = sync_session.do_in_transaction(op, max_attempts=5, sleep_between_retries=0.0)
    assert result == "eventually"
    assert calls == 3
    # Two aborted attempts + one committed attempt:
    assert len(client._pac.abort_calls) == 2
    assert len(client._pac.commit_calls) == 1


def test_do_in_transaction_exhausts_retries() -> None:
    sync_session, client = _make_sync_session()

    def op(tx):
        raise AerospikeError(
            "persistent conflict",
            result_code=ResultCode.MRT_VERSION_MISMATCH,
        )

    with pytest.raises(AerospikeError):
        sync_session.do_in_transaction(op, max_attempts=3, sleep_between_retries=0.0)
    assert len(client._pac.abort_calls) == 3
    assert len(client._pac.commit_calls) == 0


def test_do_in_transaction_rejects_zero_attempts() -> None:
    sync_session, _ = _make_sync_session()
    with pytest.raises(ValueError, match="max_attempts"):
        sync_session.do_in_transaction(lambda tx: None, max_attempts=0)


def test_do_in_transaction_retries_commit_failure() -> None:
    """A failed commit is a transient conflict, like a failed operation.

    The commit is the last thing an attempt does, so a failure there means the
    same thing as a conflict raised mid-block: nothing was applied, and a fresh
    attempt may succeed. It arrives as CommitFailedError -- the roll-up that
    carries no result code -- so classifying retryability by result code alone
    misses it.
    """
    sync_session, client = _make_sync_session()
    calls = 0

    def commit_blocking(txn):
        nonlocal calls
        client._pac.commit_calls.append(txn)
        calls += 1
        if calls < 3:
            raise CommitFailedError("commit verify failed")
        return CommitStatus.OK

    client._pac.commit_blocking = commit_blocking

    assert sync_session.do_in_transaction(
        lambda tx: "done", max_attempts=5, sleep_between_retries=0.0,
    ) == "done"
    assert calls == 3


def test_do_in_transaction_commit_failure_exhausts_retries() -> None:
    """When it never succeeds, the caller sees a PSDK commit failure.

    The underlying client's own exception type is translated at the boundary
    like every other failure, so a caller catching PSDK exceptions does not
    have to reach past the SDK for this one case.
    """
    sync_session, client = _make_sync_session()

    def commit_blocking(txn):
        client._pac.commit_calls.append(txn)
        raise CommitFailedError("commit verify failed")

    client._pac.commit_blocking = commit_blocking

    with pytest.raises(CommitError):
        sync_session.do_in_transaction(lambda tx: None, max_attempts=3, sleep_between_retries=0.0)
    assert len(client._pac.commit_calls) == 3


def test_nested_do_in_transaction_joins_the_outer_one() -> None:
    """Sync twin: nesting joins rather than opening a second transaction."""
    sync_session, client = _make_sync_session()
    seen: list = []

    def inner(tx):
        seen.append(tx)
        return "inner"

    def outer(tx):
        seen.append(tx)
        return tx.do_in_transaction(inner)

    assert sync_session.do_in_transaction(outer) == "inner"
    assert len(client._pac.commit_calls) == 1
    assert len(client._pac.abort_calls) == 0
    assert seen[0] is seen[1]


def test_cluster_settings_drive_retry_when_not_overridden() -> None:
    """Sync twin: with no per-call arguments the cluster settings decide.

    Each runtime reads the settings off its own client, so this can be right
    in one leg and wrong in the other.
    """
    sync_session, client = _make_sync_session()
    client._sdk_settings = replace(
        client._sdk_settings,
        transactions=TransactionSettings(
            number_of_attempts=3, sleep_between_attempts=timedelta(0),
        ),
    )
    attempts = 0

    def always_blocked(tx):
        nonlocal attempts
        attempts += 1
        raise AerospikeError("blocked", result_code=ResultCode.MRT_BLOCKED)

    with pytest.raises(AerospikeError):
        sync_session.do_in_transaction(always_blocked)
    assert attempts == 3


def test_per_call_arguments_override_cluster_settings() -> None:
    """Sync twin: an explicit argument still wins over the configured value."""
    sync_session, client = _make_sync_session()
    client._sdk_settings = replace(
        client._sdk_settings,
        transactions=TransactionSettings(
            number_of_attempts=9, sleep_between_attempts=timedelta(0),
        ),
    )
    attempts = 0

    def always_blocked(tx):
        nonlocal attempts
        attempts += 1
        raise AerospikeError("blocked", result_code=ResultCode.MRT_BLOCKED)

    with pytest.raises(AerospikeError):
        sync_session.do_in_transaction(
            always_blocked, max_attempts=2, sleep_between_retries=0.0,
        )
    assert attempts == 2


def test_sleep_between_retries_accepts_a_timedelta() -> None:
    """Sync twin: seconds and timedelta are interchangeable here."""
    sync_session, _ = _make_sync_session()
    attempts = 0

    def always_blocked(tx):
        nonlocal attempts
        attempts += 1
        raise AerospikeError("blocked", result_code=ResultCode.MRT_BLOCKED)

    with pytest.raises(AerospikeError):
        sync_session.do_in_transaction(
            always_blocked, max_attempts=2, sleep_between_retries=timedelta(0),
        )
    assert attempts == 2
