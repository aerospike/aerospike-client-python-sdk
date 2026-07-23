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

"""Implicit multi-record transactions for batch writes.

When a multi-key write batch executes against a strong-consistency (SC)
namespace on an MRT-capable cluster and is not already inside a
transaction, the SDK wraps it in an implicit multi-record transaction so
the batch's writes commit atomically. The behavior is controlled by
:attr:`~aerospike_sdk.policy.system_settings.TransactionSettings.implicit_batch_write_transactions`
(default ``True``) and is skipped entirely when any gate condition fails:
AP namespace, single-key operation, read-only batch, an explicit
transaction already active, a cluster node without MRT support, or the
setting turned off.

This module holds the gate predicate and the per-runtime execution
runners (async and blocking). The batch dispatchers in
:mod:`aerospike_sdk.aio.operations.query` consult the gate at execute
time — so a hot-reload of the setting takes effect on the next
operation — and route through a runner when it passes.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional, TypeVar

from aerospike_async import BatchPolicy, Txn
from aerospike_async.exceptions import CommitFailedError, ResultCode

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy.behavior_settings import Mode

log = logging.getLogger(SdkLoggers.COMMAND)

T = TypeVar("T")

# Transient MRT conflicts safe to retry with a fresh transaction. Matches
# the retry set of Session.do_in_transaction. A failed commit surfaces as
# CommitFailedError (the transaction-failed roll-up, carrying no result
# code); retrying it is safe because each attempt aborts and starts fresh.
_RETRYABLE_CODES = {
    ResultCode.MRT_BLOCKED,
    ResultCode.MRT_VERSION_MISMATCH,
}
_TXN_FAILED = getattr(ResultCode, "TXN_FAILED", None)
if _TXN_FAILED is not None:
    _RETRYABLE_CODES.add(_TXN_FAILED)

# Fallbacks when TransactionSettings fields are None (e.g. constructed raw
# instead of through fill_hard_defaults, which supplies the same values).
_DEFAULT_ATTEMPTS = 5
_DEFAULT_SLEEP_SECONDS = 1.0


def implicit_txn_enabled(sdk_client: Any, txn: Optional[Txn], mode: Optional[Mode]) -> bool:
    """Cheap synchronous portion of the implicit-transaction gate.

    Returns ``True`` when the batch is a wrap candidate: SC namespace, no
    explicit transaction active, and the
    ``implicit_batch_write_transactions`` setting enabled on the owning
    client. Callers must additionally confirm the batch contains writes
    and that the cluster supports MRT (``_supports_mrt`` /
    ``_supports_mrt_blocking`` on the SDK client) before wrapping.
    """
    if txn is not None or mode is not Mode.SC or sdk_client is None:
        return False
    transactions = sdk_client._sdk_settings.transactions
    return bool(transactions.implicit_batch_write_transactions)


def stamp_txn(policy: Optional[Any], txn: Txn) -> Any:
    """Return ``policy`` with ``txn`` stamped, creating a bare
    :class:`~aerospike_async.BatchPolicy` when the caller had none."""
    if policy is None:
        policy = BatchPolicy()
    policy.txn = txn
    return policy


def _retry_plan(transactions: Any) -> tuple[int, float]:
    attempts = transactions.number_of_attempts
    if attempts is None or attempts < 1:
        attempts = _DEFAULT_ATTEMPTS
    sleep = transactions.sleep_between_attempts
    sleep_seconds = sleep.total_seconds() if sleep is not None else _DEFAULT_SLEEP_SECONDS
    return attempts, sleep_seconds


def _should_retry(exc: BaseException, attempt: int, attempts: int) -> bool:
    if attempt + 1 >= attempts:
        return False
    return (
        isinstance(exc, CommitFailedError) or getattr(exc, "result_code", None) in _RETRYABLE_CODES
    )


async def run_in_implicit_txn(
    pac_client: Any,
    transactions: Any,
    attempt_fn: Callable[[Txn], Awaitable[T]],
) -> T:
    """Run one batch attempt inside a fresh implicit transaction (async).

    Creates a :class:`~aerospike_async.Txn`, invokes ``attempt_fn(txn)``
    (which must stamp the txn on its batch policy), commits on success and
    aborts on failure. Transient MRT conflicts (``MRT_BLOCKED``,
    ``MRT_VERSION_MISMATCH``) and failed commits
    (:class:`~aerospike_async.exceptions.CommitFailedError`) restart the
    whole attempt with a new transaction, up to
    ``transactions.number_of_attempts`` times with
    ``transactions.sleep_between_attempts`` between tries.
    """
    attempts, sleep_seconds = _retry_plan(transactions)
    for attempt in range(attempts):
        txn = Txn()
        try:
            result = await attempt_fn(txn)
            await pac_client.commit(txn)
            return result
        except Exception as exc:
            try:
                await pac_client.abort(txn)
            except Exception:
                log.debug("implicit txn abort failed", exc_info=True)
            if not _should_retry(exc, attempt, attempts):
                raise
            log.debug(
                "implicit txn attempt %d/%d hit retryable %r; retrying",
                attempt + 1, attempts, getattr(exc, "result_code", None),
            )
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
    raise AssertionError("unreachable: retry loop exits by return or raise")


def run_in_implicit_txn_blocking(
    pac_client: Any,
    transactions: Any,
    attempt_fn: Callable[[Txn], T],
) -> T:
    """Blocking sibling of :func:`run_in_implicit_txn` (no asyncio loop)."""
    attempts, sleep_seconds = _retry_plan(transactions)
    for attempt in range(attempts):
        txn = Txn()
        try:
            result = attempt_fn(txn)
            pac_client.commit_blocking(txn)
            return result
        except Exception as exc:
            try:
                pac_client.abort_blocking(txn)
            except Exception:
                log.debug("implicit txn abort failed", exc_info=True)
            if not _should_retry(exc, attempt, attempts):
                raise
            log.debug(
                "implicit txn attempt %d/%d hit retryable %r; retrying",
                attempt + 1, attempts, getattr(exc, "result_code", None),
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    raise AssertionError("unreachable: retry loop exits by return or raise")
