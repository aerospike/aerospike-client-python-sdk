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

"""Retry classification for multi-record transactions.

Shared by the async and sync retrying runners and the implicit-transaction
runner, which otherwise drift: a conflict one of them retries and another
re-raises is the same transaction behaving differently depending on which
entry point the caller used.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional, Union

from aerospike_async import ResultCode

from aerospike_sdk.exceptions import CommitError

# Transient conflicts safe to retry with a fresh transaction. Retrying is safe
# because each attempt aborts and starts over, so nothing partial carries over.
RETRYABLE_TXN_CODES = frozenset(
    {
        ResultCode.MRT_BLOCKED,
        ResultCode.MRT_VERSION_MISMATCH,
    }
)


def is_retryable_txn_error(exc: BaseException) -> bool:
    """Report whether a failed transaction attempt is worth retrying.

    A commit failure is classified by type rather than by result code: it is
    the roll-up of a failed verify or roll phase and carries no code of its
    own. It means the same thing as a conflict raised mid-block -- nothing was
    applied -- so it is retryable on the same grounds.

    Args:
        exc: The exception that ended the attempt.

    Returns:
        ``True`` when a fresh attempt may succeed.
    """
    if isinstance(exc, CommitError):
        return True
    return getattr(exc, "result_code", None) in RETRYABLE_TXN_CODES


# Fallbacks when TransactionSettings fields are None (e.g. constructed raw
# instead of through fill_hard_defaults, which supplies the same values).
DEFAULT_ATTEMPTS = 5
DEFAULT_SLEEP_SECONDS = 1.0


def resolve_retry_plan(
    transactions: Any,
    max_attempts: Optional[int] = None,
    sleep_between_retries: Optional[Union[float, timedelta]] = None,
) -> tuple[int, float]:
    """Resolve how many attempts to make and how long to wait between them.

    Explicit per-call values win; otherwise the cluster's transaction settings
    decide. Both retrying runners resolve through here so a transaction gets
    the same retry treatment whether the caller opened it or the SDK did --
    they previously carried separate defaults, one of which never slept.

    Args:
        transactions: The cluster's
            :class:`~aerospike_sdk.policy.system_settings.TransactionSettings`.
        max_attempts: Per-call override for the attempt count.
        sleep_between_retries: Per-call override for the pause, as seconds
            or a :class:`datetime.timedelta`.

    Returns:
        A ``(attempts, sleep_seconds)`` pair.

    Raises:
        ValueError: ``max_attempts`` was given and is less than 1.
    """
    if max_attempts is not None and max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempts = max_attempts
    if attempts is None:
        attempts = getattr(transactions, "number_of_attempts", None)
    if attempts is None or attempts < 1:
        attempts = DEFAULT_ATTEMPTS

    if sleep_between_retries is not None:
        return attempts, _to_seconds(sleep_between_retries)

    sleep = getattr(transactions, "sleep_between_attempts", None)
    return attempts, _to_seconds(sleep) if sleep is not None else DEFAULT_SLEEP_SECONDS


def _to_seconds(value: Union[float, timedelta]) -> float:
    """Accept either representation of a duration.

    The settings object types durations as ``timedelta``; the per-call argument
    has always taken plain seconds. Both spell the same thing, so both are
    accepted rather than making the caller remember which surface wants which.
    """
    return value.total_seconds() if isinstance(value, timedelta) else float(value)
