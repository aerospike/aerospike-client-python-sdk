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

"""Outcome of a multi-record transaction's commit or abort."""

from __future__ import annotations

from enum import Enum

from aerospike_async import AbortStatus, CommitStatus


class TxnStatus(Enum):
    """Outcome of :meth:`TransactionalSession.commit` or :meth:`TransactionalSession.abort`.

    A successful commit returns :attr:`COMMITTED`, a successful abort
    :attr:`ABORTED`. The ``*_ABANDONED`` members are also successful outcomes
    whose cleanup the server finishes on its own; a failure that leaves writes
    provisional raises :class:`~aerospike_sdk.exceptions.CommitError` instead
    of returning a status.

    Example::

        async with session.transaction() as tx:
            await tx.upsert(accounts.id("alice")).bin("balance").add(-250).execute()
            await tx.upsert(accounts.id("bob")).bin("balance").add(250).execute()
            status = await tx.commit()

        if status is TxnStatus.ROLL_FORWARD_CLOSE_ABANDONED:
            log.info("committed; the server will finish closing the transaction")

    See Also:
        :meth:`aerospike_sdk.aio.transactional_session.TransactionalSession.commit`
        :meth:`aerospike_sdk.aio.transactional_session.TransactionalSession.abort`
        :class:`~aerospike_sdk.exceptions.CommitError`: Raised when a commit
        stage fails.
    """

    COMMITTED = "committed"
    """Commit succeeded."""

    ABORTED = "aborted"
    """Abort succeeded."""

    ALREADY_COMMITTED = "already_committed"
    """The transaction was already committed."""

    ALREADY_ABORTED = "already_aborted"
    """The transaction was already aborted."""

    ROLL_FORWARD_CLOSE_ABANDONED = "roll_forward_close_abandoned"
    """The transaction was rolled forward, but closing it was abandoned.

    The writes are durable; the server will eventually close the transaction.
    """

    ROLL_BACK_ABANDONED = "roll_back_abandoned"
    """The client abandoned the roll back; the server will eventually abort
    the transaction."""

    ROLL_BACK_CLOSE_ABANDONED = "roll_back_close_abandoned"
    """The transaction was rolled back, but closing it was abandoned.

    The server will eventually close the transaction.
    """


# PAC's ``CommitStatus.ROLL_FORWARD_ABANDONED`` and
# ``AbortStatus.COMMIT_FAILED`` are never returned: both outcomes are raised.
_FROM_COMMIT = {
    CommitStatus.OK: TxnStatus.COMMITTED,
    CommitStatus.ALREADY_COMMITTED: TxnStatus.ALREADY_COMMITTED,
    CommitStatus.CLOSE_ABANDONED: TxnStatus.ROLL_FORWARD_CLOSE_ABANDONED,
}

_FROM_ABORT = {
    AbortStatus.OK: TxnStatus.ABORTED,
    AbortStatus.ALREADY_ABORTED: TxnStatus.ALREADY_ABORTED,
    AbortStatus.ROLL_BACK_ABANDONED: TxnStatus.ROLL_BACK_ABANDONED,
    AbortStatus.CLOSE_ABANDONED: TxnStatus.ROLL_BACK_CLOSE_ABANDONED,
}


def _from_commit(status: CommitStatus) -> TxnStatus:
    return _FROM_COMMIT[status]


def _from_abort(status: AbortStatus) -> TxnStatus:
    return _FROM_ABORT[status]
