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

"""Synchronous multi-record transaction (MRT) session."""

from __future__ import annotations

import types
from typing import Optional, TYPE_CHECKING

from aerospike_async import AbortStatus, CommitStatus, Txn

from aerospike_sdk.sync.session import SyncSession

if TYPE_CHECKING:
    from aerospike_sdk.policy.behavior import Behavior
    from aerospike_sdk.sync.client import SyncClient


class SyncTransactionalSession(SyncSession):
    """Sync context manager grouping operations into a multi-record transaction.

    Every session API (``query``, ``upsert``, ``insert``, ``batch``, ...)
    works unchanged inside ``with``; the active :class:`~aerospike_async.Txn`
    is threaded onto every policy the builders hand to the PAC.

    On clean exit the transaction commits; if an exception propagates out
    the transaction aborts. Explicit :meth:`commit`, :meth:`abort`, and
    :meth:`rollback` (alias for ``abort``) are available for manual control.

    Example:
        >>> with client.create_session().begin_transaction() as tx:
        ...     tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
        ...     tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

    See Also:
        :meth:`SyncSession.begin_transaction`:
            Preferred construction entry.
    """

    def __init__(self, client: SyncClient, behavior: Behavior) -> None:
        """Construct via :meth:`SyncSession.begin_transaction` rather than directly."""
        super().__init__(client, behavior)
        self._finalized = False

    @property
    def txn(self) -> Txn:
        """Return the active :class:`~aerospike_async.Txn`.

        Raises:
            RuntimeError: If the session has not been entered (no active txn).
        """
        if self._txn is None:
            raise RuntimeError("TransactionalSession is not active.")
        return self._txn

    @property
    def active(self) -> bool:
        """``True`` when a transaction has been started and not yet finalized."""
        return self._txn is not None and not self._finalized

    def commit(self) -> CommitStatus:
        """Commit the transaction and return the server-reported status."""
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to commit.")
        status = self._pac_client.commit_blocking(self._txn)
        self._finalized = True
        self._txn = None
        return status

    def abort(self) -> AbortStatus:
        """Abort the transaction and return the server-reported status."""
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to abort.")
        status = self._pac_client.abort_blocking(self._txn)
        self._finalized = True
        self._txn = None
        return status

    def rollback(self) -> AbortStatus:
        """Alias for :meth:`abort`."""
        return self.abort()

    def __enter__(self) -> SyncTransactionalSession:
        if self._txn is not None:
            raise RuntimeError("TransactionalSession is already active.")
        self._txn = Txn()
        self._finalized = False
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        if self._txn is None or self._finalized:
            return
        try:
            if exc_type is None:
                self.commit()
            else:
                self.abort()
        finally:
            self._finalized = True
            self._txn = None
