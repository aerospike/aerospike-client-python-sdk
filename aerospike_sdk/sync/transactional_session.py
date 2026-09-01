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

import typing

import types
from typing import Any, Optional, TYPE_CHECKING

from aerospike_async import AbortStatus, CommitStatus, Txn

from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.sync.session import Session
from aerospike_sdk.transactional_session_shared import TransactionalSessionBase

if TYPE_CHECKING:
    from aerospike_sdk.policy.behavior import Behavior
    from aerospike_sdk.sync.client import SyncClient


class TransactionalSession(TransactionalSessionBase, Session):
    """Sync context manager grouping operations into a multi-record transaction.

    Every session API (``query``, ``upsert``, ``insert``, ``batch``, ...)
    works unchanged inside ``with``; the active :class:`~aerospike_async.Txn`
    is threaded onto every policy the builders hand to the PAC.

    On clean exit the transaction commits; if an exception propagates out
    the transaction aborts. Explicit :meth:`commit`, :meth:`abort`, and
    :meth:`rollback` (alias for ``abort``) are available for manual control.

    Example::

        with client.create_session().transaction() as tx:
            tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
            tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

    See Also:
        :meth:`~aerospike_sdk.sync.session.Session.transaction`:
            Preferred construction entry.
    """

    def __init__(self, client: SyncClient, behavior: Behavior) -> None:
        """Construct via :meth:`~aerospike_sdk.sync.session.Session.transaction` rather than directly."""
        super().__init__(client, behavior)
        # txn / active come from TransactionalSessionBase.
        self._finalized = False

    def do_in_transaction(
        self,
        operation: "typing.Callable[[SyncTransactionalSession], typing.Any]",
        *,
        max_attempts: Optional[int] = None,
        sleep_between_retries: Optional[float] = None,
    ) -> Any:
        """Join this transaction instead of starting a nested one.

        A transaction cannot contain another transaction, so the useful
        reading of a nested call is "run this as part of the transaction I am
        already in". Opening a second one instead would silently split the
        caller's work across two transactions that commit independently --
        losing the atomicity the outer call was written to get.

        The outermost call owns the commit and the retrying, so the retry
        arguments do not apply here and are accepted only to keep the
        signature substitutable.

        Args:
            operation: Callable receiving this session.
            max_attempts: Ignored; the outermost call owns retrying.
            sleep_between_retries: Ignored, for the same reason.

        Returns:
            Whatever ``operation`` returns.

        Raises:
            RuntimeError: If this session's transaction is no longer active.

        Example::

            def transfer(tx):
                tx.upsert(src).put({"balance": 90}).execute()
                # Joins the transaction already in progress.
                return tx.do_in_transaction(audit)

            session.do_in_transaction(transfer)

        See Also:
            :meth:`aerospike_sdk.sync.session.SyncSession.do_in_transaction`:
                The outermost entry point, which does open a transaction.
        """
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to join.")
        return operation(self)

    def commit(self) -> CommitStatus:
        """Commit the transaction and return the server-reported status."""
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to commit.")
        try:
            status = self._pac_client.commit_blocking(self._txn)
        except Exception as e:
            self._finalized = True
            self._txn = None
            raise _convert_pac_exception(e) from e
        self._finalized = True
        self._txn = None
        return status

    def abort(self) -> AbortStatus:
        """Abort the transaction and return the server-reported status."""
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to abort.")
        try:
            status = self._pac_client.abort_blocking(self._txn)
        except Exception as e:
            self._finalized = True
            self._txn = None
            raise _convert_pac_exception(e) from e
        self._finalized = True
        self._txn = None
        return status

    def rollback(self) -> AbortStatus:
        """Alias for :meth:`abort`."""
        return self.abort()

    def __enter__(self) -> TransactionalSession:
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


# Path-differentiated bare name is the committed convention (same as the aio
# class); the ``Sync``-prefixed alias stays importable for one deprecation
# cycle (removed at GA).
SyncTransactionalSession = TransactionalSession
