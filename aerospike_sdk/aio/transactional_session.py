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

"""TransactionalSession - Session for multi-record transactional operations (MRT)."""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from aerospike_async import AbortStatus, CommitStatus, Txn

from aerospike_sdk.aio.session import Session
from aerospike_sdk.transactional_session_shared import TransactionalSessionBase

if TYPE_CHECKING:
    from aerospike_sdk.aio.client import Client
    from aerospike_sdk.policy.behavior import Behavior


class TransactionalSession(TransactionalSessionBase, Session):
    """Async context manager that groups operations into a multi-record transaction.

    Subclasses :class:`~aerospike_sdk.aio.session.Session`, so every session
    API (``query``, ``upsert``, ``insert``, ``batch``, ...) works unchanged
    inside ``async with``; builders capture the active
    :class:`~aerospike_async.Txn` via
    :meth:`~aerospike_sdk.aio.session.Session.get_current_transaction` and
    thread it onto every policy they hand to the PAC — the user never
    touches a policy.

    On clean exit the transaction is committed; if an exception propagates
    out of the block the transaction is aborted. Explicit :meth:`commit`,
    :meth:`abort`, and :meth:`rollback` (alias for ``abort``) are also
    available for manual control.

    Example::

        async with client.create_session().transaction() as tx:
            await tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
            await tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()
        # Auto-committed on clean exit; auto-aborted on exception.

    See Also:
        :meth:`aerospike_sdk.aio.session.Session.transaction`
        :meth:`aerospike_sdk.aio.client.Client.transaction`
    """

    def __init__(
        self,
        client: "Client",
        behavior: Optional["Behavior"] = None,
    ) -> None:
        """Create a transactional session; prefer :meth:`Session.transaction`.

        Args:
            client: Connected :class:`~aerospike_sdk.aio.client.Client`.
            behavior: Policy bundle for operations started from this
                session. Defaults to :attr:`Behavior.DEFAULT` when omitted.

        Note:
            Application code should not construct ``TransactionalSession``
            directly; call :meth:`Session.transaction` or
            :meth:`Client.transaction` instead.

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.transaction`
        """
        if behavior is None:
            from aerospike_sdk.policy.behavior import Behavior as _Behavior
            behavior = _Behavior.DEFAULT
        super().__init__(client, behavior)
        # _txn is inherited from Session (initially None); __aenter__ sets it.
        # txn / active come from TransactionalSessionBase.
        self._finalized = False

    async def commit(self) -> CommitStatus:
        """Commit the transaction and return the server-reported status.

        Raises:
            RuntimeError: If the session has no active transaction.

        Returns:
            :class:`~aerospike_async.CommitStatus` reported by the server.

        See Also:
            :meth:`abort`: Undo the transaction instead of committing.
        """
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to commit.")
        status = await self._client._async_client.commit(self._txn)
        self._finalized = True
        # Drop the txn reference so operations issued after an explicit
        # commit run transaction-free instead of stamping the finalized
        # txn on their policies (mirrors the sync session and __aexit__).
        self._txn = None
        return status

    async def abort(self) -> AbortStatus:
        """Abort the transaction and return the server-reported status.

        Raises:
            RuntimeError: If the session has no active transaction.

        Returns:
            :class:`~aerospike_async.AbortStatus` reported by the server.

        See Also:
            :meth:`commit`: Persist the transaction instead of aborting.
            :meth:`rollback`: Alias for this method.
        """
        if self._txn is None or self._finalized:
            raise RuntimeError("No active transaction to abort.")
        status = await self._client._async_client.abort(self._txn)
        self._finalized = True
        self._txn = None
        return status

    async def rollback(self) -> AbortStatus:
        """Alias for :meth:`abort`.

        Returns:
            :class:`~aerospike_async.AbortStatus` reported by the server.
        """
        return await self.abort()

    async def __aenter__(self) -> "TransactionalSession":
        if self._txn is not None:
            raise RuntimeError("TransactionalSession is already active.")
        self._txn = Txn()
        self._finalized = False
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        if self._txn is None or self._finalized:
            return
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.abort()
        finally:
            self._finalized = True
            # Drop the txn reference so builders created after exit don't
            # accidentally participate in a finalized transaction.
            self._txn = None
