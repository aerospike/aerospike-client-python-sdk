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

"""Runtime-agnostic transactional-session state shared by both trees."""

from __future__ import annotations

from typing import Optional

from aerospike_async import Txn


class TransactionalSessionBase:
    """Mixin holding the transaction-state view shared by both trees.

    Mixed in *before* each tree's ``Session`` leaf, so a transactional session
    is still a full session (writes, queries, batches, ...) while the
    transaction-lifecycle *view* — :attr:`txn` and :attr:`active` — is defined
    exactly once. The lifecycle *terminals* (``commit`` / ``abort`` /
    ``rollback`` and the context-manager protocol) stay per-leaf because they
    differ by runtime (async ``await`` vs blocking) and by the PAC entry they
    call.
    """

    # Set by the leaf ``Session.__init__`` (``_txn``, initially ``None``) and by
    # the transactional leaf's ``__init__`` (``_finalized``). Declared here so
    # the shared view can read them without the type-checker flagging a missing
    # attribute.
    _txn: Optional[Txn]
    _finalized: bool

    @property
    def txn(self) -> Txn:
        """Return the active :class:`~aerospike_async.Txn`.

        Raises:
            RuntimeError: If the session has not been entered (no active txn).

        Returns:
            The active :class:`~aerospike_async.Txn`.

        Example::

            with session.transaction() as tx:
                assert tx.txn is not None
        """
        if self._txn is None:
            raise RuntimeError(
                "TransactionalSession is not active; enter the transaction "
                "block before accessing .txn."
            )
        return self._txn

    @property
    def active(self) -> bool:
        """``True`` when a transaction has been started and not yet finalized.

        Returns:
            Whether a transaction is currently active on this session.

        Example::

            with session.transaction() as tx:
                assert tx.active
        """
        return self._txn is not None and not self._finalized
