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

from typing import Any, List, Optional, Union

from aerospike_native import Key, ResultCode, Txn

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import TransactionError
from aerospike_sdk.txn_status import TxnStatus


class TransactionalSessionBase:
    """Mixin holding the transaction-state view shared by both trees.

    Mixed in *before* each tree's ``Session`` leaf, so a transactional session
    is still a full session (writes, queries, batches, ...) while the
    transaction-lifecycle *view* — :attr:`txn` and :attr:`active` — is defined
    exactly once. The lifecycle *terminals* (``commit`` / ``abort`` and the
    context-manager protocol) stay per-leaf because they differ by runtime
    (async ``await`` vs blocking) and by the PNC entry they call.
    """

    # Set by the leaf ``Session.__init__`` (``_txn``, initially ``None``) and by
    # the transactional leaf's ``__init__`` (``_finalized``). Declared here so
    # the shared view can read them without the type-checker flagging a missing
    # attribute.
    _txn: Optional[Txn]
    _finalized: bool
    # Set alongside ``_finalized`` by the leaf terminals: the code a later
    # operation is refused with, so the refusal says how the transaction
    # ended.
    _final_code: ResultCode = ResultCode.TXN_ALREADY_ABORTED

    @property
    def txn(self) -> Txn:
        """Return the active :class:`~aerospike_native.Txn`.

        Raises:
            RuntimeError: If the session has not been entered (no active txn).

        Returns:
            The active :class:`~aerospike_native.Txn`.

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

    # -- Finalized-session guard ---------------------------------------------
    # Once committed or aborted the session holds no transaction, and the
    # plain-session paths would run a later operation transaction-free. Every
    # operation entry point checks here first so the write cannot escape.

    def _require_open(self) -> None:
        """Refuse an operation on a finalized session.

        Raises:
            TransactionError: With ``ResultCode.TXN_ALREADY_COMMITTED`` or
                ``ResultCode.TXN_ALREADY_ABORTED`` after :meth:`commit` or
                :meth:`abort` (explicit or on block exit).
        """
        if self._finalized:
            raise self._finalized_error()

    def _finalized_error(self) -> TransactionError:
        committed = self._final_code is ResultCode.TXN_ALREADY_COMMITTED
        return TransactionError(
            f"Transaction already {'committed' if committed else 'aborted'}; "
            "start a new transaction for further operations",
            result_code=self._final_code,
        )

    def _commit_after_finalize(self) -> TxnStatus:
        """Answer a second ``commit()``: a repeat is a no-op status, a commit
        after an abort is an error."""
        if self._final_code is ResultCode.TXN_ALREADY_COMMITTED:
            return TxnStatus.ALREADY_COMMITTED
        raise self._finalized_error()

    def _abort_after_finalize(self) -> TxnStatus:
        """Answer a second ``abort()``: a repeat is a no-op status, an abort
        after a commit is an error."""
        if self._final_code is ResultCode.TXN_ALREADY_COMMITTED:
            raise self._finalized_error()
        return TxnStatus.ALREADY_ABORTED

    def _fast_write_segment(self, op_type: str, key: Key) -> Any:
        self._require_open()
        return super()._fast_write_segment(op_type, key)  # type: ignore[misc]

    def _dataset_write_builder(self, op_type: str, dataset: DataSet) -> Any:
        self._require_open()
        return super()._dataset_write_builder(op_type, dataset)  # type: ignore[misc]

    def _build_write_segment(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> Any:
        self._require_open()
        return super()._build_write_segment(op_type, arg1, *more_keys)  # type: ignore[misc]

    def _fast_query_builder(self, key: Key) -> Any:
        self._require_open()
        return super()._fast_query_builder(key)  # type: ignore[misc]

    def _build_query_builder(
        self, *, dataset: Optional[DataSet], keys: Optional[List[Key]],
    ) -> Any:
        self._require_open()
        return super()._build_query_builder(dataset=dataset, keys=keys)  # type: ignore[misc]

    def execute_udf(self, *keys: Key) -> Any:
        self._require_open()
        return super().execute_udf(*keys)  # type: ignore[misc]
