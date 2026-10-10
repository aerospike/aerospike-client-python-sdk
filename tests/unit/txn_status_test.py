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

"""Mapping of PNC commit and abort statuses onto :class:`TxnStatus`."""

import pytest

from aerospike_native import AbortStatus, CommitStatus

from aerospike_sdk import TxnStatus
from aerospike_sdk.txn_status import _FROM_ABORT, _FROM_COMMIT, _from_abort, _from_commit


def _members(enum_type):
    return [getattr(enum_type, name) for name in dir(enum_type) if name.isupper()]


@pytest.mark.parametrize(
    ("pnc_status", "expected"),
    [
        (CommitStatus.OK, TxnStatus.COMMITTED),
        (CommitStatus.ALREADY_COMMITTED, TxnStatus.ALREADY_COMMITTED),
        (CommitStatus.CLOSE_ABANDONED, TxnStatus.ROLL_FORWARD_CLOSE_ABANDONED),
    ],
)
def test_commit_status_maps(pnc_status, expected):
    assert _from_commit(pnc_status) is expected


@pytest.mark.parametrize(
    ("pnc_status", "expected"),
    [
        (AbortStatus.OK, TxnStatus.ABORTED),
        (AbortStatus.ALREADY_ABORTED, TxnStatus.ALREADY_ABORTED),
        (AbortStatus.ROLL_BACK_ABANDONED, TxnStatus.ROLL_BACK_ABANDONED),
        (AbortStatus.CLOSE_ABANDONED, TxnStatus.ROLL_BACK_CLOSE_ABANDONED),
    ],
)
def test_abort_status_maps(pnc_status, expected):
    assert _from_abort(pnc_status) is expected


def test_every_returnable_pnc_status_is_mapped():
    """A status PNC adds later must fail here, not as a ``KeyError`` mid-commit."""
    raised_not_returned = {CommitStatus.ROLL_FORWARD_ABANDONED, AbortStatus.COMMIT_FAILED}
    for status in _members(CommitStatus):
        if status not in raised_not_returned:
            _from_commit(status)
    for status in _members(AbortStatus):
        if status not in raised_not_returned:
            _from_abort(status)


def test_every_txn_status_is_reachable():
    assert set(_FROM_COMMIT.values()) | set(_FROM_ABORT.values()) == set(TxnStatus)
