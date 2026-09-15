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

"""Retry classification and commit-status guards for multi-record transactions."""


from aerospike_sdk import ResultCode
from aerospike_async import CommitErrorType
from aerospike_sdk.exceptions import AerospikeError, CommitError
from aerospike_sdk.txn_shared import (
    is_retryable_txn_error,
)


def test_verify_fail_commit_error_is_retryable() -> None:
    assert is_retryable_txn_error(CommitError("verify failed")) is True


def test_roll_forward_abandoned_error_type_is_not_retryable() -> None:
    err = CommitError(
        "abandoned",
        commit_error_type=CommitErrorType.ROLL_FORWARD_ABANDONED,
    )
    assert is_retryable_txn_error(err) is False


def test_mrt_blocked_is_retryable() -> None:
    err = AerospikeError("blocked", result_code=ResultCode.MRT_BLOCKED)
    assert is_retryable_txn_error(err) is True


def test_unrelated_error_is_not_retryable() -> None:
    assert is_retryable_txn_error(AerospikeError("nope")) is False


