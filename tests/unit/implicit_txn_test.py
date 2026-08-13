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

"""Implicit batch-write transaction gate + runner tests (no server needed)."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from aerospike_sdk import Key, Txn
from aerospike_async import BatchPolicy
from aerospike_async.exceptions import AerospikeError as PacAerospikeError
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from aerospike_sdk.implicit_txn import (
    implicit_txn_enabled,
    run_in_implicit_txn,
    run_in_implicit_txn_blocking,
    stamp_txn,
)
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.policy.system_settings import SystemSettings, TransactionSettings
from aerospike_sdk.sync.operations.query import SyncQueryBuilder


def _sdk_client(implicit=True):
    settings = fill_hard_defaults(SystemSettings(
        transactions=TransactionSettings(implicit_batch_write_transactions=implicit),
    ))
    return SimpleNamespace(
        _sdk_settings=settings,
        _cached_supports_server_compiled_ael=False,
        _cached_supports_query_selection=False,
    )


def _settings(attempts=None, sleep=timedelta(0)):
    return TransactionSettings(
        implicit_batch_write_transactions=True,
        number_of_attempts=attempts,
        sleep_between_attempts=sleep,
    )


class _MrtError(Exception):
    def __init__(self, result_code):
        super().__init__(f"code={result_code}")
        self.result_code = result_code


class _FakePacClient:
    """Records commit/abort calls for both runtimes."""

    def __init__(self, commit_error_codes=()):
        self.commits: list = []
        self.aborts: list = []
        self._commit_error_codes = list(commit_error_codes)

    def _commit(self, txn):
        if self._commit_error_codes:
            raise _MrtError(self._commit_error_codes.pop(0))
        self.commits.append(txn)

    def commit_blocking(self, txn):
        self._commit(txn)

    def abort_blocking(self, txn):
        self.aborts.append(txn)

    async def commit(self, txn):
        self._commit(txn)

    async def abort(self, txn):
        self.aborts.append(txn)


class TestGate:
    """implicit_txn_enabled truth table (cheap conditions only)."""

    def test_all_conditions_met(self):
        assert implicit_txn_enabled(_sdk_client(), None, Mode.SC) is True

    def test_explicit_txn_active(self):
        assert implicit_txn_enabled(_sdk_client(), Txn(), Mode.SC) is False

    def test_ap_namespace(self):
        assert implicit_txn_enabled(_sdk_client(), None, Mode.AP) is False

    def test_mode_unresolved(self):
        assert implicit_txn_enabled(_sdk_client(), None, None) is False

    def test_setting_disabled(self):
        assert implicit_txn_enabled(_sdk_client(implicit=False), None, Mode.SC) is False

    def test_no_sdk_client(self):
        assert implicit_txn_enabled(None, None, Mode.SC) is False


class TestStampTxn:

    def test_creates_policy_when_none(self):
        txn = Txn()
        policy = stamp_txn(None, txn)
        assert isinstance(policy, BatchPolicy)
        assert policy.txn is not None

    def test_stamps_existing_policy_in_place(self):
        policy = BatchPolicy()
        stamped = stamp_txn(policy, Txn())
        assert stamped is policy
        assert policy.txn is not None


class TestBlockingRunner:

    def test_commit_on_success(self):
        pac = _FakePacClient()
        result = run_in_implicit_txn_blocking(pac, _settings(), lambda txn: "ok")
        assert result == "ok"
        assert len(pac.commits) == 1
        assert pac.aborts == []

    def test_abort_and_raise_on_non_retryable(self):
        pac = _FakePacClient()

        def attempt(txn):
            raise _MrtError(ResultCode.PARAMETER_ERROR)

        with pytest.raises(_MrtError):
            run_in_implicit_txn_blocking(pac, _settings(), attempt)
        assert len(pac.aborts) == 1
        assert pac.commits == []

    def test_retryable_then_success(self):
        pac = _FakePacClient()
        calls: list = []

        def attempt(txn):
            calls.append(txn)
            if len(calls) == 1:
                raise _MrtError(ResultCode.MRT_BLOCKED)
            return "ok"

        assert run_in_implicit_txn_blocking(pac, _settings(), attempt) == "ok"
        assert len(calls) == 2
        # Each attempt gets a fresh transaction.
        assert calls[0] is not calls[1]
        assert len(pac.aborts) == 1
        assert len(pac.commits) == 1

    def test_attempts_exhausted(self):
        pac = _FakePacClient()

        def attempt(txn):
            raise _MrtError(ResultCode.MRT_VERSION_MISMATCH)

        with pytest.raises(_MrtError):
            run_in_implicit_txn_blocking(pac, _settings(attempts=3), attempt)
        assert len(pac.aborts) == 3
        assert pac.commits == []

    def test_commit_failure_is_retried(self):
        pac = _FakePacClient(commit_error_codes=[ResultCode.MRT_VERSION_MISMATCH])
        result = run_in_implicit_txn_blocking(pac, _settings(), lambda txn: "ok")
        assert result == "ok"
        assert len(pac.aborts) == 1
        assert len(pac.commits) == 1

    def test_abort_failure_does_not_mask_original_error(self):
        pac = _FakePacClient()

        def failing_abort(txn):
            raise RuntimeError("abort exploded")

        pac.abort_blocking = failing_abort

        def attempt(txn):
            raise _MrtError(ResultCode.PARAMETER_ERROR)

        with pytest.raises(_MrtError):
            run_in_implicit_txn_blocking(pac, _settings(), attempt)

    def test_defaults_when_settings_fields_unset(self):
        pac = _FakePacClient()
        raw = TransactionSettings(
            implicit_batch_write_transactions=True,
            sleep_between_attempts=timedelta(0),
        )
        attempts: list = []

        def attempt(txn):
            attempts.append(txn)
            raise _MrtError(ResultCode.MRT_BLOCKED)

        with pytest.raises(_MrtError):
            run_in_implicit_txn_blocking(pac, raw, attempt)
        assert len(attempts) == 5


class TestAsyncRunner:

    @pytest.mark.asyncio
    async def test_commit_on_success(self):
        pac = _FakePacClient()

        async def attempt(txn):
            return "ok"

        result = await run_in_implicit_txn(pac, _settings(), attempt)
        assert result == "ok"
        assert len(pac.commits) == 1
        assert pac.aborts == []

    @pytest.mark.asyncio
    async def test_retryable_then_success(self):
        pac = _FakePacClient()
        calls: list = []

        async def attempt(txn):
            calls.append(txn)
            if len(calls) == 1:
                raise _MrtError(ResultCode.MRT_BLOCKED)
            return "ok"

        assert await run_in_implicit_txn(pac, _settings(), attempt) == "ok"
        assert len(calls) == 2
        assert len(pac.aborts) == 1
        assert len(pac.commits) == 1

    @pytest.mark.asyncio
    async def test_abort_and_raise_on_non_retryable(self):
        pac = _FakePacClient()

        async def attempt(txn):
            raise _MrtError(ResultCode.PARAMETER_ERROR)

        with pytest.raises(_MrtError):
            await run_in_implicit_txn(pac, _settings(), attempt)
        assert len(pac.aborts) == 1
        assert pac.commits == []


class _RecordingBatchClient:
    """Fake PAC surface for driving the sync chain's blocking batch dispatch."""

    def __init__(self):
        self.batch_policies: list = []
        self.commits: list = []
        self.aborts: list = []

    def batch_operate_blocking(self, keys, ops_per_key, batch_policy=None, write_policy=None):
        self.batch_policies.append(batch_policy)
        return []

    def batch_read_blocking(self, keys, bins, batch_policy=None, read_policy=None):
        self.batch_policies.append(batch_policy)
        return []

    def batch_blocking(self, ops, batch_policy=None):
        self.batch_policies.append(batch_policy)
        return []

    def commit_blocking(self, txn):
        self.commits.append(txn)

    def abort_blocking(self, txn):
        self.aborts.append(txn)


def _write_chain_builder(pac, sdk_client, mode=Mode.SC, txn=None):
    return SyncQueryBuilder(
        client=pac,
        namespace="test",
        set_name="s",
        txn=txn,
        namespace_mode_resolver_blocking=lambda ns: mode,
        sdk_client=sdk_client,
    )


class TestMultiKeyWriteChainWrap:
    """End-to-end gate + wrap through the sync multi-key write chain."""

    def _sdk_client(self, implicit=True, supports_mrt=True):
        client = _sdk_client(implicit=implicit)
        client._supports_mrt_blocking = lambda: supports_mrt
        return client

    def _keys(self):
        return [Key("test", "s", 1), Key("test", "s", 2)]

    def test_sc_write_batch_is_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client())
        builder.upsert(self._keys()).bin("a").set_to(1).execute()
        assert len(pac.commits) == 1
        assert pac.batch_policies[0] is not None
        assert pac.batch_policies[0].txn is not None

    def test_ap_batch_is_not_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client(), mode=Mode.AP)
        builder.upsert(self._keys()).bin("a").set_to(1).execute()
        assert pac.commits == []
        assert pac.batch_policies[0] is None

    def test_explicit_txn_is_not_double_wrapped(self):
        pac = _RecordingBatchClient()
        explicit = Txn()
        builder = _write_chain_builder(pac, self._sdk_client(), txn=explicit)
        builder.upsert(self._keys()).bin("a").set_to(1).execute()
        # The explicit txn is stamped; no implicit commit happens.
        assert pac.commits == []
        assert pac.batch_policies[0].txn is not None

    def test_with_txn_none_opts_out(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client())
        builder.upsert(self._keys()).bin("a").set_to(1).with_txn(None).execute()
        assert pac.commits == []
        assert pac.batch_policies[0] is None

    def test_setting_disabled_is_not_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client(implicit=False))
        builder.upsert(self._keys()).bin("a").set_to(1).execute()
        assert pac.commits == []

    def test_cluster_without_mrt_is_not_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client(supports_mrt=False))
        builder.upsert(self._keys()).bin("a").set_to(1).execute()
        assert pac.commits == []

    def test_read_only_batch_is_not_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client())
        builder._keys = self._keys()
        builder.execute()
        assert pac.commits == []
        assert len(pac.batch_policies) == 1

    def test_multi_namespace_batch_is_not_wrapped(self):
        # A transaction cannot span namespaces. Wrapping is SDK-initiated, so
        # the batch goes through unwrapped and the server answers per key,
        # rather than the whole batch failing client-side.
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client())
        keys = [Key("test", "s", 1), Key("other", "s", 2)]
        builder.upsert(keys).bin("a").set_to(1).execute()
        assert pac.commits == []
        assert pac.batch_policies[0] is None

    def test_multi_namespace_write_chain_is_not_wrapped(self):
        pac = _RecordingBatchClient()
        builder = _write_chain_builder(pac, self._sdk_client())
        (
            builder.upsert(Key("test", "s", 1)).bin("a").set_to(1)
            .upsert(Key("other", "s", 2)).bin("a").set_to(2)
            .execute()
        )
        assert pac.commits == []
        assert pac.batch_policies[0] is None

    def test_failed_wrapped_batch_reports_every_row_as_failed(self):
        # The transaction aborted, so nothing was written and no row may
        # claim success. A client-side rejection carries no server result
        # code, so the rows read OK — the attached exception is the signal.
        pac = _RecordingBatchClient()

        def failing_batch(keys, ops_per_key, batch_policy=None, write_policy=None):
            raise PacAerospikeError("client rejected the command")

        pac.batch_operate_blocking = failing_batch
        builder = _write_chain_builder(pac, self._sdk_client())
        rows = list(builder.upsert(self._keys()).bin("a").set_to(1).execute())

        assert len(pac.aborts) == 1
        assert pac.commits == []
        assert len(rows) == 2
        assert not any(row.is_ok for row in rows)
        for row in rows:
            with pytest.raises(AerospikeError, match="client rejected"):
                row.or_raise()
