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

"""Mixed-mode batch resolution: per-row policies + parent SC escalation.

A batch may span namespaces whose consistency modes differ. Mode-scoped
settings (durable-delete defaults on SC) must resolve per key, and the
batch-level parent policy must resolve with SC-scoped settings whenever any
key lands in an SC namespace. These tests drive the sync chain against a
recording fake client — the resolution logic is shared with the async chain
via ``query_shared``. Order-independence is the core regression: resolving
from ``keys[0]`` alone applied one namespace's mode to every key.
"""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from aerospike_async import Key

import aerospike_sdk.query_shared as query_shared

from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, Settings
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.policy.system_settings import (
    SystemSettings,
    TransactionSettings,
)
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder
from aerospike_sdk.sync.operations.udf import UdfFunctionBuilder

AP_NS = "test"
SC_NS = "test_sc"


def _resolver(namespace: str) -> Mode:
    return Mode.SC if namespace == SC_NS else Mode.AP


def _sdk_client():
    settings = fill_hard_defaults(SystemSettings(
        transactions=TransactionSettings(implicit_batch_write_transactions=False),
    ))
    client = SimpleNamespace(
        _sdk_settings=settings,
        _cached_supports_server_compiled_ael=False,
        _cached_supports_query_selection=False,
    )
    client._supports_mrt_blocking = lambda: False
    return client


class _FakeBatchRecord:
    def __init__(self, key):
        self.key = key
        self.record = None
        self.result_code = 0
        self.in_doubt = False
        self.sub_code = None
        self.server_message = None
        self.exp_trace = None


class _CapturedDeleteOp:
    """Stand-in for the PAC ``BatchDeleteOp`` (native ops hide their fields)."""

    def __init__(self, key, policy=None):
        self.key = key
        self.policy = policy


class _CapturedWriteOp:
    """Stand-in for the PAC ``BatchWriteOp``."""

    def __init__(self, key, operations, policy=None):
        self.key = key
        self.operations = operations
        self.policy = policy


@pytest.fixture(autouse=True)
def _capture_batch_ops(monkeypatch):
    """Swap the native batch-op constructors for field-visible stand-ins."""
    monkeypatch.setattr(query_shared, "BatchDeleteOp", _CapturedDeleteOp)
    monkeypatch.setattr(query_shared, "BatchWriteOp", _CapturedWriteOp)


class _RecordingClient:
    """Fake PAC blocking surface recording batch dispatch shapes."""

    def __init__(self):
        self.batch_delete_calls: list = []
        self.batch_mixed_calls: list = []
        self.batch_operate_calls: list = []
        self.batch_apply_calls: list = []

    def batch_delete_blocking(self, keys, batch_policy=None, delete_policy=None):
        self.batch_delete_calls.append((list(keys), batch_policy, delete_policy))
        return [_FakeBatchRecord(k) for k in keys]

    def batch_operate_blocking(self, keys, ops_per_key, batch_policy=None, write_policy=None):
        self.batch_operate_calls.append((list(keys), batch_policy, write_policy))
        return [_FakeBatchRecord(k) for k in keys]

    def batch_blocking(self, ops, batch_policy=None):
        self.batch_mixed_calls.append((list(ops), batch_policy))
        return [_FakeBatchRecord(op.key) for op in ops]

    def batch_apply_blocking(
        self, keys, package, function, args, batch_policy=None, udf_policy=None,
    ):
        self.batch_apply_calls.append((list(keys), batch_policy, udf_policy))
        return [_FakeBatchRecord(k) for k in keys]


def _builder(pac, namespace=AP_NS, behavior=Behavior.DEFAULT):
    return SyncQueryBuilder(
        client=pac,
        namespace=namespace,
        set_name="s",
        behavior=behavior,
        namespace_mode_resolver_blocking=_resolver,
        sdk_client=_sdk_client(),
    )


def _k_ap(i: int = 1) -> Key:
    return Key(AP_NS, "s", i)


def _k_sc(i: int = 1) -> Key:
    return Key(SC_NS, "s", i)


class TestSingleNamespaceUnchanged:

    def test_sc_only_delete_stays_on_batch_delete_entry(self):
        pac = _RecordingClient()
        _builder(pac, namespace=SC_NS).delete(_k_sc(1), _k_sc(2)).execute()
        assert len(pac.batch_delete_calls) == 1
        assert pac.batch_mixed_calls == []
        _, _, bdp = pac.batch_delete_calls[0]
        assert bdp is not None and bdp.durable_delete is True

    def test_ap_only_delete_stays_on_batch_delete_entry(self):
        pac = _RecordingClient()
        _builder(pac, namespace=AP_NS).delete(_k_ap(1), _k_ap(2)).execute()
        assert len(pac.batch_delete_calls) == 1
        assert pac.batch_mixed_calls == []
        _, _, bdp = pac.batch_delete_calls[0]
        # No durable-delete default on AP and no other row settings.
        assert bdp is None or not bdp.durable_delete


class TestMixedModeDelete:

    def _rows(self, pac):
        assert pac.batch_delete_calls == []
        assert len(pac.batch_mixed_calls) == 1
        ops, _ = pac.batch_mixed_calls[0]
        return ops

    def test_ap_first_ordering_resolves_per_row(self):
        pac = _RecordingClient()
        _builder(pac, namespace=AP_NS).delete(_k_ap(), _k_sc()).execute()
        ops = self._rows(pac)
        assert [op.key.namespace for op in ops] == [AP_NS, SC_NS]
        assert all(isinstance(op, _CapturedDeleteOp) for op in ops)
        ap_policy, sc_policy = ops[0].policy, ops[1].policy
        assert sc_policy is not None and sc_policy.durable_delete is True
        assert ap_policy is None or not ap_policy.durable_delete

    def test_sc_first_ordering_resolves_per_row(self):
        pac = _RecordingClient()
        _builder(pac, namespace=SC_NS).delete(_k_sc(), _k_ap()).execute()
        ops = self._rows(pac)
        assert [op.key.namespace for op in ops] == [SC_NS, AP_NS]
        sc_policy, ap_policy = ops[0].policy, ops[1].policy
        assert sc_policy is not None and sc_policy.durable_delete is True
        assert ap_policy is None or not ap_policy.durable_delete


class TestMixedModeWriteWithRecordDelete:

    def test_record_delete_rows_scope_durable_delete_per_mode(self):
        pac = _RecordingClient()
        (
            _builder(pac, namespace=AP_NS)
            .upsert(_k_ap(), _k_sc())
            .delete_record()
            .execute()
        )
        assert pac.batch_operate_calls == []
        assert len(pac.batch_mixed_calls) == 1
        ops, _ = pac.batch_mixed_calls[0]
        assert all(isinstance(op, _CapturedWriteOp) for op in ops)
        by_ns = {op.key.namespace: op.policy for op in ops}
        assert by_ns[SC_NS] is not None and by_ns[SC_NS].durable_delete is True
        assert by_ns[AP_NS] is None or not by_ns[AP_NS].durable_delete

    def test_plain_mixed_write_keeps_single_policy_entry(self):
        # No record-delete op: nothing row-level is mode-scoped, so the
        # single-policy batch_operate entry remains correct (and fast).
        pac = _RecordingClient()
        (
            _builder(pac, namespace=AP_NS)
            .upsert(_k_ap(), _k_sc())
            .bin("a").set_to(1)
            .execute()
        )
        assert len(pac.batch_operate_calls) == 1
        assert pac.batch_mixed_calls == []


class TestParentPolicyEscalation:

    def _behavior(self):
        # Distinct batch-policy-visible knob per mode: only the SC scope
        # sets a total timeout, so an escalated parent policy is observable.
        return Behavior.DEFAULT.derive_with_changes(
            name="escalation-probe",
            writes_sc=Settings(total_timeout=timedelta(seconds=5)),
        )

    def test_mixed_batch_parent_policy_uses_sc_settings(self):
        pac = _RecordingClient()
        _builder(pac, namespace=AP_NS, behavior=self._behavior()).delete(
            _k_ap(), _k_sc()).execute()
        _, bp = pac.batch_mixed_calls[0]
        assert bp is not None and bp.total_timeout == 5000

    def test_ap_only_batch_parent_policy_stays_ap(self):
        pac = _RecordingClient()
        _builder(pac, namespace=AP_NS, behavior=self._behavior()).delete(
            _k_ap(1), _k_ap(2)).execute()
        _, _, bdp = pac.batch_delete_calls[0]
        # Parent policy comes back through the batch_delete entry.
        _, bp, _ = pac.batch_delete_calls[0]
        assert bp is None or bp.total_timeout != 5000


class TestMixedModeUdf:

    def test_udf_groups_by_mode_and_merges_in_request_order(self):
        pac = _RecordingClient()
        keys = [_k_ap(1), _k_sc(1), _k_ap(2), _k_sc(2)]
        # Mirror the session's execute_udf entry: multi-key builder with the
        # UDF op type, wrapped in the function builder.
        builder = _builder(pac, namespace=keys[0].namespace)
        builder._keys = list(keys)
        builder._op_type = "execute_udf"
        results = UdfFunctionBuilder(builder).function("pkg", "fn").execute()
        assert len(pac.batch_apply_calls) == 2
        by_mode = {}
        for call_keys, _, udf_policy in pac.batch_apply_calls:
            namespaces = {k.namespace for k in call_keys}
            assert len(namespaces) == 1  # one mode per call
            by_mode[namespaces.pop()] = udf_policy
        assert by_mode[SC_NS] is not None and by_mode[SC_NS].durable_delete is True
        assert by_mode[AP_NS] is None or not by_mode[AP_NS].durable_delete
        # Merged results come back in request order despite the split — the
        # fake returns the very Key objects it was handed, so identity pins it.
        assert [r.key for r in results] == keys
