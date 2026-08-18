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

"""Batch write/delete/UDF policy building from behavior + chain.

Unit coverage for how the sync chain assembles per-row ``BatchWritePolicy`` /
``BatchDeletePolicy`` / ``BatchUDFPolicy`` objects against a recording fake
client; the assembly logic is shared with the async chain via ``query_shared``.

Two themes:

* **Mixed-mode resolution** — a batch may span namespaces whose consistency
  modes differ. Mode-scoped settings (durable-delete defaults on SC) resolve
  per key, and the batch-level parent policy escalates to SC-scoped settings
  whenever any key lands in an SC namespace. Order-independence is the core
  regression: resolving from ``keys[0]`` alone applied one namespace's mode to
  every key.
* **Fold vs sequential dispatch** — a key spanning segments forces per-segment
  execution; disjoint chains keep the single-round-trip fold.
* **Sub-policy field flow** — commit level (from behavior), the generation
  policy (from an expected generation), and the record expiration (from the
  chain's TTL verbs) each thread onto the per-row policy. Commit level has no
  observable single-node wire effect, so it is pinned here at the policy-shape
  level; generation and TTL effects are additionally exercised end-to-end in
  the integration suites.
"""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from aerospike_async import (
    CommitLevel,
    Expiration,
    GenerationPolicy,
    Key,
)

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
        supports_server_compiled_ael=False,
        supports_query_selection=False,
        _sdk_settings=settings,
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


def _commit_master():
    """Behavior whose batch writes resolve to a non-default COMMIT_MASTER."""
    return Behavior.DEFAULT.derive_with_changes(
        "commit_master_batch",
        writes_batch=Settings(commit_level=CommitLevel.COMMIT_MASTER),
    )


def _run_udf_apply(pac, apply_verbs, behavior=Behavior.DEFAULT):
    """Drive a two-key AP UDF apply; return the captured ``udf_policy``.

    *apply_verbs* receives the ``UdfBuilder`` to chain TTL/other verbs onto.
    """
    builder = _builder(pac, namespace=AP_NS, behavior=behavior)
    builder._keys = [_k_ap(1), _k_ap(2)]
    builder._op_type = "execute_udf"
    ub = UdfFunctionBuilder(builder).function("m", "f")
    apply_verbs(ub)
    ub.execute()
    return pac.batch_apply_calls[0][2]  # single AP mode -> one apply call


class TestCommitLevel:
    """Non-default behavior commit level threads onto each batch sub-policy."""

    def test_batch_write_carries_non_default_commit_level(self):
        pac = _RecordingClient()
        (
            _builder(pac, behavior=_commit_master())
            .insert(_k_ap(1)).put({"b": 1})
            .insert(_k_ap(2)).put({"b": 2})
            .execute()
        )
        ops, _ = pac.batch_mixed_calls[0]
        assert ops[0].policy.commit_level == CommitLevel.COMMIT_MASTER

    def test_batch_delete_carries_non_default_commit_level(self):
        pac = _RecordingClient()
        _builder(pac, behavior=_commit_master()).delete(_k_ap(1), _k_ap(2)).execute()
        _, _, bdp = pac.batch_delete_calls[0]
        assert bdp is not None and bdp.commit_level == CommitLevel.COMMIT_MASTER

    def test_batch_udf_carries_non_default_commit_level(self):
        pac = _RecordingClient()
        up = _run_udf_apply(pac, lambda ub: None, behavior=_commit_master())
        assert up is not None and up.commit_level == CommitLevel.COMMIT_MASTER

    def test_default_commit_level_keeps_no_policy_fast_path(self):
        # AP default resolves COMMIT_ALL, which equals core's own default, so a
        # plain batch delete needs no policy object at all.
        pac = _RecordingClient()
        _builder(pac, namespace=AP_NS).delete(_k_ap(1), _k_ap(2)).execute()
        _, _, bdp = pac.batch_delete_calls[0]
        assert bdp is None


class TestGenerationPolicy:
    """An expected generation sets ``generation_policy`` on write + delete."""

    def test_batch_delete_with_expected_generation(self):
        pac = _RecordingClient()
        _builder(pac).delete(_k_ap(1), _k_ap(2)).ensure_generation_is(7).execute()
        _, _, bdp = pac.batch_delete_calls[0]
        assert bdp.generation_policy == GenerationPolicy.EXPECT_GEN_EQUAL
        assert bdp.generation == 7

    def test_batch_write_with_expected_generation(self):
        pac = _RecordingClient()
        (
            _builder(pac)
            .update(_k_ap(1)).put({"b": 1}).ensure_generation_is(3)
            .update(_k_ap(2)).put({"b": 2}).ensure_generation_is(3)
            .execute()
        )
        ops, _ = pac.batch_mixed_calls[0]
        assert ops[0].policy.generation_policy == GenerationPolicy.EXPECT_GEN_EQUAL
        assert ops[0].policy.generation == 3

    def test_batch_delete_without_generation_leaves_policy_gen_none(self):
        pac = _RecordingClient()
        _builder(pac).delete(_k_ap(1), _k_ap(2)).with_durable_delete().execute()
        _, _, bdp = pac.batch_delete_calls[0]
        assert bdp.generation_policy == GenerationPolicy.NONE


class TestBatchUdfExpiration:
    """The chain's TTL verbs reach ``BatchUDFPolicy.expiration``."""

    def test_seconds_ttl_reaches_udf_policy(self):
        pac = _RecordingClient()
        up = _run_udf_apply(pac, lambda ub: ub.expire_record_after_seconds(600))
        assert up.expiration == Expiration.seconds(600)

    def test_never_expire_reaches_udf_policy(self):
        pac = _RecordingClient()
        up = _run_udf_apply(pac, lambda ub: ub.never_expire())
        assert up.expiration == Expiration.NEVER_EXPIRE

    def test_no_ttl_keeps_no_policy_fast_path(self):
        pac = _RecordingClient()
        up = _run_udf_apply(pac, lambda ub: None)
        assert up is None


class TestSameKeyChainFolding:
    """A key spanning segments forces sequential dispatch; disjoint chains fold.

    Batch sub-transactions against one key are unordered server-side, so folding
    an overlapping chain lets a later segment race an earlier one. The fold is
    what keeps the common (disjoint) chain a single round trip, so both
    directions are pinned.
    """

    def test_overlapping_key_runs_segments_sequentially(self):
        pac = _RecordingClient()
        (
            _builder(pac)
            .upsert(_k_ap(1), _k_ap(2)).bin("a").set_to(1)
            .upsert(_k_ap(2), _k_ap(3)).bin("a").set_to(2)
            .execute()
        )
        # k2 spans both segments: no combined batch, one dispatch per segment.
        assert pac.batch_mixed_calls == []
        assert len(pac.batch_operate_calls) == 2

    def test_disjoint_keys_keep_the_single_batch_fold(self):
        pac = _RecordingClient()
        (
            _builder(pac)
            .upsert(_k_ap(1), _k_ap(2)).bin("a").set_to(1)
            .upsert(_k_ap(3), _k_ap(4)).bin("a").set_to(2)
            .execute()
        )
        # No shared key: both segments fold into one round trip.
        assert len(pac.batch_mixed_calls) == 1
        assert pac.batch_operate_calls == []

    def test_single_segment_never_pays_the_overlap_scan(self):
        # The common high-volume shape is one segment; the check short-circuits
        # before touching any key.
        builder = _builder(_RecordingClient())
        builder.upsert(_k_ap(1), _k_ap(2)).bin("a").set_to(1)
        builder._finalize_current_spec()
        assert builder._specs_overlap_on_a_key() is False
