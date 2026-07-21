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

"""Tests for the SDK config ``behaviors:`` section: parsing, apply, session push."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from aerospike_async import ReadModeAP, ReadModeSC, Replica

from aerospike_sdk.policy import behavior_registry
from aerospike_sdk.policy import sdk_config_loader as loader
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Scope
from aerospike_sdk.sync.session import SyncSession

_FULL = """
behaviors:
  high-performance:
    allOperations:
      abandonCallAfter: 1s
      waitForCallToComplete: 3s
      maximumNumberOfCallAttempts: 2
      delayBetweenRetries: 25ms
      replicaOrder: SEQUENCE
      sendKey: false
      useCompression: false
      resetTtlOnReadAtPercent: 50
    retryableWrites:
      useDurableDelete: false
      maximumNumberOfCallAttempts: 3
    nonRetryableWrites:
      maximumNumberOfCallAttempts: 1
    consistencyModeReads:
      readConsistency: LINEARIZE
    availabilityModeReads:
      migrationReadConsistency: ONE
    batchReads:
      maxConcurrentServers: 8
      allowInlineMemoryAccess: true
      allowInlineSsdAccess: false
    batchWrites:
      maxConcurrentServers: 6
    query:
      recordQueueSize: 5000
"""


@pytest.fixture(autouse=True)
def _restore_behavior_state():
    """apply_behaviors mutates process-global state; snapshot and restore it."""
    saved_registry = dict(behavior_registry._registry)
    saved_default_patches = dict(Behavior.DEFAULT._patches)
    saved_applied = dict(loader._last_applied_behaviors)
    yield
    loader._last_applied_behaviors.clear()
    loader._last_applied_behaviors.update(saved_applied)
    Behavior.DEFAULT._reload_patches(saved_default_patches)
    behavior_registry._registry.clear()
    behavior_registry._registry.update(saved_registry)


class TestParseBehaviors:
    """Selector blocks map to scopes; fields convert with fail-soft skipping."""

    def test_selector_blocks_map_to_scopes(self):
        spec = loader.parse_behaviors(_FULL)["high-performance"]
        assert set(spec.patches) == {
            Scope.ALL, Scope.WRITES_RETRYABLE, Scope.WRITES_NON_RETRYABLE,
            Scope.READS_SC, Scope.READS_AP, Scope.READS_BATCH,
            Scope.WRITES_BATCH, Scope.READS_QUERY,
        }

    def test_common_fields(self):
        all_scope = loader.parse_behaviors(_FULL)["high-performance"].patches[Scope.ALL]
        assert all_scope.total_timeout == timedelta(seconds=1)
        assert all_scope.socket_timeout == timedelta(seconds=3)
        assert all_scope.retry_delay == timedelta(milliseconds=25)
        assert all_scope.replica == Replica.SEQUENCE
        assert all_scope.send_key is False
        assert all_scope.use_compression is False
        assert all_scope.read_touch_ttl_percent == 50

    def test_attempts_count_the_initial_call(self):
        spec = loader.parse_behaviors(_FULL)["high-performance"]
        assert spec.patches[Scope.ALL].max_retries == 1
        assert spec.patches[Scope.WRITES_RETRYABLE].max_retries == 2
        assert spec.patches[Scope.WRITES_NON_RETRYABLE].max_retries == 0

    def test_mode_specific_enums(self):
        spec = loader.parse_behaviors(_FULL)["high-performance"]
        assert spec.patches[Scope.READS_SC].read_mode_sc == ReadModeSC.LINEARIZE
        assert spec.patches[Scope.READS_AP].read_mode_ap == ReadModeAP.ONE

    def test_batch_and_query_fields(self):
        spec = loader.parse_behaviors(_FULL)["high-performance"]
        assert spec.patches[Scope.READS_BATCH].max_concurrent_nodes == 8
        assert spec.patches[Scope.READS_BATCH].allow_inline is True
        assert spec.patches[Scope.READS_BATCH].allow_inline_ssd is False
        assert spec.patches[Scope.WRITES_BATCH].max_concurrent_nodes == 6
        assert spec.patches[Scope.READS_QUERY].record_queue_size == 5000

    def test_parent_captured(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  child:\n    parent: high-performance\n"
            "    query:\n      recordQueueSize: 100\n"
        )
        assert specs["child"].parent == "high-performance"

    def test_unsupported_block_ignored(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  b:\n    systemTxnVerify:\n      abandonCallAfter: 2s\n"
            "    query:\n      recordQueueSize: 9\n"
        )
        assert set(specs["b"].patches) == {Scope.READS_QUERY}

    def test_unmapped_field_ignored(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  b:\n    allOperations:\n"
            "      waitForConnectionToComplete: 500ms\n"
            "      abandonCallAfter: 2s\n"
        )
        all_scope = specs["b"].patches[Scope.ALL]
        assert all_scope.total_timeout == timedelta(seconds=2)

    def test_bad_enum_value_skipped_rest_applies(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  b:\n    allOperations:\n"
            "      replicaOrder: NOT_A_REPLICA\n"
            "      sendKey: true\n"
        )
        all_scope = specs["b"].patches[Scope.ALL]
        assert all_scope.replica is None
        assert all_scope.send_key is True

    def test_attempts_below_one_skipped(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  b:\n    allOperations:\n"
            "      maximumNumberOfCallAttempts: 0\n      sendKey: true\n"
        )
        all_scope = specs["b"].patches[Scope.ALL]
        assert all_scope.max_retries is None
        assert all_scope.send_key is True

    def test_no_behaviors_section(self):
        assert loader.parse_behaviors('version: "1.0.0"\n') == {}


class TestApplyBehaviors:
    """Registry creation, in-place reload, inheritance, and change gating."""

    def test_new_behavior_registered_with_default_parent(self):
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        registered = behavior_registry.get_behavior("high-performance")
        assert registered is not None
        assert registered.parent is Behavior.DEFAULT
        settings = registered.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert settings.max_retries == 2
        assert settings.total_timeout == timedelta(seconds=1)

    def test_forward_declared_parent_resolves(self):
        specs = loader.parse_behaviors(
            "behaviors:\n"
            "  child:\n    parent: base\n    query:\n      recordQueueSize: 111\n"
            "  base:\n    allOperations:\n      abandonCallAfter: 7s\n"
        )
        loader.apply_behaviors(specs)
        child = behavior_registry.get_behavior("child")
        assert child.parent is behavior_registry.get_behavior("base")
        settings = child.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
        assert settings.record_queue_size == 111
        assert settings.total_timeout == timedelta(seconds=7)

    def test_unknown_parent_falls_back_to_default(self):
        loader.apply_behaviors(loader.parse_behaviors(
            "behaviors:\n  b:\n    parent: no-such-behavior\n"
            "    query:\n      recordQueueSize: 5\n"
        ))
        assert behavior_registry.get_behavior("b").parent is Behavior.DEFAULT

    def test_in_place_reload_keeps_identity_and_cascades(self):
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        parent = behavior_registry.get_behavior("high-performance")
        child = parent.derive_with_changes("hp-child")
        loader.apply_behaviors(loader.parse_behaviors(
            _FULL.replace("abandonCallAfter: 1s", "abandonCallAfter: 9s")
        ))
        assert behavior_registry.get_behavior("high-performance") is parent
        assert parent.get_settings(
            OpKind.READ, OpShape.POINT, Mode.AP).total_timeout == timedelta(seconds=9)
        assert child.get_settings(
            OpKind.READ, OpShape.POINT, Mode.AP).total_timeout == timedelta(seconds=9)

    def test_parent_change_replaces_registration(self):
        loader.apply_behaviors(loader.parse_behaviors(
            "behaviors:\n"
            "  base:\n    allOperations:\n      abandonCallAfter: 7s\n"
            "  b:\n    query:\n      recordQueueSize: 5\n"
        ))
        original = behavior_registry.get_behavior("b")
        loader.apply_behaviors(loader.parse_behaviors(
            "behaviors:\n"
            "  base:\n    allOperations:\n      abandonCallAfter: 7s\n"
            "  b:\n    parent: base\n    query:\n      recordQueueSize: 5\n"
        ))
        replacement = behavior_registry.get_behavior("b")
        assert replacement is not original
        assert replacement.parent is behavior_registry.get_behavior("base")

    def test_default_entry_layers_on_factory_patches(self):
        loader.apply_behaviors(loader.parse_behaviors(
            "behaviors:\n  DEFAULT:\n    allOperations:\n      abandonCallAfter: 42s\n"
        ))
        settings = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert settings.total_timeout == timedelta(seconds=42)
        assert settings.send_key is True

    def test_unchanged_spec_is_skipped(self):
        specs = loader.parse_behaviors(_FULL)
        loader.apply_behaviors(specs)
        registered = behavior_registry.get_behavior("high-performance")
        resolved_before = registered._resolved
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        assert registered._resolved is resolved_before

    def test_parent_cycle_broken_with_warning(self):
        loader.apply_behaviors(loader.parse_behaviors(
            "behaviors:\n"
            "  a:\n    parent: b\n    query:\n      recordQueueSize: 1\n"
            "  b:\n    parent: a\n    query:\n      recordQueueSize: 2\n"
        ))
        assert behavior_registry.get_behavior("a") is not None
        assert behavior_registry.get_behavior("b") is not None


class TestSessionPush:
    """Live sessions get rebuilt cached policies when their behavior reloads."""

    def _offline_session(self, behavior):
        return SyncSession(client=MagicMock(), behavior=behavior)

    def test_reload_pushes_policies_into_live_session(self):
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        behavior = behavior_registry.get_behavior("high-performance")
        session = self._offline_session(behavior)
        assert session._cached_read_policy.max_retries == 1

        loader.apply_behaviors(loader.parse_behaviors(
            _FULL.replace(
                "maximumNumberOfCallAttempts: 2", "maximumNumberOfCallAttempts: 6",
            )
        ))
        assert session._cached_read_policy.max_retries == 5

    def test_child_behavior_session_updates_on_parent_reload(self):
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        parent = behavior_registry.get_behavior("high-performance")
        child = parent.derive_with_changes("hp-session-child")
        session = self._offline_session(child)
        assert session._cached_read_policy.total_timeout == 1_000

        loader.apply_behaviors(loader.parse_behaviors(
            _FULL.replace("abandonCallAfter: 1s", "abandonCallAfter: 9s")
        ))
        assert session._cached_read_policy.total_timeout == 9_000

    def test_dead_session_is_not_retained(self):
        loader.apply_behaviors(loader.parse_behaviors(_FULL))
        behavior = behavior_registry.get_behavior("high-performance")
        session = self._offline_session(behavior)
        assert len(behavior._sessions) == 1
        del session
        import gc
        gc.collect()
        assert len(behavior._sessions) == 0
