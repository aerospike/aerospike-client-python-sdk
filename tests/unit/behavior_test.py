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

"""Tests for Behavior model: Settings merge, scope resolution, pre-defined behaviors."""

from datetime import timedelta

import pytest
from aerospike_async import CommitLevel, ReadModeAP, ReadModeSC, Replica

from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_registry import (
    get_all_behaviors,
    get_behavior,
    get_behavior_or_default,
)
from aerospike_sdk.policy.behavior_settings import (
    Mode,
    OpKind,
    OpShape,
    Scope,
    Settings,
    resolution_order,
)


class TestSettingsMerge:
    """Verify Settings.merge() picks non-None override values while
    preserving base values for fields the override leaves unset."""
    def test_override_wins(self):
        base = Settings(total_timeout=timedelta(seconds=10), max_retries=2)
        override = Settings(total_timeout=timedelta(seconds=5))
        merged = Settings.merge(base, override)
        assert merged.total_timeout == timedelta(seconds=5)
        assert merged.max_retries == 2

    def test_none_override_keeps_base(self):
        base = Settings(send_key=True, replica=Replica.SEQUENCE)
        override = Settings()
        merged = Settings.merge(base, override)
        assert merged.send_key is True
        assert merged.replica == Replica.SEQUENCE

    def test_both_none_stays_none(self):
        merged = Settings.merge(Settings(), Settings())
        assert merged.total_timeout is None
        assert merged.max_retries is None

    def test_compression_threshold_merges(self):
        # Override takes precedence; base value carries through when
        # override leaves the field unset.
        base = Settings(compression_threshold=128)
        override = Settings(compression_threshold=4096)
        merged = Settings.merge(base, override)
        assert merged.compression_threshold == 4096

        merged_keep_base = Settings.merge(base, Settings())
        assert merged_keep_base.compression_threshold == 128


class TestResolutionOrder:
    """Verify that resolution_order() returns the correct scope chain
    (least-specific to most-specific) for each OpKind/OpShape/Mode combo.
    This chain determines how patches layer during get_settings()."""
    def test_read_point_ap(self):
        order = resolution_order(OpKind.READ, OpShape.POINT, Mode.AP)
        assert order == (Scope.ALL, Scope.READS, Scope.READS_AP, Scope.READS_POINT)

    def test_read_batch_sc(self):
        order = resolution_order(OpKind.READ, OpShape.BATCH, Mode.SC)
        assert order == (Scope.ALL, Scope.READS, Scope.READS_SC, Scope.READS_BATCH)

    def test_write_retryable_point_ap(self):
        order = resolution_order(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert order == (
            Scope.ALL,
            Scope.WRITES,
            Scope.WRITES_AP,
            Scope.WRITES_RETRYABLE,
            Scope.WRITES_POINT,
        )

    def test_write_non_retryable_query_sc(self):
        order = resolution_order(OpKind.WRITE_NON_RETRYABLE, OpShape.QUERY, Mode.SC)
        assert order == (
            Scope.ALL,
            Scope.WRITES,
            Scope.WRITES_SC,
            Scope.WRITES_NON_RETRYABLE,
            Scope.WRITES_QUERY,
        )


class TestBehaviorGetSettings:
    """Verify Behavior.DEFAULT resolves correct values for each operation
    type, including scope-specific overrides (e.g. query retries, write
    commit level) and that every OpKind/OpShape/Mode combo produces
    non-None settings."""
    def test_default_read_point_has_all_scope_values(self):
        s = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT)
        assert s.total_timeout == timedelta(seconds=30)
        assert s.socket_timeout == timedelta(seconds=5)
        assert s.max_retries == 2
        assert s.retry_delay == timedelta(0)
        assert s.send_key is True
        assert s.replica == Replica.SEQUENCE

    def test_default_read_ap_has_read_touch_ttl_percent(self):
        s = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert s.read_touch_ttl_percent == 0

    def test_default_batch_read_settings(self):
        s = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
        assert s.max_concurrent_nodes == 1
        assert s.allow_inline is True
        assert s.allow_inline_ssd is False

    def test_default_batch_write_settings(self):
        s = Behavior.DEFAULT.get_settings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP)
        assert s.max_concurrent_nodes == 1
        assert s.allow_inline is True
        assert s.allow_inline_ssd is False

    def test_default_retryable_write_ap_settings(self):
        s = Behavior.DEFAULT.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert s.durable_delete is False
        assert s.commit_level == CommitLevel.COMMIT_ALL

    def test_default_retryable_write_sc_settings(self):
        s = Behavior.DEFAULT.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.SC)
        assert s.durable_delete is True

    def test_default_write_non_retryable_has_zero_retries(self):
        s = Behavior.DEFAULT.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT)
        assert s.max_retries == 0
        assert s.durable_delete is False

    def test_default_reads_query_overrides_max_retries(self):
        s = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.QUERY)
        assert s.max_retries == 5
        assert s.record_queue_size == 5000

    def test_default_write_ap_has_commit_all(self):
        s = Behavior.DEFAULT.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert s.commit_level == CommitLevel.COMMIT_ALL

    def test_all_operation_combinations_return_settings(self):
        for shape in OpShape:
            for mode in Mode:
                s = Behavior.DEFAULT.get_settings(OpKind.READ, shape, mode)
                assert s is not None
                assert s.total_timeout is not None

        for kind in (OpKind.WRITE_RETRYABLE, OpKind.WRITE_NON_RETRYABLE):
            for shape in OpShape:
                for mode in Mode:
                    s = Behavior.DEFAULT.get_settings(kind, shape, mode)
                    assert s is not None
                    assert s.total_timeout is not None


class TestDeriveWithChanges:
    """Verify derive_with_changes() creates child behaviors that correctly
    inherit from parents, apply scope-keyed and flat-kwarg overrides,
    and never mutate the parent behavior."""
    def test_scope_override(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "child",
            reads=Settings(total_timeout=timedelta(seconds=1)),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT)
        assert s.total_timeout == timedelta(seconds=1)
        # Writes should still inherit from DEFAULT ALL scope
        w = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert w.total_timeout == timedelta(seconds=30)

    def test_flat_kwargs_apply_to_all_scope(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "child",
            total_timeout=timedelta(seconds=3),
        )
        r = child.get_settings(OpKind.READ, OpShape.POINT)
        assert r.total_timeout == timedelta(seconds=3)
        w = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert w.total_timeout == timedelta(seconds=3)

    def test_specific_scope_overrides_general(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "child",
            all=Settings(max_retries=10),
            reads_point=Settings(max_retries=1),
        )
        rp = child.get_settings(OpKind.READ, OpShape.POINT)
        assert rp.max_retries == 1
        rb = child.get_settings(OpKind.READ, OpShape.BATCH)
        assert rb.max_retries == 10

    def test_grandchild_inherits(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "child",
            reads=Settings(max_retries=7),
        )
        grandchild = child.derive_with_changes(
            "grandchild",
            reads_point=Settings(max_retries=1),
        )
        rp = grandchild.get_settings(OpKind.READ, OpShape.POINT)
        assert rp.max_retries == 1
        rb = grandchild.get_settings(OpKind.READ, OpShape.BATCH)
        assert rb.max_retries == 7

    def test_parent_not_mutated(self):
        before = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT)
        Behavior.DEFAULT.derive_with_changes(
            "child",
            all=Settings(max_retries=999),
        )
        after = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT)
        assert before.max_retries == after.max_retries

    def test_empty_derive_inherits_defaults(self):
        child = Behavior.DEFAULT.derive_with_changes("empty")
        s = child.get_settings(OpKind.READ, OpShape.POINT)
        assert s.total_timeout == timedelta(seconds=30)
        assert s.max_retries == 2
        assert s.send_key is True

    def test_child_inherits_and_overrides(self):
        parent = Behavior.DEFAULT.derive_with_changes(
            "parent",
            all=Settings(max_retries=5, send_key=False),
        )
        child = parent.derive_with_changes(
            "child",
            all=Settings(max_retries=10),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT)
        assert s.max_retries == 10
        assert s.send_key is False

    def test_multi_level_inheritance(self):
        grandparent = Behavior.DEFAULT.derive_with_changes(
            "grandparent",
            all=Settings(max_retries=0, send_key=True, durable_delete=True),
        )
        parent = grandparent.derive_with_changes(
            "parent",
            all=Settings(max_retries=1),
        )
        child = parent.derive_with_changes(
            "child",
            all=Settings(max_retries=2),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT)
        assert s.max_retries == 2
        assert s.send_key is True
        assert s.durable_delete is True

    def test_different_attributes_merge(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "child",
            all=Settings(max_retries=5, send_key=False),
            reads_ap=Settings(read_touch_ttl_percent=50),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert s.max_retries == 5
        assert s.send_key is False
        assert s.read_touch_ttl_percent == 50


class TestIsolation:
    """Verify that scoped settings are properly isolated: AP vs SC mode,
    point vs batch shape, retryable vs non-retryable, and reads vs
    writes do not leak into each other."""
    def test_mode_isolation(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "mode_test",
            reads_ap=Settings(read_touch_ttl_percent=50),
            reads_sc=Settings(read_touch_ttl_percent=75),
        )
        ap = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        sc = child.get_settings(OpKind.READ, OpShape.POINT, Mode.SC)
        assert ap.read_touch_ttl_percent == 50
        assert sc.read_touch_ttl_percent == 75

    def test_shape_isolation(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "shape_test",
            reads_batch=Settings(max_concurrent_nodes=10),
        )
        batch = child.get_settings(OpKind.READ, OpShape.BATCH)
        point = child.get_settings(OpKind.READ, OpShape.POINT)
        assert batch.max_concurrent_nodes == 10
        assert point.max_concurrent_nodes == 1

    def test_retryability_isolation(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "retry_test",
            writes_retryable=Settings(max_retries=10),
            writes_non_retryable=Settings(max_retries=1),
        )
        retryable = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        non_retryable = child.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT)
        assert retryable.max_retries == 10
        assert non_retryable.max_retries == 1

    def test_reads_do_not_affect_writes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "kind_test",
            reads=Settings(max_retries=99),
        )
        r = child.get_settings(OpKind.READ, OpShape.POINT)
        w = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert r.max_retries == 99
        assert w.max_retries == 2

    def test_writes_do_not_affect_reads(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "kind_test2",
            writes=Settings(durable_delete=True),
        )
        r = child.get_settings(OpKind.READ, OpShape.POINT)
        w = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert r.durable_delete is False
        assert w.durable_delete is True


class TestNameProperty:
    """Verify the name property is set correctly on DEFAULT, derived, and
    pre-defined behaviors."""
    def test_default_name(self):
        assert Behavior.DEFAULT.name == "DEFAULT"

    def test_derived_name(self):
        child = Behavior.DEFAULT.derive_with_changes("my_custom")
        assert child.name == "my_custom"

    def test_predefined_names(self):
        assert Behavior.READ_FAST.name == "READ_FAST"
        assert Behavior.STRICTLY_CONSISTENT.name == "STRICTLY_CONSISTENT"
        assert Behavior.FAST_RACK_AWARE.name == "FAST_RACK_AWARE"


class TestParentProperty:
    """Verify the parent property returns the correct ancestor at each
    level of the derivation chain."""
    def test_default_has_no_parent(self):
        assert Behavior.DEFAULT.parent is None

    def test_derived_parent_is_default(self):
        child = Behavior.DEFAULT.derive_with_changes("child")
        assert child.parent is Behavior.DEFAULT

    def test_grandchild_parent_chain(self):
        parent = Behavior.DEFAULT.derive_with_changes("parent")
        child = parent.derive_with_changes("child")
        assert child.parent is parent
        assert child.parent.parent is Behavior.DEFAULT


class TestChildrenProperty:
    """Verify the children property tracks all behaviors derived from
    a given parent."""
    def test_default_has_predefined_children(self):
        names = {c.name for c in Behavior.DEFAULT.children}
        assert "READ_FAST" in names
        assert "STRICTLY_CONSISTENT" in names

    def test_derived_children(self):
        parent = Behavior.DEFAULT.derive_with_changes("parent")
        child1 = parent.derive_with_changes("child1")
        child2 = parent.derive_with_changes("child2")
        assert len(parent.children) == 2
        assert child1 in parent.children
        assert child2 in parent.children

    def test_leaf_has_no_children(self):
        leaf = Behavior.DEFAULT.derive_with_changes("leaf")
        assert leaf.children == []


class TestScopePrecedenceCascade:
    """Verify the full precedence cascade: when multiple scopes set the
    same field, the most-specific scope wins while less-specific scopes
    still apply to their respective operation types."""
    def test_full_cascade_most_specific_wins(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "cascade",
            all=Settings(max_retries=1),
            reads=Settings(max_retries=2),
            reads_batch=Settings(max_retries=3),
        )
        batch_ap = child.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
        assert batch_ap.max_retries == 3

        point_ap = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert point_ap.max_retries == 2

        write_ap = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert write_ap.max_retries == 1

    def test_write_scope_cascade(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "w_cascade",
            all=Settings(max_retries=1),
            writes=Settings(max_retries=2),
            writes_retryable=Settings(max_retries=3),
            writes_point=Settings(max_retries=4),
        )
        wp = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert wp.max_retries == 4

        wb = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.BATCH)
        assert wb.max_retries == 3

        wnr = child.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT)
        assert wnr.max_retries == 4

        reads = child.get_settings(OpKind.READ, OpShape.POINT)
        assert reads.max_retries == 1


class TestPreDefinedBehaviors:
    """Verify the pre-defined behaviors (READ_FAST, STRICTLY_CONSISTENT,
    FAST_RACK_AWARE) have the expected field values and correctly
    inherit from their parents for non-overridden scopes."""
    def test_read_fast_has_short_timeouts(self):
        s = Behavior.READ_FAST.get_settings(OpKind.READ, OpShape.POINT)
        assert s.total_timeout == timedelta(milliseconds=200)
        assert s.socket_timeout == timedelta(milliseconds=50)
        assert s.max_retries == 3

    def test_read_fast_writes_inherit_from_default(self):
        s = Behavior.READ_FAST.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT)
        assert s.total_timeout == timedelta(seconds=30)

    def test_strictly_consistent_inherits_defaults(self):
        s = Behavior.STRICTLY_CONSISTENT.get_settings(OpKind.READ, OpShape.POINT)
        assert s.total_timeout == timedelta(seconds=30)
        assert s.max_retries == 2
        assert s.replica == Replica.SEQUENCE

    def test_strictly_consistent_applies_linearize_to_sc_reads(self):
        s = Behavior.STRICTLY_CONSISTENT.get_settings(OpKind.READ, OpShape.POINT, Mode.SC)
        assert s.read_mode_sc == ReadModeSC.LINEARIZE

    def test_strictly_consistent_leaves_ap_reads_unset(self):
        s = Behavior.STRICTLY_CONSISTENT.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert s.read_mode_sc is None

    def test_fast_rack_aware_uses_prefer_rack(self):
        s = Behavior.FAST_RACK_AWARE.get_settings(OpKind.READ, OpShape.POINT)
        assert s.replica == Replica.PREFER_RACK
        assert s.total_timeout == timedelta(milliseconds=200)


class TestComprehensiveAttributes:
    """Verify every Settings field can be set via derive_with_changes and
    retrieved through get_settings for the appropriate scope."""
    def test_all_common_attributes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "common",
            all=Settings(
                total_timeout=timedelta(seconds=99),
                socket_timeout=timedelta(seconds=88),
                max_retries=99,
                retry_delay=timedelta(milliseconds=999),
                send_key=False,
                replica=Replica.MASTER,
                durable_delete=True,
                max_concurrent_nodes=42,
                read_touch_ttl_percent=75,
            ),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert s.total_timeout == timedelta(seconds=99)
        assert s.socket_timeout == timedelta(seconds=88)
        assert s.max_retries == 99
        assert s.retry_delay == timedelta(milliseconds=999)
        assert s.send_key is False
        assert s.replica == Replica.MASTER
        assert s.durable_delete is True
        assert s.max_concurrent_nodes == 42
        assert s.read_touch_ttl_percent == 75

    def test_all_batch_attributes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "batch_attrs",
            reads_batch=Settings(
                max_concurrent_nodes=99,
                allow_inline=False,
                allow_inline_ssd=True,
            ),
        )
        s = child.get_settings(OpKind.READ, OpShape.BATCH, Mode.AP)
        assert s.max_concurrent_nodes == 99
        assert s.allow_inline is False
        assert s.allow_inline_ssd is True

    def test_all_query_attributes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "query_attrs",
            reads_query=Settings(record_queue_size=99999),
        )
        s = child.get_settings(OpKind.READ, OpShape.QUERY, Mode.AP)
        assert s.record_queue_size == 99999

    def test_all_write_attributes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "write_attrs",
            writes=Settings(durable_delete=True),
        )
        r = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        nr = child.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP)
        assert r.durable_delete is True
        assert nr.durable_delete is True

    def test_all_write_ap_attributes(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "write_ap_attrs",
            writes_ap=Settings(commit_level=CommitLevel.COMMIT_MASTER),
        )
        s = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert s.commit_level == CommitLevel.COMMIT_MASTER


class TestCoreV3Fields:
    """Verify the v3 core additions (ReadModeAP/SC, compression, new Replica
    variants) flow through Settings, merge, and derive_with_changes."""

    def test_read_mode_ap_resolves(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "ap_all",
            reads_ap=Settings(read_mode_ap=ReadModeAP.ALL),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
        assert s.read_mode_ap == ReadModeAP.ALL

    def test_read_mode_sc_resolves(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "sc_linearize",
            reads_sc=Settings(read_mode_sc=ReadModeSC.LINEARIZE),
        )
        s = child.get_settings(OpKind.READ, OpShape.POINT, Mode.SC)
        assert s.read_mode_sc == ReadModeSC.LINEARIZE

    def test_use_compression_flat_kwarg(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "compressed", use_compression=True,
        )
        s = child.get_settings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP)
        assert s.use_compression is True

    def test_new_replica_variants_available(self):
        for variant in (Replica.MASTER_PROLES, Replica.RANDOM):
            child = Behavior.DEFAULT.derive_with_changes(
                f"replica_{variant}", reads=Settings(replica=variant),
            )
            s = child.get_settings(OpKind.READ, OpShape.POINT, Mode.AP)
            assert s.replica == variant


class TestBackwardCompatProperties:
    """Verify the backward-compatible read-only properties (total_timeout,
    socket_timeout, max_retries, retry_delay, send_key) that resolve
    from (READ, POINT, AP) scope, preserving the original flat-field API."""
    def test_total_timeout(self):
        assert Behavior.DEFAULT.total_timeout == timedelta(seconds=30)

    def test_max_retries(self):
        assert Behavior.DEFAULT.max_retries == 2

    def test_socket_timeout(self):
        assert Behavior.DEFAULT.socket_timeout == timedelta(seconds=5)

    def test_retry_delay(self):
        assert Behavior.DEFAULT.retry_delay == timedelta(0)

    def test_send_key(self):
        assert Behavior.DEFAULT.send_key is True

    def test_repr(self):
        r = repr(Behavior.DEFAULT)
        assert "DEFAULT" in r
        assert "total_timeout" in r


class TestCaching:
    """Verify that get_settings uses a pre-computed cache and that
    clear_cache recomputes correctly, cascading to children."""
    def test_get_settings_returns_cached_object(self):
        s1 = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT)
        s2 = Behavior.DEFAULT.get_settings(OpKind.READ, OpShape.POINT)
        assert s1 is s2

    def test_clear_cache_preserves_values(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "cache_test",
            all=Settings(max_retries=7),
        )
        before = child.get_settings(OpKind.READ, OpShape.POINT)
        child.clear_cache()
        after = child.get_settings(OpKind.READ, OpShape.POINT)
        assert before.max_retries == after.max_retries == 7

    def test_clear_cache_cascades_to_children(self):
        parent = Behavior.DEFAULT.derive_with_changes(
            "cache_parent",
            all=Settings(max_retries=5),
        )
        child = parent.derive_with_changes(
            "cache_child",
            all=Settings(max_retries=10),
        )
        parent.clear_cache()
        assert child.get_settings(OpKind.READ, OpShape.POINT).max_retries == 10


class TestExplain:
    """Verify that explain() returns a useful diagnostic string containing
    the behavior name, its patches, and the resolved matrix."""
    def test_contains_name(self):
        output = Behavior.DEFAULT.explain()
        assert "Behavior: DEFAULT" in output

    def test_contains_patches_section(self):
        output = Behavior.DEFAULT.explain()
        assert "--- Patches ---" in output
        assert "all" in output

    def test_contains_resolved_matrix(self):
        output = Behavior.DEFAULT.explain()
        assert "--- Resolved Matrix ---" in output
        assert "read:point:ap" in output

    def test_empty_derive_shows_no_overrides(self):
        child = Behavior.DEFAULT.derive_with_changes("explain_empty")
        output = child.explain()
        assert "(no overrides)" in output

    def test_derived_shows_patch(self):
        child = Behavior.DEFAULT.derive_with_changes(
            "explain_child",
            reads=Settings(max_retries=99),
        )
        output = child.explain()
        assert "reads" in output
        assert "max_retries=99" in output


class TestFindBehavior:
    """Verify find_behavior traverses the behavior tree downward to
    locate a descendant by name."""
    def test_find_self(self):
        assert Behavior.DEFAULT.find_behavior("DEFAULT") is Behavior.DEFAULT

    def test_find_direct_child(self):
        found = Behavior.DEFAULT.find_behavior("READ_FAST")
        assert found is Behavior.READ_FAST

    def test_find_grandchild(self):
        found = Behavior.DEFAULT.find_behavior("FAST_RACK_AWARE")
        assert found is Behavior.FAST_RACK_AWARE

    def test_not_found_returns_none(self):
        assert Behavior.DEFAULT.find_behavior("NONEXISTENT") is None

    def test_scoped_search_from_subtree(self):
        parent = Behavior.DEFAULT.derive_with_changes("find_parent")
        child = parent.derive_with_changes("find_child")
        assert parent.find_behavior("find_child") is child
        assert parent.find_behavior("READ_FAST") is None


class TestRegistry:
    """Verify the global behavior registry auto-registers behaviors and
    supports name-based lookup."""
    def test_default_is_registered(self):
        assert get_behavior("DEFAULT") is Behavior.DEFAULT

    def test_predefined_are_registered(self):
        assert get_behavior("READ_FAST") is Behavior.READ_FAST
        assert get_behavior("STRICTLY_CONSISTENT") is Behavior.STRICTLY_CONSISTENT
        assert get_behavior("FAST_RACK_AWARE") is Behavior.FAST_RACK_AWARE

    def test_derived_auto_registered(self):
        child = Behavior.DEFAULT.derive_with_changes("registry_test_child")
        assert get_behavior("registry_test_child") is child

    def test_get_behavior_returns_none_for_unknown(self):
        assert get_behavior("DOES_NOT_EXIST") is None

    def test_get_behavior_or_default_falls_back(self):
        result = get_behavior_or_default("DOES_NOT_EXIST")
        assert result is Behavior.DEFAULT

    def test_get_behavior_or_default_finds_existing(self):
        assert get_behavior_or_default("READ_FAST") is Behavior.READ_FAST

    def test_get_all_behaviors_includes_predefined(self):
        all_b = get_all_behaviors()
        assert "DEFAULT" in all_b
        assert "READ_FAST" in all_b
        assert "STRICTLY_CONSISTENT" in all_b
        assert "FAST_RACK_AWARE" in all_b
