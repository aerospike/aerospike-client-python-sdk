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

"""Tests for Selector and the ``on`` entries of a Behavior (no server needed)."""

import dataclasses
import itertools
import logging
import random
from datetime import timedelta

import pytest

from aerospike_sdk import Key
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.background_shared import make_background_write_policy
from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy import (
    Behavior,
    CommitLevel,
    Mode,
    OpKind,
    OpShape,
    ReadModeAP,
    ReadModeSC,
    ReadSelector,
    Replica,
    Scope,
    Selector,
    Settings,
    WriteSelector,
    behavior_registry,
    policy_mapper,
)
from aerospike_sdk.policy.behavior_settings import resolution_order
from aerospike_sdk.policy.selector import _DOMAIN_OF
from aerospike_sdk.query_shared import _OperationSpec

READ, RETRYABLE, NON_RETRYABLE = OpKind.READ, OpKind.WRITE_RETRYABLE, OpKind.WRITE_NON_RETRYABLE
WRITES = (RETRYABLE, NON_RETRYABLE)
KEYS = [(kind, shape, mode) for kind in OpKind for shape in OpShape for mode in Mode]
TXN_SCOPES = (Scope.SYSTEM_TXN_VERIFY, Scope.SYSTEM_TXN_ROLL)
PRESETS = ("DEFAULT", "READ_FAST", "STRICTLY_CONSISTENT", "FAST_RACK_AWARE")

_names = itertools.count()


def _name() -> str:
    return f"selector-test-{next(_names)}"


def _derive(parent=None, **kwargs) -> Behavior:
    return (parent or Behavior.DEFAULT).derive_with_changes(_name(), **kwargs)


def _txn_settings(behavior: Behavior, scope: Scope) -> Settings:
    if scope is Scope.SYSTEM_TXN_VERIFY:
        return behavior.get_txn_verify_settings()
    return behavior.get_txn_roll_settings()


@pytest.fixture(autouse=True)
def _restore_behaviors():
    """Every test here derives behaviors; drop them from the registry and the presets."""
    registry = dict(behavior_registry._registry)
    children = {name: list(getattr(Behavior, name)._children) for name in PRESETS}
    yield
    behavior_registry._registry.clear()
    behavior_registry._registry.update(registry)
    for name, saved in children.items():
        getattr(Behavior, name)._children[:] = saved


# -- Selector -----------------------------------------------------------------

def _matrix_selectors():
    """Every buildable matrix selector, with the cells it must match."""
    yield Selector.all(), set(KEYS)
    shapes = [(None, None), *((shape.value, shape) for shape in OpShape)]
    modes = [(None, None), *((mode.value, mode) for mode in Mode)]
    for (shape_method, shape), (mode_method, mode) in itertools.product(shapes, modes):
        starts = [
            (Selector.reads(), {READ}),
            (Selector.writes(), set(WRITES)),
            (Selector.writes().retryable(), {RETRYABLE}),
            (Selector.writes().non_retryable(), {NON_RETRYABLE}),
        ]
        for selector, kinds in starts:
            for method in (shape_method, mode_method):
                if method is not None:
                    selector = getattr(selector, method)()
            yield selector, {
                key for key in KEYS
                if key[0] in kinds and shape in (None, key[1]) and mode in (None, key[2])
            }


class TestSelectorMatching:

    def test_there_are_49_distinct_matrix_selectors(self):
        assert len({selector for selector, _ in _matrix_selectors()}) == 49

    @pytest.mark.parametrize(
        ("selector", "expected"), list(_matrix_selectors()), ids=lambda value: str(value)[:40],
    )
    def test_matrix_selector_matches_exactly_its_cells(self, selector, expected):
        assert {key for key in KEYS if selector._matches(*key)} == expected

    def test_only_all_among_matrix_selectors_reaches_the_transaction_phases(self):
        for selector, _ in _matrix_selectors():
            reached = [scope for scope in TXN_SCOPES if selector._matches_txn(scope)]
            assert reached == (list(TXN_SCOPES) if selector == Selector.all() else [])

    @pytest.mark.parametrize(
        ("selector", "phase"),
        [(Selector.txn_verify(), Scope.SYSTEM_TXN_VERIFY), (Selector.txn_roll(), Scope.SYSTEM_TXN_ROLL)],
        ids=["verify", "roll"],
    )
    def test_transaction_selector_matches_only_its_phase(self, selector, phase):
        assert [scope for scope in TXN_SCOPES if selector._matches_txn(scope)] == [phase]
        assert not any(selector._matches(*key) for key in KEYS)


class TestSelectorResolution:
    """Each selector, through a derived behavior, changes exactly the cells it names."""

    @pytest.mark.parametrize(
        ("selector", "expected"), list(_matrix_selectors()), ids=lambda value: str(value)[:40],
    )
    def test_entry_changes_exactly_the_selected_cells(self, selector, expected):
        behavior = _derive(on=[(selector, Settings(max_retries=91))])
        for key in KEYS:
            unchanged = Behavior.DEFAULT.get_settings(*key).max_retries
            assert behavior.get_settings(*key).max_retries == (91 if key in expected else unchanged), key
        for scope in TXN_SCOPES:
            unchanged = _txn_settings(Behavior.DEFAULT, scope).max_retries
            reached = selector == Selector.all()
            assert _txn_settings(behavior, scope).max_retries == (91 if reached else unchanged), scope


class TestSelectorValue:

    def test_chain_order_does_not_matter(self):
        one = Selector.writes().retryable().point().ap()
        other = Selector.writes().ap().point().retryable()
        assert one == other
        assert hash(one) == hash(other)

    def test_different_selectors_are_unequal(self):
        assert Selector.reads().ap() != Selector.reads().sc()
        assert Selector.reads() != Selector.writes()
        assert Selector.txn_verify() != Selector.txn_roll()

    @pytest.mark.parametrize(
        ("selector", "text"),
        [
            (Selector.all(), "all()"),
            (Selector.reads(), "reads()"),
            (Selector.reads().ap().batch(), "reads().batch().ap()"),
            (Selector.writes().sc(), "writes().sc()"),
            (Selector.writes().ap().point().non_retryable(), "writes().non_retryable().point().ap()"),
            (Selector.txn_verify(), "txn_verify()"),
            (Selector.txn_roll(), "txn_roll()"),
        ],
    )
    def test_str_is_the_canonical_chain(self, selector, text):
        assert str(selector) == text
        assert repr(selector) == f"Selector.{text}"

    def test_entry_points_return_the_narrowable_types(self):
        assert type(Selector.reads()) is ReadSelector
        assert type(Selector.writes()) is WriteSelector
        assert type(Selector.reads().batch().sc()) is ReadSelector
        for plain in (Selector.all(), Selector.txn_verify(), Selector.txn_roll()):
            assert type(plain) is Selector

    @pytest.mark.parametrize(
        ("narrow_twice", "message"),
        [
            (lambda: Selector.reads().ap().sc(), r"reads\(\)\.ap\(\) already selects a mode"),
            (lambda: Selector.reads().point().batch(), "already selects a shape"),
            (lambda: Selector.writes().point().sc().query(), "already selects a shape"),
            (lambda: Selector.writes().retryable().non_retryable(), "already selects a retryability"),
            (lambda: Selector.writes().retryable().retryable(), "already selects a retryability"),
        ],
    )
    def test_narrowing_an_axis_twice_is_refused(self, narrow_twice, message):
        with pytest.raises(ValueError, match=message):
            narrow_twice()

    def test_only_write_selectors_have_retryability(self):
        assert not hasattr(Selector.reads(), "retryable")
        assert not hasattr(Selector.reads(), "non_retryable")

    @pytest.mark.parametrize("build", [Selector.all, Selector.txn_verify, Selector.txn_roll])
    def test_all_and_transaction_selectors_take_no_narrowing(self, build):
        for method in ("point", "batch", "query", "ap", "sc", "retryable"):
            assert not hasattr(build(), method)

    def test_selector_is_immutable(self):
        with pytest.raises(AttributeError):
            Selector.reads()._mode = Mode.SC


# -- Precedence ---------------------------------------------------------------

class TestOnPrecedence:

    def test_entry_applies_after_a_narrower_keyword(self):
        behavior = _derive(
            reads_batch=Settings(max_retries=5),
            on=[(Selector.reads(), Settings(max_retries=1))],
        )
        assert behavior.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 1

    def test_entry_overrides_only_the_fields_it_sets(self):
        behavior = _derive(
            writes=Settings(max_retries=5, total_timeout=timedelta(seconds=7)),
            on=[(Selector.writes().point(), Settings(max_retries=1))],
        )
        settings = behavior.get_settings(RETRYABLE, OpShape.POINT, Mode.AP)
        assert (settings.max_retries, settings.total_timeout) == (1, timedelta(seconds=7))

    def test_later_entry_wins_whatever_its_breadth(self):
        narrow_then_broad = _derive(on=[
            (Selector.reads().batch().ap(), Settings(max_retries=9)),
            (Selector.reads(), Settings(max_retries=1)),
        ])
        broad_then_narrow = _derive(on=[
            (Selector.reads(), Settings(max_retries=1)),
            (Selector.reads().batch().ap(), Settings(max_retries=9)),
        ])
        assert narrow_then_broad.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 1
        assert broad_then_narrow.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 9

    def test_repeated_selector_applies_every_entry(self):
        behavior = _derive(on=[
            (Selector.writes().retryable().point().ap(), Settings(commit_level=CommitLevel.COMMIT_MASTER)),
            (Selector.writes().ap().retryable().point(), Settings(total_timeout=timedelta(seconds=2))),
        ])
        settings = behavior.get_settings(RETRYABLE, OpShape.POINT, Mode.AP)
        assert settings.commit_level == CommitLevel.COMMIT_MASTER
        assert settings.total_timeout == timedelta(seconds=2)

    def test_overlapping_selectors_all_apply(self):
        behavior = _derive(on=[
            (Selector.reads().ap(), Settings(max_retries=6)),
            (Selector.reads().batch(), Settings(total_timeout=timedelta(seconds=3))),
        ])
        settings = behavior.get_settings(READ, OpShape.BATCH, Mode.AP)
        assert (settings.max_retries, settings.total_timeout) == (6, timedelta(seconds=3))

    def test_child_keyword_overrides_parent_entry(self):
        parent = _derive(on=[(Selector.reads().batch().ap(), Settings(max_retries=9))])
        child = _derive(parent, reads=Settings(max_retries=2))
        assert child.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 2

    def test_child_inherits_parent_entry(self):
        parent = _derive(on=[(Selector.reads().batch().ap(), Settings(max_retries=9))])
        child = _derive(parent, writes=Settings(max_retries=2))
        assert child.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 9

    def test_general_to_specific_cascade(self):
        behavior = _derive(on=[
            (Selector.all(), Settings(max_retries=1)),
            (Selector.reads(), Settings(max_retries=2)),
            (Selector.reads().batch(), Settings(max_retries=3)),
            (Selector.reads().batch().ap(), Settings(max_retries=4)),
        ])
        assert behavior.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 4
        assert behavior.get_settings(READ, OpShape.BATCH, Mode.SC).max_retries == 3
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).max_retries == 2
        assert behavior.get_settings(RETRYABLE, OpShape.POINT, Mode.AP).max_retries == 1

    def test_same_selector_setting_one_field_twice_keeps_the_later_value(self):
        behavior = _derive(on=[
            (Selector.reads().ap(), Settings(read_mode_ap=ReadModeAP.ONE)),
            (Selector.reads().ap(), Settings(read_mode_ap=ReadModeAP.ALL)),
        ])
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).read_mode_ap == ReadModeAP.ALL

    def test_three_levels_of_inheritance(self):
        parent = _derive(on=[(Selector.reads().batch().ap(), Settings(
            max_retries=9, total_timeout=timedelta(seconds=7),
        ))])
        child = _derive(parent, on=[(Selector.reads().batch(), Settings(max_retries=6))])
        grandchild = _derive(child, on=[(Selector.reads().ap(), Settings(
            retry_delay=timedelta(milliseconds=3),
        ))])
        settings = grandchild.get_settings(READ, OpShape.BATCH, Mode.AP)
        assert (settings.max_retries, settings.total_timeout, settings.retry_delay) == (
            6, timedelta(seconds=7), timedelta(milliseconds=3),
        )
        assert grandchild.get_settings(READ, OpShape.POINT, Mode.AP).max_retries == (
            Behavior.DEFAULT.get_settings(READ, OpShape.POINT, Mode.AP).max_retries
        )

    def test_constructor_takes_on(self):
        behavior = Behavior(
            _name(), {Scope.READS: Settings(max_retries=5)},
            on=[(Selector.reads().query(), Settings(max_retries=8))],
        )
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).max_retries == 5
        assert behavior.get_settings(READ, OpShape.QUERY, Mode.SC).max_retries == 8


class TestOnTransactionPhases:

    def test_all_reaches_both_phases(self):
        behavior = _derive(on=[(Selector.all(), Settings(max_retries=9))])
        assert behavior.get_txn_verify_settings().max_retries == 9
        assert behavior.get_txn_roll_settings().max_retries == 9

    def test_phase_selector_reaches_only_its_phase(self):
        behavior = _derive(on=[(Selector.txn_roll(), Settings(max_retries=9))])
        assert behavior.get_txn_roll_settings().max_retries == 9
        assert behavior.get_txn_verify_settings() == Behavior.DEFAULT.get_txn_verify_settings()
        assert all(behavior.get_settings(*key) == Behavior.DEFAULT.get_settings(*key) for key in KEYS)

    def test_phase_selector_listed_after_all_wins(self):
        behavior = _derive(on=[
            (Selector.all(), Settings(max_retries=9)),
            (Selector.txn_roll(), Settings(max_retries=3)),
        ])
        assert behavior.get_txn_roll_settings().max_retries == 3
        assert behavior.get_txn_verify_settings().max_retries == 9

    def test_all_entry_applies_after_the_transaction_keyword(self):
        behavior = _derive(
            system_txn_roll=Settings(max_retries=3),
            on=[(Selector.all(), Settings(max_retries=9))],
        )
        assert behavior.get_txn_roll_settings().max_retries == 9

    @pytest.mark.parametrize("build", [Selector.reads, Selector.writes], ids=["reads", "writes"])
    def test_read_and_write_selectors_do_not_reach_the_phases(self, build):
        behavior = _derive(on=[(build(), Settings(max_retries=9))])
        for scope in TXN_SCOPES:
            assert _txn_settings(behavior, scope) == _txn_settings(Behavior.DEFAULT, scope)


# -- Input, explain, hot reload -----------------------------------------------

class TestOnInput:

    @pytest.mark.parametrize(
        ("on", "position"),
        [
            ([Selector.reads()], 0),
            ([(Selector.reads(), Settings()), (Selector.reads(), None)], 1),
            ([(Selector.reads(), Settings()), (Scope.READS, Settings())], 1),
            ([(Selector.reads(), Settings(), Settings())], 0),
            ([[Selector.reads(), Settings()]], 0),
        ],
        ids=["bare-selector", "none-settings", "scope-for-selector", "triple", "list-entry"],
    )
    def test_entry_that_is_not_a_pair_is_a_type_error_naming_its_position(self, on, position):
        with pytest.raises(TypeError, match=rf"on\[{position}\] must be a \(Selector, Settings\) pair"):
            _derive(on=on)

    def test_any_iterable_is_accepted(self):
        entries = ((Selector.reads(), Settings(max_retries=n)) for n in (4, 6))
        behavior = _derive(on=entries)
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).max_retries == 6

    def test_caller_changes_to_the_list_do_not_reach_the_behavior(self):
        entries = [(Selector.reads(), Settings(max_retries=4))]
        behavior = _derive(on=entries)
        entries.append((Selector.reads(), Settings(max_retries=6)))
        behavior.clear_cache()
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).max_retries == 4

    @pytest.mark.parametrize("on", [None, [], ()], ids=["none", "empty-list", "empty-tuple"])
    def test_no_entries_resolves_as_the_keywords_alone(self, on):
        with_on = _derive(reads=Settings(max_retries=5), on=on)
        without = _derive(reads=Settings(max_retries=5))
        assert all(with_on.get_settings(*key) == without.get_settings(*key) for key in KEYS)


class TestExplainAndReload:

    def test_explain_lists_entries_after_the_keywords_in_order(self):
        behavior = _derive(
            writes=Settings(max_retries=5),
            on=[
                (Selector.writes().retryable().point().ap(), Settings(max_retries=4)),
                (Selector.txn_roll(), Settings(max_retries=3)),
            ],
        )
        patches = behavior.explain().split("--- Resolved Matrix ---")[0].splitlines()
        assert patches[2].startswith("01 writes -> Settings(")
        assert patches[3].startswith("02 on writes().retryable().point().ap() -> Settings(")
        assert patches[4].startswith("03 on txn_roll() -> Settings(")

    def test_explain_with_entries_only_does_not_say_no_overrides(self):
        text = _derive(on=[(Selector.reads(), Settings(max_retries=4))]).explain()
        assert "(no overrides)" not in text
        assert "01 on reads() -> Settings(" in text

    def test_reload_drops_entries_and_logs_how_many(self, caplog):
        behavior = _derive(on=[
            (Selector.reads().batch().ap(), Settings(max_retries=9)),
            (Selector.txn_roll(), Settings(max_retries=3)),
        ])
        with caplog.at_level(logging.INFO, logger=SdkLoggers.BEHAVIOR):
            behavior._reload_patches({Scope.READS: Settings(max_retries=2)})
        assert behavior.get_settings(READ, OpShape.BATCH, Mode.AP).max_retries == 2
        assert behavior.get_txn_roll_settings() == Behavior.DEFAULT.get_txn_roll_settings()
        assert f"Behavior {behavior.name!r} reloaded from the config file; dropped 2 on entries" in caplog.text

    def test_reload_without_entries_logs_nothing(self, caplog):
        behavior = _derive(reads=Settings(max_retries=9))
        with caplog.at_level(logging.INFO, logger=SdkLoggers.BEHAVIOR):
            behavior._reload_patches({Scope.READS: Settings(max_retries=2)})
        assert "dropped" not in caplog.text


# -- Compatibility ------------------------------------------------------------

def _keyword_resolution(behavior: Behavior, key) -> Settings:
    """The keyword-only resolver as it was before ``on`` existed: the reference."""
    base = _keyword_resolution(behavior.parent, key) if behavior.parent is not None else Settings()
    for scope in resolution_order(*key):
        patch = behavior._patches.get(scope)
        if patch is not None:
            base = Settings.merge(base, patch)
    return base


def _keyword_txn_resolution(behavior: Behavior, txn_scope: Scope) -> Settings:
    base = (
        _keyword_txn_resolution(behavior.parent, txn_scope)
        if behavior.parent is not None else Settings()
    )
    for scope in (Scope.ALL, txn_scope):
        patch = behavior._patches.get(scope)
        if patch is not None:
            base = Settings.merge(base, patch)
    return base


def _assert_resolves_as_keywords_alone(behavior: Behavior) -> None:
    for key in KEYS:
        assert behavior.get_settings(*key) == _keyword_resolution(behavior, key), key
    for scope in TXN_SCOPES:
        assert _txn_settings(behavior, scope) == _keyword_txn_resolution(behavior, scope), scope


class TestKeywordCallsAreUnchanged:

    @pytest.mark.parametrize("preset", PRESETS)
    def test_preset_resolves_as_before(self, preset):
        _assert_resolves_as_keywords_alone(getattr(Behavior, preset))

    def test_generated_keyword_only_behaviors_resolve_as_before(self):
        rng = random.Random(20261121)
        pool = [
            Settings(total_timeout=timedelta(milliseconds=ms)) for ms in (10, 20, 30)
        ] + [
            Settings(max_retries=0),
            Settings(max_retries=5, retry_delay=timedelta(milliseconds=4)),
            Settings(replica=Replica.MASTER),
            Settings(durable_delete=True),
            Settings(read_mode_ap=ReadModeAP.ALL, socket_timeout=timedelta(seconds=9)),
            Settings(commit_level=CommitLevel.COMMIT_MASTER, max_retries=7),
        ]
        scopes = list(Scope)

        def patches():
            return {scope: rng.choice(pool) for scope in rng.sample(scopes, rng.randint(0, 9))}

        for _ in range(150):
            parent = Behavior(_name(), patches(), parent=getattr(Behavior, rng.choice(PRESETS)))
            child = Behavior(_name(), patches(), parent=parent)
            _assert_resolves_as_keywords_alone(parent)
            _assert_resolves_as_keywords_alone(child)


# -- Combinations no keyword can name -----------------------------------------

_CRITICAL = dict(
    total_timeout=timedelta(seconds=5),
    retry_delay=timedelta(milliseconds=50),
    commit_level=CommitLevel.COMMIT_ALL,
)

# (on entries, [(kinds, shape, mode, fields)]): the entries, and the same
# targets written out as cell rules to apply over DEFAULT.
_COMBINATIONS = {
    "batch-ap-reads-and-retryable-point-ap-writes": (
        [
            (Selector.reads().batch().ap(), Settings(read_mode_ap=ReadModeAP.ALL)),
            (Selector.writes().retryable().point().ap(), Settings(commit_level=CommitLevel.COMMIT_ALL)),
            (Selector.writes().ap().retryable().point(), Settings(total_timeout=timedelta(seconds=2))),
        ],
        [
            ((READ,), OpShape.BATCH, Mode.AP, dict(read_mode_ap=ReadModeAP.ALL)),
            ((RETRYABLE,), OpShape.POINT, Mode.AP, dict(
                commit_level=CommitLevel.COMMIT_ALL, total_timeout=timedelta(seconds=2),
            )),
        ],
    ),
    "point-ap-writes-by-retryability": (
        [
            (Selector.writes().retryable().point().ap(), Settings(**_CRITICAL, max_retries=4)),
            (Selector.writes().non_retryable().point().ap(), Settings(**_CRITICAL, max_retries=0)),
        ],
        [
            ((RETRYABLE,), OpShape.POINT, Mode.AP, dict(_CRITICAL, max_retries=4)),
            ((NON_RETRYABLE,), OpShape.POINT, Mode.AP, dict(_CRITICAL, max_retries=0)),
        ],
    ),
    "point-and-batch-ap-reads": (
        [
            (Selector.reads().point().ap(), Settings(
                total_timeout=timedelta(milliseconds=100), max_retries=1,
                retry_delay=timedelta(milliseconds=5), read_mode_ap=ReadModeAP.ALL,
            )),
            (Selector.reads().batch().ap(), Settings(
                total_timeout=timedelta(milliseconds=200), max_retries=2,
            )),
        ],
        [
            ((READ,), OpShape.POINT, Mode.AP, dict(
                total_timeout=timedelta(milliseconds=100), max_retries=1,
                retry_delay=timedelta(milliseconds=5), read_mode_ap=ReadModeAP.ALL,
            )),
            ((READ,), OpShape.BATCH, Mode.AP, dict(
                total_timeout=timedelta(milliseconds=200), max_retries=2,
            )),
        ],
    ),
    "retryable-point-ap-writes": (
        [(Selector.writes().retryable().point().ap(), Settings(max_retries=4))],
        [((RETRYABLE,), OpShape.POINT, Mode.AP, dict(max_retries=4))],
    ),
}


class TestCombinations:

    @pytest.mark.parametrize("name", list(_COMBINATIONS))
    def test_entries_change_exactly_the_targeted_cells(self, name):
        on, rules = _COMBINATIONS[name]
        behavior = _derive(on=on)
        for key in KEYS:
            expected = Behavior.DEFAULT.get_settings(*key)
            for kinds, shape, mode, fields in rules:
                if key[0] in kinds and key[1] is shape and key[2] is mode:
                    expected = dataclasses.replace(expected, **fields)
            assert behavior.get_settings(*key) == expected, key

    def test_point_write_entries_leave_background_query_writes_alone(self):
        on, _ = _COMBINATIONS["point-ap-writes-by-retryability"]
        behavior = _derive(on=on)
        for kind in WRITES:
            key = (kind, OpShape.QUERY, Mode.AP)
            assert behavior.get_settings(*key).total_timeout == (
                Behavior.DEFAULT.get_settings(*key).total_timeout
            )


# -- Field applicability ------------------------------------------------------

# One selector that covers none of the field's cells, and one that covers some.
_APPLICABILITY = [
    ("timeout_delay", timedelta(milliseconds=5), Selector.txn_roll(), Selector.reads(),
     "every operation, but not the transaction phases"),
    ("use_compression", True, Selector.txn_verify(), Selector.writes().batch(),
     "every operation, but not the transaction phases"),
    ("compression_threshold", 64, Selector.txn_roll(), Selector.all(),
     "every operation, but not the transaction phases"),
    ("error_detail_verbosity", 1, Selector.txn_verify(), Selector.reads().query(),
     "every operation, but not the transaction phases"),
    ("commit_level", CommitLevel.COMMIT_ALL, Selector.reads().batch().ap(), Selector.writes(),
     "writes on AP namespaces"),
    ("commit_level", CommitLevel.COMMIT_ALL, Selector.writes().sc(), Selector.writes().point().ap(),
     "writes on AP namespaces"),
    ("durable_delete", True, Selector.reads(), Selector.writes().query().sc(), "writes"),
    ("send_key", True, Selector.reads().point(), Selector.writes().retryable(), "writes"),
    ("read_mode_ap", ReadModeAP.ALL, Selector.writes().point(), Selector.writes(),
     "reads and batch writes on AP namespaces"),
    ("read_mode_ap", ReadModeAP.ALL, Selector.reads().sc(), Selector.reads().batch().ap(),
     "reads and batch writes on AP namespaces"),
    ("read_mode_sc", ReadModeSC.LINEARIZE, Selector.reads().ap(), Selector.reads().sc(),
     "reads and batch writes on SC namespaces, and the transaction verify phase"),
    ("read_mode_sc", ReadModeSC.LINEARIZE, Selector.txn_roll(), Selector.txn_verify(),
     "reads and batch writes on SC namespaces, and the transaction verify phase"),
    ("read_touch_ttl_percent", 50, Selector.reads().query(), Selector.reads().point(),
     "point and batch reads"),
    ("replica", Replica.MASTER, Selector.writes().point(), Selector.writes().batch(),
     "reads, batch writes, and the transaction phases"),
    ("replica", Replica.MASTER, Selector.writes().query().ap(), Selector.txn_roll(),
     "reads, batch writes, and the transaction phases"),
    ("allow_inline", True, Selector.reads().point(), Selector.writes().batch().sc(),
     "batch reads and writes"),
    ("allow_inline_ssd", True, Selector.writes().query(), Selector.reads().batch(),
     "batch reads and writes"),
    ("max_concurrent_nodes", 4, Selector.writes().query(), Selector.reads().query(),
     "batch reads and writes, and query reads"),
    ("max_concurrent_nodes", 4, Selector.reads().point(), Selector.writes().batch(),
     "batch reads and writes, and query reads"),
    ("record_queue_size", 100, Selector.writes().query(), Selector.reads().query().sc(),
     "query reads"),
    ("allow_scans_with_where", True, Selector.reads().batch(), Selector.reads(), "query reads"),
]

# Fields every cell reads, with a value to set each to.
_UNRESTRICTED_VALUES = {
    "total_timeout": timedelta(milliseconds=123),
    "socket_timeout": timedelta(milliseconds=45),
    "max_retries": 6,
    "retry_delay": timedelta(milliseconds=7),
}
_UNRESTRICTED = set(_UNRESTRICTED_VALUES)


class TestFieldApplicability:

    @pytest.mark.parametrize(
        ("field", "value", "excluding", "covering", "where"), _APPLICABILITY,
        ids=[f"{row[0]}-{row[2]}" for row in _APPLICABILITY],
    )
    def test_field_is_refused_where_it_cannot_apply_and_accepted_where_it_can(
        self, field, value, excluding, covering, where,
    ):
        settings = Settings(**{field: value})
        with pytest.raises(ValueError) as exc_info:
            _derive(on=[(excluding, settings)])
        assert str(exc_info.value) == f"{field} does not apply to {excluding}: it applies to {where}"
        _derive(on=[(covering, settings)])

    def test_every_settings_field_is_classified(self):
        fields = {field.name for field in dataclasses.fields(Settings)}
        assert set(_DOMAIN_OF) | _UNRESTRICTED == fields
        assert not set(_DOMAIN_OF) & _UNRESTRICTED
        assert {row[0] for row in _APPLICABILITY} == set(_DOMAIN_OF)

    def test_all_accepts_every_field(self):
        _derive(on=[(Selector.all(), Settings(
            commit_level=CommitLevel.COMMIT_ALL, read_mode_sc=ReadModeSC.LINEARIZE,
            record_queue_size=100, allow_inline=True, read_touch_ttl_percent=50,
        ))])

    def test_keywords_stay_permissive(self):
        behavior = _derive(reads=Settings(commit_level=CommitLevel.COMMIT_MASTER))
        assert behavior.get_settings(READ, OpShape.POINT, Mode.AP).commit_level == (
            CommitLevel.COMMIT_MASTER
        )

    def test_unset_fields_are_not_checked(self):
        _derive(on=[(Selector.txn_roll(), Settings(max_retries=3))])


def _field_cases():
    """Every Settings field with a value and a selector that covers some of its cells."""
    for field, value in _UNRESTRICTED_VALUES.items():
        yield field, value, Selector.all()
    seen = set()
    for field, value, _, covering, _ in _APPLICABILITY:
        if field not in seen:
            seen.add(field)
            yield field, value, covering


class TestEveryFieldResolvesThroughOn:

    @pytest.mark.parametrize(
        ("field", "value", "selector"), list(_field_cases()), ids=lambda value: str(value)[:30],
    )
    def test_field_set_through_an_entry_reads_back_on_the_selected_cells(self, field, value, selector):
        behavior = _derive(on=[(selector, Settings(**{field: value}))])
        cells = [key for key in KEYS if selector._matches(*key)]
        phases = [scope for scope in TXN_SCOPES if selector._matches_txn(scope)]
        assert cells or phases
        for key in cells:
            assert getattr(behavior.get_settings(*key), field) == value, key
        for scope in phases:
            assert getattr(_txn_settings(behavior, scope), field) == value, scope


# The applicability table against the policy builders: outside a field's
# cells, setting the field changes no policy the SDK builds for the cell.

_PROBE_VALUES = {
    "timeout_delay": (timedelta(milliseconds=77), timedelta(milliseconds=78)),
    "use_compression": (True, False),
    "compression_threshold": (77, 78),
    "error_detail_verbosity": (1, 2),
    "commit_level": (CommitLevel.COMMIT_MASTER, CommitLevel.COMMIT_ALL),
    "durable_delete": (True, False),
    "send_key": (True, False),
    "read_mode_ap": (ReadModeAP.ALL, ReadModeAP.ONE),
    "read_mode_sc": (ReadModeSC.LINEARIZE, ReadModeSC.ALLOW_REPLICA),
    "read_touch_ttl_percent": (50, 60),
    "replica": (Replica.RANDOM, Replica.MASTER),
    "allow_inline": (True, False),
    "allow_inline_ssd": (True, False),
    "max_concurrent_nodes": (3, 4),
    "record_queue_size": (77, 78),
    "allow_scans_with_where": (True, False),
}


def _snapshot(policy, depth: int = 0):
    """Every public value on a built policy, as comparable text."""
    if policy is None:
        return None
    if depth > 2:
        return type(policy).__name__
    values = {}
    for name in dir(policy):
        if name.startswith("_"):
            continue
        try:
            value = getattr(policy, name)
        except Exception as e:
            value = type(e).__name__
        if callable(value):
            continue
        text = repr(value)
        values[name] = _snapshot(value, depth + 1) if " object at 0x" in text else text
    return values


def _built_policies(cell, settings: Settings) -> list:
    """Snapshots of each policy the SDK builds for *cell* when it resolves to *settings*."""
    if cell is Scope.SYSTEM_TXN_VERIFY:
        return [_snapshot(policy_mapper.to_txn_verify_policy(settings))]
    if cell is Scope.SYSTEM_TXN_ROLL:
        return [_snapshot(policy_mapper.to_txn_roll_policy(settings))]
    kind, shape, mode = cell
    if kind is READ:
        if shape is OpShape.POINT:
            built = [policy_mapper.to_read_policy(settings), policy_mapper.to_read_operate_policy(settings)]
        elif shape is OpShape.BATCH:
            built = [policy_mapper.to_batch_policy(settings), policy_mapper.to_batch_read_policy(settings)]
        else:
            built = [policy_mapper.to_query_policy(settings)]
        return [_snapshot(policy) for policy in built]
    retryable = kind is RETRYABLE
    if shape is OpShape.POINT:
        return [_snapshot(policy_mapper.to_write_policy(settings))]
    # A root behavior whose every cell resolves to exactly *settings*.
    behavior = Behavior(_name(), {Scope.ALL: settings})
    if shape is OpShape.QUERY:
        return [_snapshot(make_background_write_policy(
            behavior, None, None, namespace_mode=mode, retryable=retryable,
        ))]
    builder = QueryBuilder(client=object(), namespace="test", set_name="unit", behavior=behavior)
    key = Key("test", "unit", "k")
    rows = [
        builder._make_batch_write_policy(
            _OperationSpec(keys=[key], op_type=op_type, retryable=retryable), mode,
        )
        for op_type in ("upsert", "delete")
    ]
    return [_snapshot(policy_mapper.to_batch_policy(settings))] + [_snapshot(row) for row in rows]


def _default_settings(cell) -> Settings:
    if isinstance(cell, Scope):
        return _txn_settings(Behavior.DEFAULT, cell)
    return Behavior.DEFAULT.get_settings(*cell)


def _in_domain(field: str, cell) -> bool:
    _, applies, txn_phases = _DOMAIN_OF[field]
    return cell in txn_phases if isinstance(cell, Scope) else applies(*cell)


def _reaches_built_policy(field: str, cell) -> bool:
    base = _default_settings(cell)
    value = next(v for v in _PROBE_VALUES[field] if v != getattr(base, field))
    changed = dataclasses.replace(base, **{field: value})
    return _built_policies(cell, base) != _built_policies(cell, changed)


def _carried_outside_its_domain(field: str, cell) -> bool:
    """The two documented cases of a field reaching a policy where the server ignores it.

    A mode-specific field on the other mode's cells: the builders pass it
    whatever the namespace mode. And a write-only field on point reads: an
    operate that only reads builds its policy on a ``WritePolicy``.
    """
    if isinstance(cell, Scope):
        return False
    kind, shape, mode = cell
    reads_or_batch_writes = kind is READ or shape is OpShape.BATCH
    if field == "commit_level" and kind in WRITES and mode is Mode.SC:
        return True
    if field == "read_mode_ap" and reads_or_batch_writes and mode is Mode.SC:
        return True
    if field == "read_mode_sc" and reads_or_batch_writes and mode is Mode.AP:
        return True
    return (
        field in ("commit_level", "durable_delete", "send_key")
        and kind is READ and shape is OpShape.POINT
    )


class TestApplicabilityFollowsTheBuilders:

    @pytest.mark.parametrize("field", sorted(_DOMAIN_OF))
    def test_field_is_inert_outside_its_cells(self, field):
        reached = [
            cell for cell in [*KEYS, *TXN_SCOPES]
            if not _in_domain(field, cell) and _reaches_built_policy(field, cell)
        ]
        expected = [
            cell for cell in [*KEYS, *TXN_SCOPES]
            if not _in_domain(field, cell) and _carried_outside_its_domain(field, cell)
        ]
        assert reached == expected

    def test_probe_values_cover_every_restricted_field(self):
        assert set(_PROBE_VALUES) == set(_DOMAIN_OF)
