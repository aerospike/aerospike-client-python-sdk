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

"""Unit tests for durable-delete resolution and policy wiring (Phase 1 TDD)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from aerospike_sdk.aio.operations.batch import BatchOperationBuilder
from aerospike_sdk.aio.operations.query import QueryBuilder, _OperationSpec
from aerospike_sdk.background_shared import make_background_write_policy
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, Settings
from aerospike_sdk.policy.policy_mapper import resolve_durable_delete


def _make_key(i: int = 1):
    from aerospike_async import Key

    return Key("test", "unit", f"k{i}")


def _make_qb(**kwargs) -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit", **kwargs)


class TestResolveDurableDelete:
    @pytest.mark.parametrize(
        ("setting", "command_default", "override", "expected"),
        [
            (False, False, False, False),
            (False, False, True, True),
            (False, True, False, False),
            (False, True, True, True),
            (True, False, False, False),
            (True, False, True, True),
            (True, True, False, False),
            (True, True, True, True),
        ],
    )
    def test_resolver_override_wins_over_default_and_setting(
        self, setting, command_default, override, expected,
    ):
        assert (
            resolve_durable_delete(setting, command_default, override) is expected
        )

    def test_resolver_default_wins_over_setting_when_no_override(self):
        assert resolve_durable_delete(True, False, None) is False
        assert resolve_durable_delete(False, True, None) is True

    def test_resolver_none_setting_falls_through_to_false(self):
        assert resolve_durable_delete(None, None, None) is False


class TestMakeWritePolicy:
    def test_make_write_policy_picks_setting_when_no_spec_override(self):
        behavior = Behavior.DEFAULT.derive_with_changes(
            "writes_durable",
            writes=Settings(durable_delete=True),
        )
        qb = _make_qb(behavior=behavior)
        qb._namespace_mode = Mode.AP
        qb._base_write_policy = None
        spec = _OperationSpec(keys=[_make_key()], op_type="delete", durable_delete=None)
        wp = qb._make_write_policy(spec)
        assert wp.durable_delete is True

    def test_make_write_policy_explicit_false_must_override_behavior_true(self):
        """Explicit non-durable must win when behavior requests durable deletes."""
        behavior = Behavior.DEFAULT.derive_with_changes(
            "writes_durable",
            writes=Settings(durable_delete=True),
        )
        qb = _make_qb(behavior=behavior)
        qb._namespace_mode = Mode.AP
        qb._base_write_policy = None
        spec = _OperationSpec(keys=[_make_key()], op_type="delete", durable_delete=False)
        wp = qb._make_write_policy(spec)
        assert wp.durable_delete is False


class TestBatchPolicies:
    def test_make_batch_delete_policy_when_only_spec_durable(self):
        spec = _OperationSpec(
            keys=[_make_key()],
            op_type="delete",
            durable_delete=True,
        )
        qb = _make_qb()
        qb._namespace_mode = Mode.AP
        bdp = qb._make_batch_delete_policy(spec)
        assert bdp is not None
        assert bdp.durable_delete is True

    def test_make_batch_write_policy_when_only_spec_durable(self):
        spec = _OperationSpec(
            keys=[_make_key()],
            op_type="upsert",
            durable_delete=True,
            contains_record_delete_op=True,
        )
        qb = _make_qb()
        qb._namespace_mode = Mode.AP
        bwp = qb._make_batch_write_policy(spec)
        assert bwp is not None
        assert bwp.durable_delete is True


@pytest.mark.asyncio
class TestBatchOperationBuilderPolicies:
    async def test_batch_delete_policy_arg_non_none_when_batch_writes_durable(self):
        mock_client = MagicMock()
        mock_client.batch_delete = AsyncMock(return_value=[])
        mock_client.batch_operate = AsyncMock(return_value=[])

        behavior = Behavior.DEFAULT.derive_with_changes(
            "batch_durable",
            writes_batch=Settings(durable_delete=True),
        )
        bob = BatchOperationBuilder(mock_client, behavior=behavior)
        k = _make_key()
        await bob.delete(k).execute()

        mock_client.batch_delete.assert_awaited()
        call = mock_client.batch_delete.await_args
        delete_policy = call.kwargs["delete_policy"]
        assert delete_policy is not None

    async def test_batch_delete_policy_uses_sc_namespace_mode(self):
        mock_client = MagicMock()
        mock_client.batch_delete = AsyncMock(return_value=[])
        mock_client.batch_operate = AsyncMock(return_value=[])

        async def resolve_mode(namespace: str) -> Mode:
            assert namespace == "test"
            return Mode.SC

        behavior = Behavior.DEFAULT.derive_with_changes(
            "sc_batch_durable",
            writes_sc=Settings(durable_delete=True),
        )
        bob = BatchOperationBuilder(
            mock_client,
            behavior=behavior,
            namespace_mode_resolver=resolve_mode,
        )
        await bob.delete(_make_key()).execute()

        mock_client.batch_delete.assert_awaited()
        delete_policy = mock_client.batch_delete.await_args.kwargs["delete_policy"]
        assert delete_policy is not None
        assert delete_policy.durable_delete is True


class TestUdfSpec:
    def test_finalize_udf_spec_carries_durable_delete_on_spec(self):
        qb = _make_qb()
        qb._single_key = _make_key()
        qb._udf_package = "pkg"
        qb._udf_function = "fn"
        qb._durable_delete = True
        qb._durable_delete_command_default = True
        qb._finalize_udf_spec()
        assert len(qb._specs) == 1
        assert qb._specs[0].durable_delete is True
        assert qb._specs[0].durable_delete_command_default is True

    def test_make_udf_write_policy_explicit_false_overrides_sc_behavior(self):
        behavior = Behavior.DEFAULT.derive_with_changes(
            "sc_udf_durable",
            writes_sc=Settings(durable_delete=True),
        )
        qb = _make_qb(behavior=behavior)
        qb._namespace_mode = Mode.SC
        spec = _OperationSpec(
            keys=[_make_key()],
            op_type="udf",
            durable_delete=False,
            udf_package="pkg",
            udf_function="fn",
        )
        wp = qb._make_udf_write_policy(spec)
        assert wp.durable_delete is False

    def test_make_batch_udf_policy_uses_sc_behavior(self):
        behavior = Behavior.DEFAULT.derive_with_changes(
            "sc_batch_udf_durable",
            writes_sc=Settings(durable_delete=True),
        )
        qb = _make_qb(behavior=behavior)
        qb._namespace_mode = Mode.SC
        spec = _OperationSpec(
            keys=[_make_key(1), _make_key(2)],
            op_type="udf",
            udf_package="pkg",
            udf_function="fn",
        )
        policy = qb._make_batch_udf_policy(spec)
        assert policy is not None
        assert policy.durable_delete is True


class TestBackgroundWritePolicy:
    def test_background_write_policy_carries_writes_query_durable(self):
        behavior = Behavior.DEFAULT.derive_with_changes(
            "bg_durable",
            writes_query=Settings(durable_delete=True),
        )
        wp = make_background_write_policy(behavior, None, None, None)
        assert wp.durable_delete is True


class TestTouchSegment:
    def test_touch_policy_should_ignore_spec_durable_delete(self):
        """Touch must not honor durable-delete bits on the spec (Phase 3f)."""
        behavior = Behavior.DEFAULT.derive_with_changes(
            "writes_durable",
            writes=Settings(durable_delete=True),
        )
        qb = _make_qb(behavior=behavior)
        qb._namespace_mode = Mode.AP
        qb._base_write_policy = None
        spec = _OperationSpec(
            keys=[_make_key()],
            op_type="touch",
            durable_delete=True,
        )
        wp = qb._make_write_policy(spec)
        assert wp.durable_delete is False
