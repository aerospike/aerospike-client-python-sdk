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

"""Builder-level unit tests for the durable-delete quartet.

Each write-shaped builder must expose:

* ``with_durable_delete()`` — operation override, force durable
* ``without_durable_delete()`` — operation override, force non-durable
* ``default_with_durable_delete()`` — builder default, prefer durable when
  resolving behavior settings (typically used for SC namespaces)
* ``default_without_durable_delete()`` — builder default, prefer non-durable

These tests assert each method sets the expected internal flag and returns
the builder for chaining. End-to-end semantics (server-side tombstones,
SC enforcement, batch wiring) live in the integration suite.
"""

from unittest.mock import AsyncMock, MagicMock

from aerospike_async import Key

from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder,
    BackgroundUdfBuilder,
    _OpType,
)
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.aio.operations.udf import UdfBuilder
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.operations.query import SyncWriteSegmentBuilder


def _key(val: int = 1) -> Key:
    return Key("test", "test", val)


def _session_mock() -> MagicMock:
    s = MagicMock()
    s.behavior = Behavior.DEFAULT
    fc = MagicMock()
    fc._client = MagicMock()
    s.client = fc
    s._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return s


class TestWriteSegmentBuilder:
    """``WriteSegmentBuilder`` writes through the wrapped ``QueryBuilder``."""

    @staticmethod
    def _make():
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="t")
        qb._op_type = "upsert"
        qb._single_key = _key()
        return WriteSegmentBuilder(qb), qb

    def test_with_durable_delete_sets_override_true(self):
        wsb, qb = self._make()
        assert qb._durable_delete is None
        assert wsb.with_durable_delete() is wsb
        assert qb._durable_delete is True

    def test_without_durable_delete_sets_override_false(self):
        wsb, qb = self._make()
        assert wsb.without_durable_delete() is wsb
        assert qb._durable_delete is False

    def test_default_with_durable_delete_sets_command_default(self):
        wsb, qb = self._make()
        assert qb._durable_delete_command_default is None
        assert wsb.default_with_durable_delete() is wsb
        assert qb._durable_delete_command_default is True
        # The override flag must remain unset — these two flags are distinct.
        assert qb._durable_delete is None

    def test_default_without_durable_delete_sets_command_default_false(self):
        wsb, qb = self._make()
        assert qb._durable_delete_command_default is None
        assert wsb.default_without_durable_delete() is wsb
        assert qb._durable_delete_command_default is False
        assert qb._durable_delete is None


class TestSingleKeyWriteSegment:
    """``_SingleKeyWriteSegment`` has fast-path slot fields used until promoted.

    Once a feature requires a full ``QueryBuilder`` (e.g. ``where``, TTL,
    multi-op chaining), the segment promotes itself by populating ``self._qb``
    and inherited ``WriteSegmentBuilder`` methods take over.
    """

    @staticmethod
    def _make():
        seg = _SingleKeyWriteSegment(
            client=MagicMock(),
            key=_key(),
            op_type="upsert",
            behavior=Behavior.DEFAULT,
            write_policy=None,
        )
        return seg

    def test_fast_path_with_durable_delete_sets_dd_override_true(self):
        seg = self._make()
        assert seg._dd_override is None
        assert seg.with_durable_delete() is seg
        assert seg._dd_override is True
        assert seg._qb is None  # still on fast path

    def test_fast_path_without_durable_delete_sets_dd_override_false(self):
        seg = self._make()
        assert seg.without_durable_delete() is seg
        assert seg._dd_override is False
        assert seg._qb is None

    def test_fast_path_default_with_sets_dd_command_default_true(self):
        seg = self._make()
        assert seg._dd_command_default is None
        assert seg.default_with_durable_delete() is seg
        assert seg._dd_command_default is True
        assert seg._dd_override is None  # distinct from override flag

    def test_fast_path_default_without_sets_dd_command_default_false(self):
        seg = self._make()
        assert seg._dd_command_default is None
        assert seg.default_without_durable_delete() is seg
        assert seg._dd_command_default is False
        assert seg._dd_override is None

    def test_promoted_path_delegates_to_query_builder(self):
        # Force promotion by assigning a QB. Once _qb is set, the methods
        # delegate via super() and write to qb._durable_delete*.
        seg = self._make()
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="t")
        qb._op_type = "delete"
        qb._single_key = _key()
        seg._qb = qb
        assert seg.with_durable_delete() is seg
        assert qb._durable_delete is True
        assert seg.default_with_durable_delete() is seg
        assert qb._durable_delete_command_default is True
        assert seg.without_durable_delete() is seg
        assert qb._durable_delete is False
        assert seg.default_without_durable_delete() is seg
        assert qb._durable_delete_command_default is False


class TestBackgroundOperationBuilder:

    @staticmethod
    def _make(op_type: _OpType = _OpType.DELETE):
        return BackgroundOperationBuilder(_session_mock(), DataSet.of("test", "bg"), op_type)

    def test_with_durable_delete_sets_override_true(self):
        b = self._make()
        assert b._durable_delete_override is None
        assert b.with_durable_delete() is b
        assert b._durable_delete_override is True

    def test_without_durable_delete_sets_override_false(self):
        b = self._make()
        assert b.without_durable_delete() is b
        assert b._durable_delete_override is False

    def test_default_with_sets_command_default_true(self):
        b = self._make()
        assert b._durable_delete_command_default is None
        assert b.default_with_durable_delete() is b
        assert b._durable_delete_command_default is True
        assert b._durable_delete_override is None  # distinct from override flag

    def test_default_without_sets_command_default_false(self):
        b = self._make()
        assert b.default_without_durable_delete() is b
        assert b._durable_delete_command_default is False
        assert b._durable_delete_override is None


class TestBackgroundUdfBuilder:

    @staticmethod
    def _make():
        return BackgroundUdfBuilder(
            _session_mock(), DataSet.of("test", "bgu"), "pkg", "fn",
        )

    def test_with_durable_delete_sets_override_true(self):
        b = self._make()
        assert b._durable_delete_override is None
        assert b.with_durable_delete() is b
        assert b._durable_delete_override is True

    def test_without_durable_delete_sets_override_false(self):
        b = self._make()
        assert b.without_durable_delete() is b
        assert b._durable_delete_override is False

    def test_default_with_sets_command_default_true(self):
        b = self._make()
        assert b.default_with_durable_delete() is b
        assert b._durable_delete_command_default is True
        assert b._durable_delete_override is None

    def test_default_without_sets_command_default_false(self):
        b = self._make()
        assert b.default_without_durable_delete() is b
        assert b._durable_delete_command_default is False
        assert b._durable_delete_override is None


class TestUdfBuilder:
    """``UdfBuilder`` wraps a ``QueryBuilder``; flags land on the QB."""

    @staticmethod
    def _make():
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="udf")
        qb._op_type = "execute_udf"
        qb._single_key = _key()
        return UdfBuilder(qb), qb

    def test_with_durable_delete_sets_qb_override_true(self):
        b, qb = self._make()
        assert qb._durable_delete is None
        assert b.with_durable_delete() is b
        assert qb._durable_delete is True

    def test_without_durable_delete_sets_qb_override_false(self):
        b, qb = self._make()
        assert b.without_durable_delete() is b
        assert qb._durable_delete is False

    def test_default_with_sets_qb_command_default_true(self):
        b, qb = self._make()
        assert qb._durable_delete_command_default is None
        assert b.default_with_durable_delete() is b
        assert qb._durable_delete_command_default is True
        assert qb._durable_delete is None

    def test_default_without_sets_qb_command_default_false(self):
        b, qb = self._make()
        assert b.default_without_durable_delete() is b
        assert qb._durable_delete_command_default is False
        assert qb._durable_delete is None


class TestSyncWriteSegmentBuilder:
    """SyncWriteSegmentBuilder is a native subclass of ``_WriteSegmentBuilderBase``;
    durable-delete methods mutate the wrapped builder's state directly."""

    @staticmethod
    def _make():
        from aerospike_sdk.sync.operations.query import SyncQueryBuilder
        qb = SyncQueryBuilder(client=MagicMock(), namespace="test", set_name="t")
        qb._op_type = "upsert"
        qb._single_key = _key()
        return SyncWriteSegmentBuilder(qb), qb

    def test_with_durable_delete_forwards(self):
        sync_wsb, qb = self._make()
        assert sync_wsb.with_durable_delete() is sync_wsb
        assert qb._durable_delete is True

    def test_without_durable_delete_forwards(self):
        sync_wsb, qb = self._make()
        assert sync_wsb.without_durable_delete() is sync_wsb
        assert qb._durable_delete is False

    def test_default_with_durable_delete_forwards(self):
        sync_wsb, qb = self._make()
        assert sync_wsb.default_with_durable_delete() is sync_wsb
        assert qb._durable_delete_command_default is True

    def test_default_without_durable_delete_forwards(self):
        sync_wsb, qb = self._make()
        assert sync_wsb.default_without_durable_delete() is sync_wsb
        assert qb._durable_delete_command_default is False
