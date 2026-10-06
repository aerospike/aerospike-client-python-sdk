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

"""A builder takes one ``where()`` per operation; a second call is an error.

Silently replacing or ignoring the first predicate would run a filter the
caller did not ask for, so every builder that accepts ``where()`` refuses a
second one. A chained operation starts fresh, so it may set its own.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk import Exp, Key
from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder,
    BackgroundUdfFunctionBuilder,
    _OpType,
)
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.background import (
    BackgroundOperationBuilder as SyncBackgroundOperationBuilder,
)
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder

_EXP_A = Exp.eq(Exp.int_bin("a"), Exp.int_val(1))
_EXP_B = Exp.eq(Exp.int_bin("b"), Exp.int_val(2))
_AEL = "$.c == 3"

_SECOND_CALLS = [
    pytest.param(_EXP_A, _EXP_B, id="exp-then-exp"),
    pytest.param(_EXP_A, _AEL, id="exp-then-ael"),
    pytest.param(_AEL, _EXP_B, id="ael-then-exp"),
    pytest.param(_AEL, "$.d == 4", id="ael-then-ael"),
]


def _key(val: int = 1) -> Key:
    return Key("test", "t", val)


def _session_mock() -> MagicMock:
    s = MagicMock()
    s.behavior = Behavior.DEFAULT
    s.current_transaction = None
    s._client = MagicMock()
    s._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return s


def _query_builder(qb_cls=QueryBuilder):
    return qb_cls(client=MagicMock(), namespace="test", set_name="t")


def _multi_key_segment():
    qb = _query_builder()
    qb._op_type = "upsert"
    qb._single_key = _key()
    return WriteSegmentBuilder(qb)


def _single_key_segment():
    return _SingleKeyWriteSegment(
        client=MagicMock(),
        key=_key(),
        op_type="upsert",
        behavior=Behavior.DEFAULT,
        write_policy=None,
    )


def _udf_segment():
    qb = _query_builder()
    qb._set_current_keys_from_varargs((_key(),))
    return UdfFunctionBuilder(qb).function("pkg", "fn")


def _background_operation():
    return BackgroundOperationBuilder(
        _session_mock(), DataSet.of("test", "bgset"), _OpType.UPDATE,
    )


def _background_udf():
    return BackgroundUdfFunctionBuilder(
        _session_mock(), DataSet.of("test", "bgset"),
    ).function("pkg", "fn")


def _sync_background_operation():
    return SyncBackgroundOperationBuilder(_background_operation())


_BUILDERS = [
    pytest.param(_query_builder, id="query"),
    pytest.param(lambda: _query_builder(SyncQueryBuilder), id="sync-query"),
    pytest.param(_multi_key_segment, id="write-segment"),
    pytest.param(_single_key_segment, id="single-key-write-segment"),
    pytest.param(_udf_segment, id="udf-segment"),
    pytest.param(_background_operation, id="background-operation"),
    pytest.param(_background_udf, id="background-udf"),
    pytest.param(_sync_background_operation, id="sync-background-operation"),
]


@pytest.mark.parametrize("make_builder", _BUILDERS)
def test_second_where_raises(make_builder):
    builder = make_builder().where(_EXP_A)
    with pytest.raises(ValueError, match=r"where\(\) can only be called once"):
        builder.where(_AEL)


@pytest.mark.parametrize("first, second", _SECOND_CALLS)
def test_query_second_where_raises_in_any_order(first, second):
    """The query builder compiles AEL lazily, so each pairing takes its own path."""
    builder = _query_builder().where(first)
    with pytest.raises(ValueError, match=r"where\(\) can only be called once"):
        builder.where(second)


def test_next_chained_operation_takes_its_own_where():
    segment = _multi_key_segment().where(_EXP_A)
    next_segment = segment.upsert(_key(2)).where(_EXP_B)
    assert next_segment._qb._filter_expression is _EXP_B
    assert next_segment._qb._specs[0].filter_expression is _EXP_A
