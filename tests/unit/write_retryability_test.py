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

"""A write resolves the retryable or non-retryable scope from its operations.

A write whose operations are all idempotent (puts, deletes, touches, reads)
resolves ``WRITE_RETRYABLE``; one that carries ``add``, ``append``,
``prepend``, any list, map, bit, HLL, string, or expression modify, or a UDF
resolves ``WRITE_NON_RETRYABLE``. A batch is retryable only when every write
row is. A background job classifies by its operations the same way.

The probe is ``send_key``: it is set on exactly one of the two scopes, so the
policy a write hands to PNC shows which scope resolved. The retry budget under
``Behavior.DEFAULT`` is checked directly as well.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_native import Operation

import aerospike_sdk.query_shared as query_shared

from aerospike_sdk import Behavior, DataSet, Key
from aerospike_sdk.aio.session import Session
from aerospike_sdk.policy import Settings
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.sdk_config_loader import fill_hard_defaults
from aerospike_sdk.policy.system_settings import SystemSettings, TransactionSettings
from aerospike_sdk.sync.session import Session as SyncSession

NS = "test"
DS = DataSet.of(NS, "retry")

RETRYABLE_MARKED = Behavior.DEFAULT.derive_with_changes(
    "retryable-marked", writes_retryable=Settings(send_key=True),
)
NON_RETRYABLE_MARKED = Behavior.DEFAULT.derive_with_changes(
    "non-retryable-marked", writes_non_retryable=Settings(send_key=True),
)


def _key(i: int = 1) -> Key:
    return Key(NS, "retry", i)


class _Record:
    bins: dict = {}
    generation = 1
    ttl = 0


class _BatchRecord:
    def __init__(self, key):
        self.key = key
        self.record = None
        self.result_code = 0
        self.in_doubt = False
        self.sub_code = None
        self.server_message = None
        self.exp_trace = None


class _CapturedWriteOp:
    def __init__(self, key, operations, policy=None, bins=None):
        self.key = key
        self.operations = operations
        self.policy = policy
        self.bins = bins


class _CapturedDeleteOp:
    def __init__(self, key, policy=None):
        self.key = key
        self.policy = policy


@pytest.fixture(autouse=True)
def _capture_batch_ops(monkeypatch):
    monkeypatch.setattr(query_shared, "BatchWriteOp", _CapturedWriteOp)
    monkeypatch.setattr(query_shared, "BatchDeleteOp", _CapturedDeleteOp)


class _FakePnc:
    """Records every write-bearing PNC call on both the async and blocking surfaces."""

    def __init__(self):
        self.calls: list = []

    def _record(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))

    # -- point --
    async def put(self, key, bins, **kw):
        self._record("put", key, bins, **kw)

    def put_blocking(self, key, bins, **kw):
        self._record("put", key, bins, **kw)

    async def operate(self, key, ops, **kw):
        self._record("operate", key, ops, **kw)
        return _Record()

    def operate_blocking(self, key, ops, **kw):
        self._record("operate", key, ops, **kw)
        return _Record()

    async def delete(self, key, **kw):
        self._record("delete", key, **kw)
        return True

    def delete_blocking(self, key, **kw):
        self._record("delete", key, **kw)
        return True

    async def touch(self, key, **kw):
        self._record("touch", key, **kw)

    def touch_blocking(self, key, **kw):
        self._record("touch", key, **kw)

    async def execute_udf(self, key, package, function, args, **kw):
        self._record("execute_udf", key, **kw)
        return None

    def execute_udf_blocking(self, key, package, function, args, **kw):
        self._record("execute_udf", key, **kw)
        return None

    # -- batch --
    async def batch_operate(self, keys, ops_per_key, **kw):
        self._record("batch_operate", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    def batch_operate_blocking(self, keys, ops_per_key, **kw):
        self._record("batch_operate", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    async def batch_delete(self, keys, **kw):
        self._record("batch_delete", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    def batch_delete_blocking(self, keys, **kw):
        self._record("batch_delete", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    async def batch(self, ops, **kw):
        self._record("batch", ops, **kw)
        return [_BatchRecord(op.key) for op in ops]

    def batch_blocking(self, ops, **kw):
        self._record("batch", ops, **kw)
        return [_BatchRecord(op.key) for op in ops]

    async def batch_apply(self, keys, package, function, args, **kw):
        self._record("batch_apply", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    def batch_apply_blocking(self, keys, package, function, args, **kw):
        self._record("batch_apply", keys, **kw)
        return [_BatchRecord(k) for k in keys]

    # -- background --
    async def query_operate(self, statement, ops, **kw):
        self._record("query_operate", statement, ops, **kw)
        return MagicMock()

    def query_operate_blocking(self, statement, ops, **kw):
        self._record("query_operate", statement, ops, **kw)
        return MagicMock()

    async def query_execute_udf(self, statement, package, function, args, **kw):
        self._record("query_execute_udf", statement, **kw)
        return MagicMock()

    def query_execute_udf_blocking(self, statement, package, function, args, **kw):
        self._record("query_execute_udf", statement, **kw)
        return MagicMock()


def _sdk_client(pnc):
    settings = fill_hard_defaults(SystemSettings(
        transactions=TransactionSettings(implicit_batch_write_transactions=False),
    ))
    client = SimpleNamespace(
        _client=pnc,
        _async_client=pnc,
        underlying_client=pnc,
        _namespace_mode_cache={NS: Mode.AP},
        _sdk_settings=settings,
        supports_server_compiled_ael=False,
        supports_query_selection=False,
        _usage_on=False,
        _record_on=False,
        _cmd_count_on=False,
    )
    client._supports_mrt = lambda: False
    client._resolve_namespace_mode_blocking = lambda ns: Mode.AP
    client._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return client


def _async_session(behavior):
    pnc = _FakePnc()
    return Session(client=_sdk_client(pnc), behavior=behavior), pnc


def _sync_session(behavior):
    pnc = _FakePnc()
    return SyncSession(client=_sdk_client(pnc), behavior=behavior), pnc


def _sent_write_policies(pnc) -> list:
    """Every policy object a write handed to PNC, parent batch policies included."""
    out = []
    for name, args, kw in pnc.calls:
        for field in ("policy", "write_policy", "delete_policy", "udf_policy", "batch_policy"):
            if kw.get(field) is not None:
                out.append(kw[field])
        if name == "batch":
            out.extend(op.policy for op in args[0] if op.policy is not None)
    assert out, f"no policy reached PNC: {pnc.calls}"
    return out


_ROW_POLICY_FIELD = {
    "put": "policy", "operate": "policy", "delete": "policy", "touch": "policy",
    "execute_udf": "policy", "batch_operate": "write_policy",
    "batch_delete": "delete_policy", "batch_apply": "udf_policy",
    "query_operate": "write_policy", "query_execute_udf": "write_policy",
}


def _row_send_keys(pnc) -> list:
    """``send_key`` per row-level policy, in dispatch order.

    A batch row with nothing to say travels without a policy, which PNC
    reads as ``send_key`` off, so an absent row policy counts as ``False``.
    """
    rows: list = []
    for name, args, kw in pnc.calls:
        if name == "batch":
            rows.extend(bool(op.policy.send_key) if op.policy is not None else False
                        for op in args[0])
        else:
            policy = kw.get(_ROW_POLICY_FIELD[name])
            rows.append(bool(policy.send_key) if policy is not None else False)
    assert rows, f"no write reached PNC: {pnc.calls}"
    return rows


def _row_send_key(pnc) -> bool:
    flags = set(_row_send_keys(pnc))
    assert len(flags) == 1, f"rows disagree on send_key: {pnc.calls}"
    return flags.pop()


# Each case: (id, build, expected_retryable). ``build`` returns an executable.
_CASES = [
    ("upsert-set_to", lambda s: s.upsert(_key()).bin("v").set_to(1), True),
    ("insert-set_to", lambda s: s.insert(_key()).bin("v").set_to(1), True),
    ("update-set_to", lambda s: s.update(_key()).bin("v").set_to(1), True),
    ("upsert-put", lambda s: s.upsert(_key()).put({"v": 1}), True),
    ("upsert-remove_bin", lambda s: s.upsert(_key()).remove_bin("v"), True),
    ("delete", lambda s: s.delete(_key()), True),
    ("touch", lambda s: s.touch(_key()), True),
    ("upsert-add", lambda s: s.upsert(_key()).bin("v").add(1), False),
    ("upsert-append", lambda s: s.upsert(_key()).bin("v").append("x"), False),
    ("upsert-prepend", lambda s: s.upsert(_key()).bin("v").prepend("x"), False),
    ("upsert-list_append", lambda s: s.upsert(_key()).bin("l").list_append(1), False),
    ("upsert-map_upsert_items",
     lambda s: s.upsert(_key()).bin("m").map_upsert_items({"k": 1}), False),
    ("upsert-hll_add", lambda s: s.upsert(_key()).bin("h").hll_add([1]), False),
    ("upsert-set_to-then-add",
     lambda s: s.upsert(_key()).bin("v").set_to(1).bin("n").add(1), False),
    ("upsert-raw-put", lambda s: s.upsert(_key()).add_operation(Operation.put("v", 1)), True),
    ("upsert-raw-add", lambda s: s.upsert(_key()).add_operation(Operation.add("v", 1)), False),
    ("udf", lambda s: s.execute_udf(_key()).function("pkg", "fn"), False),
    ("batch-upsert-set_to", lambda s: s.upsert(_key(1), _key(2)).bin("v").set_to(1), True),
    ("batch-upsert-add", lambda s: s.upsert(_key(1), _key(2)).bin("v").add(1), False),
    ("batch-delete", lambda s: s.delete(_key(1), _key(2)), True),
    ("batch-touch", lambda s: s.touch(_key(1), _key(2)), True),
    # Rows resolve their own kind; the parent budget is checked separately below.
    ("chain-set_to-then-add-row",
     lambda s: s.upsert(_key(1)).bin("v").set_to(1).upsert(_key(2)).bin("v").add(1),
     [True, False]),
    ("chain-set_to-twice",
     lambda s: s.upsert(_key(1)).bin("v").set_to(1).upsert(_key(2)).bin("v").set_to(2), True),
    ("background-update-set_to",
     lambda s: s.background_task().update(DS).bin("v").set_to(1), True),
    ("background-update-add",
     lambda s: s.background_task().update(DS).bin("v").add(1), False),
    ("background-delete", lambda s: s.background_task().delete(DS), True),
    ("background-udf",
     lambda s: s.background_task().execute_udf(DS).function("pkg", "fn"), False),
]
_CASE_PARAMS = [pytest.param(build, expected, id=name) for name, build, expected in _CASES]


async def _run(executable):
    result = executable.execute()
    if hasattr(result, "__await__"):
        result = await result
    return result


@pytest.mark.parametrize("build, expected_retryable", _CASE_PARAMS)
@pytest.mark.parametrize("make_session", [_async_session, _sync_session], ids=["async", "sync"])
async def test_write_resolves_the_scope_its_operations_earn(
    make_session, build, expected_retryable,
):
    session, pnc = make_session(RETRYABLE_MARKED)
    await _run(build(session))
    if isinstance(expected_retryable, list):
        assert _row_send_keys(pnc) == expected_retryable
    else:
        assert _row_send_key(pnc) is expected_retryable

    session, pnc = make_session(NON_RETRYABLE_MARKED)
    await _run(build(session))
    if isinstance(expected_retryable, list):
        assert _row_send_keys(pnc) == [not r for r in expected_retryable]
    else:
        assert _row_send_key(pnc) is (not expected_retryable)


@pytest.mark.parametrize("make_session", [_async_session, _sync_session], ids=["async", "sync"])
async def test_session_put_is_retryable(make_session):
    session, pnc = make_session(RETRYABLE_MARKED)
    await _run_put(session)
    assert _row_send_key(pnc) is True

    session, pnc = make_session(NON_RETRYABLE_MARKED)
    await _run_put(session)
    assert _row_send_key(pnc) is False


async def _run_put(session):
    result = session.put(_key(), {"v": 1})
    if hasattr(result, "__await__"):
        await result


@pytest.mark.parametrize("make_session", [_async_session, _sync_session], ids=["async", "sync"])
async def test_default_behavior_retries_only_idempotent_writes(make_session):
    session, pnc = make_session(Behavior.DEFAULT)
    await _run(session.upsert(_key()).bin("v").set_to(1))
    assert {p.max_retries for p in _sent_write_policies(pnc)} == {2}

    session, pnc = make_session(Behavior.DEFAULT)
    await _run(session.upsert(_key()).bin("v").add(1))
    assert {p.max_retries for p in _sent_write_policies(pnc)} == {0}


@pytest.mark.parametrize("make_session", [_async_session, _sync_session], ids=["async", "sync"])
async def test_default_behavior_batch_parent_follows_the_rows(make_session):
    session, pnc = make_session(Behavior.DEFAULT)
    await _run(session.upsert(_key(1), _key(2)).bin("v").set_to(1))
    parents = [kw["batch_policy"] for _, _, kw in pnc.calls if "batch_policy" in kw]
    assert [p.max_retries for p in parents] == [2]

    session, pnc = make_session(Behavior.DEFAULT)
    await _run(session.upsert(_key(1), _key(2)).bin("v").add(1))
    parents = [kw["batch_policy"] for _, _, kw in pnc.calls if "batch_policy" in kw]
    assert [p.max_retries for p in parents] == [0]

    # One non-idempotent row takes the whole batch off the retryable budget.
    session, pnc = make_session(Behavior.DEFAULT)
    await _run(session.upsert(_key(1)).bin("v").set_to(1).upsert(_key(2)).bin("v").add(1))
    parents = [kw["batch_policy"] for _, _, kw in pnc.calls if "batch_policy" in kw]
    assert [p.max_retries for p in parents] == [0]
