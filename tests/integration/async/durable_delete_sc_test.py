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

"""Integration tests for durable delete behavior on strong-consistency namespaces.

Environment (same knobs as ``txn_test`` / ``aerospike.env.example``):

- ``AEROSPIKE_HOST``: default integration seed for general tests (most of the suite).
- ``AEROSPIKE_HOST_SC``: optional seed **only** for SC/MRT/durable-delete tests; when set,
  those suites use it and ``AEROSPIKE_HOST`` should stay on your AP/default cluster.
  When unset, SC tests use ``AEROSPIKE_HOST`` (same as CI / single-cluster setups).
- ``AEROSPIKE_SC_NAMESPACE``: optional override when several namespaces are SC. If
  unset and the cluster has **exactly one** strong-consistency namespace, that name is
  used automatically. If none are SC (AP-only dev cluster), tests skip with that fact
  instead of assuming ``test_sc``. Skip text lists **namespaces on this cluster** when useful.
- ``enterprise_sc`` fixture: most tests require Enterprise Edition on the SC seed
  (``AEROSPIKE_HOST_SC`` or ``AEROSPIKE_HOST`` when SC is unset). The default SC point-delete test does not.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest
import pytest_asyncio
from aerospike_async import Filter, UDFLang
from aerospike_async.exceptions import ResultCode

from aerospike_sdk import Client, DataSet
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.error_strategy import ErrorStrategy
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Settings

try:
    from .durable_delete_support import delete_keys_durable
    from integration.sc_namespace_resolve import (
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        pinned_namespace_env_hint,
        resolve_sc_namespace,
        skip_reason_no_sc_namespace,
    )
except ImportError:
    # Running this file directly (`python durable_delete_sc_test.py`): no package context.
    import sys
    from pathlib import Path

    _here = Path(__file__).resolve().parent
    _tests = _here.parent.parent
    for p in (_tests, _here):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    from durable_delete_support import delete_keys_durable  # noqa: E402
    from integration.sc_namespace_resolve import (  # noqa: E402
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        pinned_namespace_env_hint,
        resolve_sc_namespace,
        skip_reason_no_sc_namespace,
    )

RECORD_EXAMPLE_LUA = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "udf", "record_example.lua"),
)
RECORD_EXAMPLE_MODULE = "record_example"
RECORD_SERVER_PATH = "record_example.lua"

# --- Durable-delete UDF fixture constants ---------------------------------------
DD_UDF_INDEX_NAME = "ddudfinx"
DD_UDF_KEY_PREFIX = "ddudfk"
DD_UDF_BIN1 = "ddux1"
DD_UDF_BIN2 = "ddux2"
DD_UDF_SIZE = 10

# --- Dataset query + background UDF execute constants ---------------------------
TQE_INDEX_NAME = "tqeindex"
TQE_KEY_PREFIX = "tqekey"
TQE_BIN1 = "tqebin1"
TQE_BIN2 = "tqebin2"
TQE_SIZE = 10

# --- Background scan-delete fixture constants -----------------------------------
BG_TEST_SET = "bgtest"
BG_BIN = "bgval"
BG_BIN2 = "bgval2"

BG_TEST_LUA = br"""
local function putBin(r, name, value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

function writeBin(r, name, value)
    putBin(r, name, value)
end

function incrementBin(r, name, amount)
    if not aerospike:exists(r) then
        aerospike:create(r)
        r[name] = 0
    end
    r[name] = r[name] + amount
    aerospike:update(r)
end

function writeWithValidation(r, name, value)
    if (value >= 1 and value <= 10) then
        putBin(r, name, value)
    else
        error("1000:Invalid value")
    end
end
"""
BG_TEST_SERVER_PATH = "bg_test_example.lua"
BG_TEST_MODULE = "bg_test_example"


async def _namespaces_on_cluster_hint(session) -> str:
    """Comma-separated namespace names for skip messages (best-effort ``namespaces`` info)."""
    try:
        names = sorted(await session.info().namespaces())
    except Exception:
        return ""
    if not names:
        return ""
    return f" Namespaces on this cluster: {', '.join(names)}."


def _require_default_durable_delete(builder: Any, *, ctx: str) -> Any:
    """Require chainable ``default_durably_delete()`` on builders (Phase 3 API)."""
    fn = getattr(builder, "default_durably_delete", None)
    if fn is None:
        pytest.fail(f"{ctx}: default_durably_delete() not implemented")
    out = fn()
    return out


def _require_without_durable_delete(builder: Any, *, ctx: str) -> Any:
    fn = getattr(builder, "without_durable_delete", None)
    if fn is None:
        pytest.fail(f"{ctx}: without_durable_delete() not implemented")
    return fn()


def _skip_if_not_enterprise(enterprise_sc: bool) -> None:
    """Skip when the SC test seed is not Enterprise edition (most durable-delete cases)."""
    if not enterprise_sc:
        pytest.skip(
            "Enterprise Edition required: the session-scoped 'enterprise_sc' fixture "
            "queries edition on AEROSPIKE_HOST_SC (or AEROSPIKE_HOST when unset). "
            "Install/use an Enterprise SC cluster for these tests. "
            "The SC default point-delete test does not use this gate.",
        )


def _skip_if_role_violation(exc: BaseException) -> None:
    """Skip when RBAC denies background query UDF jobs (RoleViolation from the server)."""
    msg = str(exc)
    rc = getattr(exc, "result_code", None)
    if "RoleViolation" in msg or (rc is not None and "RoleViolation" in repr(rc)):
        pytest.skip(
            "Cluster user lacks privileges for background query UDF execution "
            f"(server: {msg})",
        )


def _assert_batch_delete_stream_ok(rows: list, expected_count: int) -> None:
    assert len(rows) == expected_count
    for rr in rows:
        rc = rr.result_code
        assert rc in (ResultCode.OK, ResultCode.KEY_NOT_FOUND_ERROR), (
            f"unexpected delete resultCode={rc} key={rr.key}"
        )


def _assert_batch_operate_delete_stream_all_ok(rows: list, expected_count: int) -> None:
    assert len(rows) == expected_count
    for rr in rows:
        rc = rr.result_code
        assert rc == ResultCode.OK, (
            f"unexpected operate-delete resultCode={rc} key={rr.key}"
        )


async def _validate_process_record_outcome(session, ds: DataSet, bin1: str, bin2: str, size: int) -> None:
    """Assert ``processRecord`` UDF outcome for the seeded durable-delete fixture."""
    expected_list = [1, 2, 3, 104, 5, 106, 7, 108, -1, 10]
    expected_size = size - 1
    count = 0
    stream = await (
        session.query(ds)
        .where(f"$.{bin1} >= 1 and $.{bin1} <= {size + 100}")
        .bins([bin1, bin2])
        .execute()
    )
    try:
        async for row in stream:
            rec = row.record_or_raise()
            value1 = rec.bins[bin1]
            value2 = rec.bins.get(bin2, 0)

            if value1 == 9:
                pytest.fail("Data mismatch. value1 9 should not exist after UDF remove")

            if value1 == 5:
                if value2 != 0:
                    pytest.fail(f"Data mismatch. value2 {value2} should be null for bin2 cleared")
            elif value1 != expected_list[value2 - 1]:
                pytest.fail(
                    f"Data mismatch. Expected {expected_list[value2 - 1]}. Received {value1}",
                )
            count += 1
    finally:
        stream.close()
    assert count == expected_size


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def durable_delete_client(aerospike_host_sc, client_policy_sc):
    """One shared client for the module (UDF registration once per module)."""
    async with Client(seeds=aerospike_host_sc, policy=client_policy_sc) as client:
        reg = await client.register_udf_from_file(
            RECORD_EXAMPLE_LUA, RECORD_SERVER_PATH, UDFLang.LUA,
        )
        reg.wait_till_complete(sleep_time=0.2, max_attempts=50)

        reg2 = await client.register_udf(BG_TEST_LUA, BG_TEST_SERVER_PATH, UDFLang.LUA)
        reg2.wait_till_complete()

        yield client


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def sc_namespace(durable_delete_client):
    sess = durable_delete_client.create_session()
    try:
        return await resolve_sc_namespace(sess)
    except MultipleScNamespacesError as e:
        pytest.skip(
            "Several namespaces have strong-consistency enabled; set "
            f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
        )
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
async def session_sc(durable_delete_client, sc_namespace):
    sess = durable_delete_client.create_session()
    env_hint = (
        f"AEROSPIKE_HOST_SC={os.environ.get('AEROSPIKE_HOST_SC', '')!r}; "
        f"AEROSPIKE_SC_NAMESPACE={sc_namespace!r} "
        f"(env raw={os.environ.get('AEROSPIKE_SC_NAMESPACE', '')!r}); "
        f"AEROSPIKE_HOST={os.environ.get('AEROSPIKE_HOST', '')!r}"
    )
    try:
        status = await sess.namespace_sc_status(sc_namespace)
    except Exception as exc:
        pytest.skip(
            f"Could not query namespace {sc_namespace!r} ({exc}). "
            f"Check seeds and namespace name. {env_hint}",
        )
    if not status.is_sc:
        ns_hint = await _namespaces_on_cluster_hint(sess)
        pin = pinned_namespace_env_hint()
        pytest.skip(f"{status.detail}{ns_hint}{pin} {env_hint}")
    return sess


@pytest.fixture
def ds_sc(sc_namespace) -> DataSet:
    return DataSet.of(sc_namespace, "durable_delete_sc")


@pytest.fixture
def ds_bg(sc_namespace) -> DataSet:
    return DataSet.of(sc_namespace, BG_TEST_SET)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def bgtest_bgval_index(durable_delete_client, sc_namespace):
    """Numeric secondary index on ``bgtest.bgval`` for background jobs using ``index_filters``."""
    client = durable_delete_client
    idx_name = "bgtest_bgval_ix"
    try:
        await (
            client.index(sc_namespace, BG_TEST_SET)
            .on_bin(BG_BIN)
            .named(idx_name)
            .numeric()
            .create()
        )
    except AerospikeError as ae:
        if ae.result_code != ResultCode.INDEX_ALREADY_EXISTS:
            raise
    await asyncio.sleep(1.5)
    yield idx_name


@pytest_asyncio.fixture(loop_scope="session")
async def prepare_dd_udf_background(session_sc, ds_sc, enterprise_sc):
    """Create index and seed records for background UDF durable-delete coverage."""
    _skip_if_not_enterprise(enterprise_sc)
    session = session_sc
    client = session.client

    try:
        await (
            client.index(ds_sc.namespace, ds_sc.set_name)
            .on_bin(DD_UDF_BIN1)
            .named(DD_UDF_INDEX_NAME)
            .numeric()
            .create()
        )
    except AerospikeError as ae:
        if ae.result_code != ResultCode.INDEX_ALREADY_EXISTS:
            raise
    await asyncio.sleep(0.5)

    for i in range(1, DD_UDF_SIZE + 1):
        key = ds_sc.id(f"{DD_UDF_KEY_PREFIX}{i}")
        await delete_keys_durable(session, [key])
        await session.upsert(key).put({DD_UDF_BIN1: i, DD_UDF_BIN2: i}).execute()

    yield True

    try:
        await client.index(ds_sc.namespace, ds_sc.set_name).named(DD_UDF_INDEX_NAME).drop()
    except Exception:
        pass


@pytest_asyncio.fixture(loop_scope="session")
async def prepare_query_execute(session_sc, ds_sc, enterprise_sc):
    """Create index and seed records for query + background UDF execute coverage."""
    _skip_if_not_enterprise(enterprise_sc)
    session = session_sc
    client = session.client

    try:
        await (
            client.index(ds_sc.namespace, ds_sc.set_name)
            .on_bin(TQE_BIN1)
            .named(TQE_INDEX_NAME)
            .numeric()
            .create()
        )
    except AerospikeError as ae:
        if ae.result_code != ResultCode.INDEX_ALREADY_EXISTS:
            raise
    await asyncio.sleep(0.5)

    for i in range(1, TQE_SIZE + 1):
        key = ds_sc.id(f"{TQE_KEY_PREFIX}{i}")
        await delete_keys_durable(session, [key])
        await session.upsert(key).put({TQE_BIN1: i, TQE_BIN2: i}).execute()

    yield True

    try:
        await client.index(ds_sc.namespace, ds_sc.set_name).named(TQE_INDEX_NAME).drop()
    except Exception:
        pass


@pytest_asyncio.fixture
async def seed_background_task_rows(session_sc, ds_bg):
    """Seed rows for background scan-delete tests."""
    session = session_sc
    for i in range(1, 11):
        k = ds_bg.id(f"bg_{i}")
        await delete_keys_durable(session, [k])
        await (
            session.upsert(k)
            .put({BG_BIN: i, BG_BIN2: "original"})
            .execute()
        )
    yield


# =============================================================================
# Background UDF remove, operate-delete, batch, and default point delete (SC)
# =============================================================================

@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteBackgroundUdf:
    async def test_background_udf_remove_uses_default_durable_delete_on_strong_consistency(
        self, session_sc, ds_sc, enterprise_sc, prepare_dd_udf_background,
    ):
        """Background UDF removes records; SC requires default durable delete on the job."""
        session = session_sc
        bg = (
            session.background_task()
            .execute_udf(ds_sc)
            .function(RECORD_EXAMPLE_MODULE, "processRecord")
            .passing(DD_UDF_BIN1, DD_UDF_BIN2, 100)
            .where(f"$.{DD_UDF_BIN1} >= 3 and $.{DD_UDF_BIN1} <= 9")
        )
        bg = _require_default_durable_delete(bg, ctx="BackgroundUdfBuilder")
        try:
            task = await bg.execute()
        except AerospikeError as exc:
            _skip_if_role_violation(exc)
            raise
        assert task.wait_till_complete(sleep_time=0.25, max_attempts=40)

        await _validate_process_record_outcome(
            session, ds_sc, DD_UDF_BIN1, DD_UDF_BIN2, DD_UDF_SIZE,
        )


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteOperate:
    async def test_update_operate_delete_record_uses_default_durable_delete_on_strong_consistency(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Operate-delete on update uses default durable delete on SC."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        key = ds_sc.id(10670)
        bin_name = "udDelBin"
        await delete_keys_durable(session, [key])

        await session.insert(key).bin(bin_name).set_to(1).execute()

        seg = session.update(key).bin(bin_name).get().delete_record()
        seg = _require_default_durable_delete(seg, ctx="WriteSegmentBuilder")
        rs = await seg.execute()

        first = await rs.first_or_raise()
        assert first.record is not None
        assert first.record.bins[bin_name] == 1

        ex = await session.exists(key).respond_all_keys().execute()
        row = await ex.first()
        assert row is not None
        assert row.as_bool() is False


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteBatchReset:
    async def test_batch_delete_durable_delete_resets_records_for_repeat_adds(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Batch durable delete clears records so repeated upserts start fresh."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        bin_name = "ddbatchbin"
        first_key = 10110
        keys = ds_sc.ids(list(range(first_key, first_key + 10)))
        for k in keys:
            await delete_keys_durable(session, [k])

        del_stream = await session.delete(*keys).durably_delete().respond_all_keys().execute(
            on_error=ErrorStrategy.IN_STREAM,
        )
        del_rows = await del_stream.collect()
        _assert_batch_delete_stream_ok(del_rows, len(keys))

        await session.upsert(keys).bin(bin_name).add(10).execute()
        await session.upsert(keys).bin(bin_name).add(5).execute()

        q = await session.query(keys).bins([bin_name]).execute()
        rows = await q.collect()
        assert len(rows) == len(keys)
        for i, row in enumerate(rows):
            assert row.record is not None
            assert row.record.bins[bin_name] == 15, f"key index {i}"

        for k in keys:
            await delete_keys_durable(session, [k])


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteTombstone:
    async def test_durable_delete_tombstone_advances_generation_after_reinsert(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Durable delete tombstone bumps generation across delete/reinsert cycles."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        key = ds_sc.id("ddg" + uuid.uuid4().hex)
        bin_name = "name"
        await delete_keys_durable(session, [key])

        await session.delete(key).durably_delete().execute()

        await session.insert(key).bin(bin_name).set_to("bob").execute()
        r1 = await (await session.query(key).bins([bin_name]).execute()).first_or_raise()
        gen_after_first_insert = r1.record.generation

        await session.delete(key).durably_delete().execute()
        await session.insert(key).bin(bin_name).set_to("bob").execute()
        r2 = await (await session.query(key).bins([bin_name]).execute()).first_or_raise()
        gen_after_second_insert = r2.record.generation

        assert gen_after_first_insert > 0
        assert gen_after_second_insert >= gen_after_first_insert + 2


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteBatchOperateMultiKey:
    async def test_batch_operate_record_delete_with_durable_delete_overrides_behavior_when_multi_key(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Multi-key operate-delete honors explicit durable delete over batch behavior defaults."""
        _skip_if_not_enterprise(enterprise_sc)
        probe_behavior = Behavior.DEFAULT.derive_with_changes(
            "BatchOperateDurableDeleteProbe",
            writes_batch=Settings(durable_delete=False),
        )
        batch_sc = probe_behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.SC,
        )
        assert batch_sc.durable_delete is False

        session = session_sc.client.create_session(behavior=probe_behavior)

        bin_name = "ddOpDdBin"
        first_key = 10320
        keys = ds_sc.ids(first_key, first_key + 1, first_key + 2, first_key + 3)
        for k in keys:
            await delete_keys_durable(session_sc, [k])

        await session.upsert(keys).bin(bin_name).add(10).execute()
        await session.upsert(keys).bin(bin_name).add(5).execute()

        seg = session.upsert(keys)
        seg = _require_default_durable_delete(seg, ctx="multi-key operate delete")
        del_stream = await seg.bin(bin_name).get().delete_record().execute(
            on_error=ErrorStrategy.IN_STREAM,
        )
        del_rows = await del_stream.collect()
        _assert_batch_operate_delete_stream_all_ok(del_rows, len(keys))

        await session.upsert(keys).bin(bin_name).add(10).execute()
        await session.upsert(keys).bin(bin_name).add(5).execute()

        q = await session.query(keys).bins([bin_name]).execute()
        rows = await q.collect()
        for i, row in enumerate(rows):
            assert row.record is not None
            assert row.record.bins[bin_name] == 15, f"key index {i}"

        for k in keys:
            await delete_keys_durable(session_sc, [k])


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteForbiddenBatch:
    async def test_batch_delete_explicit_non_durable_rejected_on_strong_consistency(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Explicit non-durable batch delete is rejected on SC."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        bin_name = "ddNdBin"
        first_key = 10430
        keys = ds_sc.ids(first_key, first_key + 1)
        for k in keys:
            await delete_keys_durable(session, [k])

        await session.upsert(keys).bin(bin_name).add(1).execute()

        ws = _require_without_durable_delete(session.delete(*keys), ctx="batch delete")
        rs = await ws.execute(on_error=ErrorStrategy.IN_STREAM)
        rows = await rs.collect()
        assert len(rows) == len(keys)
        for rr in rows:
            assert rr.result_code == ResultCode.FAIL_FORBIDDEN, (
                "expected non-durable batch delete to be forbidden on SC"
            )

        ex = await session.exists(*keys).respond_all_keys().execute()
        ex_rows = await ex.collect()
        for i, rr in enumerate(ex_rows):
            assert rr.as_bool(), f"record should still exist after forbidden delete; index {i}"

        for k in keys:
            await delete_keys_durable(session, [k])

    async def test_batch_delete_explicit_non_durable_rejected_when_behavior_durable_true(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Non-durable batch delete stays forbidden when behavior requests durable by default."""
        _skip_if_not_enterprise(enterprise_sc)
        probe_behavior = Behavior.DEFAULT.derive_with_changes(
            "BatchDdFalseOvProbe",
            writes_batch=Settings(durable_delete=True),
        )
        batch_sc = probe_behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.SC,
        )
        assert batch_sc.durable_delete is True

        session = session_sc.client.create_session(behavior=probe_behavior)

        bin_name = "ddFbBin"
        first_key = 10460
        keys = ds_sc.ids(first_key, first_key + 1)
        for k in keys:
            await delete_keys_durable(session_sc, [k])

        await session.upsert(keys).bin(bin_name).add(1).execute()

        ws = _require_without_durable_delete(session.delete(*keys), ctx="batch delete probe")
        rs = await ws.execute(on_error=ErrorStrategy.IN_STREAM)
        rows = await rs.collect()
        assert len(rows) == len(keys)
        for rr in rows:
            assert rr.result_code == ResultCode.FAIL_FORBIDDEN

        ex = await session.exists(*keys).respond_all_keys().execute()
        ex_rows = await ex.collect()
        for i, rr in enumerate(ex_rows):
            assert rr.as_bool(), f"record should still exist; index {i}"

        for k in keys:
            await delete_keys_durable(session_sc, [k])


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteBatchOverride:
    async def test_batch_delete_durable_override_true_when_behavior_batch_durable_delete_false(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Explicit ``durably_delete()`` succeeds when batch behavior defaults to non-durable."""
        _skip_if_not_enterprise(enterprise_sc)
        probe_behavior = Behavior.DEFAULT.derive_with_changes(
            "BatchDdTrueOvProbe",
            writes_batch=Settings(durable_delete=False),
        )
        batch_sc = probe_behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.SC,
        )
        assert batch_sc.durable_delete is False

        session = session_sc.client.create_session(behavior=probe_behavior)

        bin_name = "ddOvBin"
        first_key = 10450
        keys = ds_sc.ids(first_key, first_key + 1, first_key + 2, first_key + 3)
        for k in keys:
            await delete_keys_durable(session_sc, [k])

        await session.upsert(keys).bin(bin_name).add(10).execute()
        await session.upsert(keys).bin(bin_name).add(5).execute()

        del_stream = await session.delete(*keys).durably_delete().execute(
            on_error=ErrorStrategy.IN_STREAM,
        )
        del_rows = await del_stream.collect()
        _assert_batch_delete_stream_ok(del_rows, len(keys))

        await session.upsert(keys).bin(bin_name).add(10).execute()
        await session.upsert(keys).bin(bin_name).add(5).execute()

        q = await session.query(keys).bins([bin_name]).execute()
        rows = await q.collect()
        for i, row in enumerate(rows):
            assert row.record is not None
            assert row.record.bins[bin_name] == 15, f"key index {i}"

        for k in keys:
            await delete_keys_durable(session_sc, [k])


@pytest.mark.asyncio(loop_scope="session")
class TestDurableDeleteDefaultPointDelete:
    async def test_default_session_point_delete_on_strong_consistency_without_explicit_durable_opt_in(
        self, session_sc, ds_sc,
    ):
        """Default session point delete on SC without explicit durable opt-in (no EE gate)."""
        session = session_sc
        key = ds_sc.id(10710)
        bin_name = "ddbDefSc"
        await delete_keys_durable(session, [key])
        await session.upsert(key).bin(bin_name).set_to(1).execute()

        del_stream = await session.delete(key).execute()
        assert (await del_stream.first_or_raise()).as_bool()

        ex = await session.exists(key).respond_all_keys().execute()
        row = await ex.first()
        assert row is not None
        assert row.as_bool() is False

    async def test_default_session_point_delete_on_ap_without_explicit_durable_opt_in(
        self, durable_delete_client,
    ):
        """Default session point delete on an AP namespace without explicit durable opt-in."""
        session = durable_delete_client.create_session()
        if await session.is_namespace_sc("test"):
            pytest.skip("Namespace 'test' is SC mode; this scenario expects an AP namespace.")
        ds_ap = DataSet.of("test", "durable_delete_ap_def")
        key = ds_ap.id(10711)
        bin_name = "ddbDefAp"
        try:
            await session.delete(key).execute()
        except AerospikeError:
            pass
        await session.upsert(key).bin(bin_name).set_to(1).execute()

        del_stream = await session.delete(key).execute()
        assert (await del_stream.first_or_raise()).as_bool()

        ex = await session.exists(key).respond_all_keys().execute()
        row = await ex.first()
        assert row is not None
        assert row.as_bool() is False


# =============================================================================
# Dataset query + background UDF execute
# =============================================================================

@pytest.mark.asyncio(loop_scope="session")
class TestQueryExecuteDurableDelete:
    async def test_query_execute(
        self, session_sc, ds_sc, enterprise_sc, prepare_query_execute,
    ):
        """Background UDF run over a queried dataset; SC chains default durable delete."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        is_sc = await session.is_namespace_sc(ds_sc.namespace)

        bg = (
            session.background_task()
            .execute_udf(ds_sc)
            .function(RECORD_EXAMPLE_MODULE, "processRecord")
            .passing(TQE_BIN1, TQE_BIN2, 100)
            .where(f"$.{TQE_BIN1} >= 3 and $.{TQE_BIN1} <= 9")
        )
        if is_sc:
            bg = _require_default_durable_delete(bg, ctx="QueryExecute background UDF")

        try:
            task = await bg.execute()
        except AerospikeError as exc:
            _skip_if_role_violation(exc)
            raise
        assert task.wait_till_complete(sleep_time=0.25, max_attempts=40)

        await _validate_process_record_outcome(session, ds_sc, TQE_BIN1, TQE_BIN2, TQE_SIZE)


# =============================================================================
# Background scan delete
# =============================================================================

@pytest.mark.asyncio(loop_scope="session")
class TestBackgroundTaskDelete:
    async def test_background_delete(
        self, session_sc, ds_bg, seed_background_task_rows, bgtest_bgval_index,
    ):
        """Background delete by predicate; SC uses default durable delete on the job.

        Strong-consistency namespaces need partition pruning via ``Statement.filters``
        (``index_filters`` on the builder). Policy-level expression filters alone do not
        reliably drive durable background deletes on every server build.
        """
        session = session_sc
        is_sc = await session.is_namespace_sc(ds_bg.namespace)

        b = session.background_task().delete(ds_bg).index_filters(
            Filter.range(BG_BIN, 9, 10),
        )
        if is_sc:
            b = _require_default_durable_delete(b, ctx="BackgroundOperationBuilder.delete")

        task = await b.execute()
        assert task.wait_till_complete(sleep_time=0.25, max_attempts=40)

        for i in range(1, 11):
            rs = await session.query(ds_bg.id(f"bg_{i}")).execute()
            row = await rs.first()
            if i > 8:
                assert row is None or not row.is_ok
            else:
                assert row is not None and row.is_ok and row.record is not None
