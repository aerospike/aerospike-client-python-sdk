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

"""Sync durable-delete-in-chain integration tests (strong consistency).

Focused sync mirror of the mixed-chain scenarios in
``tests/integration/async/durable_delete_sc_test.py`` that do not require
UDF registration: chained operate-delete with default durable delete
(single- and multi-key) and the SC forbidden-non-durable-delete guard. The
sync builders dispatch durable-delete flags through their own blocking
path, so they need their own coverage.

Requires an Enterprise SC cluster on ``AEROSPIKE_HOST_SC`` (or
``AEROSPIKE_HOST`` when unset); tests skip cleanly otherwise.
"""

from typing import Any

import pytest
from aerospike_async.exceptions import ResultCode

from aerospike_sdk import DataSet, ErrorStrategy, SyncClient
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Settings

try:
    from integration.sc_namespace_resolve import (
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        resolve_sc_namespace_sync,
        skip_reason_no_sc_namespace,
    )
except ImportError:
    # Running this file directly (no package context).
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from integration.sc_namespace_resolve import (  # noqa: E402
        MultipleScNamespacesError,
        NoStrongConsistencyNamespace,
        resolve_sc_namespace_sync,
        skip_reason_no_sc_namespace,
    )


def _skip_if_not_enterprise(enterprise_sc: bool) -> None:
    if not enterprise_sc:
        pytest.skip(
            "Enterprise Edition required for durable-delete SC tests "
            "(enterprise_sc fixture queries edition on AEROSPIKE_HOST_SC).",
        )


def _require_default_durable_delete(builder: Any, *, ctx: str) -> Any:
    fn = getattr(builder, "default_with_durable_delete", None)
    if fn is None:
        pytest.fail(f"{ctx}: default_with_durable_delete() not implemented")
    return fn()


def _require_without_durable_delete(builder: Any, *, ctx: str) -> Any:
    fn = getattr(builder, "without_durable_delete", None)
    if fn is None:
        pytest.fail(f"{ctx}: without_durable_delete() not implemented")
    return fn()


def _assert_batch_delete_stream_ok(rows: list, expected_count: int) -> None:
    assert len(rows) == expected_count
    for rr in rows:
        assert rr.result_code in (ResultCode.OK, ResultCode.KEY_NOT_FOUND_ERROR), (
            f"unexpected delete resultCode={rr.result_code} key={rr.key}"
        )


def _assert_batch_operate_delete_stream_all_ok(rows: list, expected_count: int) -> None:
    assert len(rows) == expected_count
    for rr in rows:
        assert rr.result_code == ResultCode.OK, (
            f"unexpected operate-delete resultCode={rr.result_code} key={rr.key}"
        )


def _delete_keys_durable(session, keys) -> None:
    for k in keys:
        try:
            session.delete(k).with_durable_delete().execute()
        except Exception:
            try:
                session.delete(k).execute()
            except Exception:
                pass


@pytest.fixture(scope="module")
def sc_client(aerospike_host_sc, client_policy_sc):
    with SyncClient(seeds=aerospike_host_sc, policy=client_policy_sc) as c:
        yield c


@pytest.fixture(scope="module")
def sc_namespace(sc_client):
    sess = sc_client.create_session()
    try:
        return resolve_sc_namespace_sync(sess)
    except MultipleScNamespacesError as e:
        pytest.skip(
            "Several namespaces have strong-consistency enabled; set "
            f"AEROSPIKE_SC_NAMESPACE to one of: {', '.join(sorted(e.names))}",
        )
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))


@pytest.fixture
def session_sc(sc_client, sc_namespace):
    sess = sc_client.create_session()
    try:
        status = sess.namespace_sc_status(sc_namespace)
    except Exception as exc:
        pytest.skip(f"Could not query namespace {sc_namespace!r} ({exc}).")
    if not status.is_sc:
        pytest.skip(status.detail)
    return sess


@pytest.fixture
def ds_sc(sc_namespace) -> DataSet:
    return DataSet.of(sc_namespace, "sync_durable_delete_sc")


class TestDurableDeleteOperate:
    def test_update_operate_delete_record_uses_default_durable_delete_on_strong_consistency(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Operate-delete on update uses default durable delete on SC."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        key = ds_sc.id(10670)
        bin_name = "udDelBin"
        _delete_keys_durable(session, [key])

        session.insert(key).bin(bin_name).set_to(1).execute()

        seg = session.update(key).bin(bin_name).get().delete_record()
        seg = _require_default_durable_delete(seg, ctx="WriteSegmentBuilder")
        first = seg.execute().first_or_raise()
        assert first.record is not None
        assert first.record.bins[bin_name] == 1

        row = session.exists(key).respond_all_keys().execute().first()
        assert row is not None
        assert row.as_bool() is False


class TestDurableDeleteBatchReset:
    def test_batch_delete_durable_delete_resets_records_for_repeat_adds(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Batch durable delete clears records so repeated upserts start fresh."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        bin_name = "ddbatchbin"
        first_key = 10110
        keys = ds_sc.ids(list(range(first_key, first_key + 10)))
        _delete_keys_durable(session, keys)

        del_rows = (
            session.delete(*keys).with_durable_delete().respond_all_keys()
            .execute(on_error=ErrorStrategy.IN_STREAM)
        ).collect()
        _assert_batch_delete_stream_ok(del_rows, len(keys))

        session.upsert(keys).bin(bin_name).add(10).execute()
        session.upsert(keys).bin(bin_name).add(5).execute()

        rows = session.query(keys).bins([bin_name]).execute().collect()
        assert len(rows) == len(keys)
        for i, row in enumerate(rows):
            assert row.record is not None
            assert row.record.bins[bin_name] == 15, f"key index {i}"

        _delete_keys_durable(session, keys)


class TestDurableDeleteBatchOperateMultiKey:
    def test_batch_operate_record_delete_with_durable_delete_overrides_behavior_when_multi_key(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Multi-key operate-delete honors explicit durable delete over batch behavior defaults."""
        _skip_if_not_enterprise(enterprise_sc)
        probe_behavior = Behavior.DEFAULT.derive_with_changes(
            "SyncBatchOperateDurableDeleteProbe",
            writes_batch=Settings(durable_delete=False),
        )
        assert probe_behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.SC,
        ).durable_delete is False

        session = session_sc.client.create_session(behavior=probe_behavior)

        bin_name = "ddOpDdBin"
        first_key = 10320
        keys = ds_sc.ids(first_key, first_key + 1, first_key + 2, first_key + 3)
        _delete_keys_durable(session_sc, keys)

        session.upsert(keys).bin(bin_name).add(10).execute()
        session.upsert(keys).bin(bin_name).add(5).execute()

        seg = _require_default_durable_delete(
            session.upsert(keys), ctx="multi-key operate delete")
        del_rows = (
            seg.bin(bin_name).get().delete_record()
            .execute(on_error=ErrorStrategy.IN_STREAM)
        ).collect()
        _assert_batch_operate_delete_stream_all_ok(del_rows, len(keys))

        session.upsert(keys).bin(bin_name).add(10).execute()
        session.upsert(keys).bin(bin_name).add(5).execute()

        rows = session.query(keys).bins([bin_name]).execute().collect()
        for i, row in enumerate(rows):
            assert row.record is not None
            assert row.record.bins[bin_name] == 15, f"key index {i}"

        _delete_keys_durable(session_sc, keys)


class TestDurableDeleteForbiddenBatch:
    def test_batch_delete_explicit_non_durable_rejected_on_strong_consistency(
        self, session_sc, ds_sc, enterprise_sc,
    ):
        """Explicit non-durable batch delete is rejected on SC."""
        _skip_if_not_enterprise(enterprise_sc)
        session = session_sc
        bin_name = "ddNdBin"
        first_key = 10430
        keys = ds_sc.ids(first_key, first_key + 1)
        _delete_keys_durable(session, keys)

        session.upsert(keys).bin(bin_name).add(1).execute()

        ws = _require_without_durable_delete(session.delete(*keys), ctx="batch delete")
        rows = ws.execute(on_error=ErrorStrategy.IN_STREAM).collect()
        assert len(rows) == len(keys)
        for rr in rows:
            assert rr.result_code == ResultCode.FAIL_FORBIDDEN, (
                "expected non-durable batch delete to be forbidden on SC"
            )

        ex_rows = session.exists(*keys).respond_all_keys().execute().collect()
        for i, rr in enumerate(ex_rows):
            assert rr.as_bool(), f"record should still exist after forbidden delete; index {i}"

        _delete_keys_durable(session, keys)
