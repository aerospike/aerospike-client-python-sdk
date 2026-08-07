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

"""Integration tests for extended server error detail (server >= 8.1.3).

A ``Behavior`` carrying ``error_detail_verbosity`` flows through the policy
mapper to the wire; a failing operation then surfaces the server's numeric
subcode and message on the raised :class:`~aerospike_sdk.AerospikeError`.

The trigger is a CDT list index/rank read out of bounds, which reliably emits
subcodes under ``OP_NOT_APPLICABLE``. Subcode *values* are asserted directly:
a value is the stable contract paired with its result code.
"""

import pytest
from aerospike_sdk import Exp, ErrorDetailVerbosity, ExpressionTrace, SubCode
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from tests.integration.namespace import general_namespace


def _bad_expression():
    """A type-mismatched comparison (int vs float) that the server rejects at
    expression *build* time — the inducer JSDK uses for the trace tests."""
    return Exp.eq(Exp.int_val(5), Exp.float_val(6.0))

# Subcode values under OP_NOT_APPLICABLE, from the server's per-status enum.
_SUB_CDT_INDEX_OUT_OF_BOUNDS = 1
_SUB_CDT_RANK_OUT_OF_BOUNDS = 2

_DS = DataSet(general_namespace(), "error_detail")


def _session(cluster, verbosity):
    """Session whose behavior requests ``verbosity`` for all operations."""
    behavior = Behavior(
        f"error-detail-{verbosity}",
        {Scope.ALL: Settings(error_detail_verbosity=verbosity)},
    )
    return cluster.create_session(behavior=behavior)


async def _read_out_of_bounds(session, key, *, kind):
    """Seed a list bin and read an out-of-bounds ``kind`` element, returning the
    raised :class:`AerospikeError`."""
    await session.upsert(key).put({"nums": [1, 2, 3]}).execute()
    builder = session.query(key).bin("nums")
    nav = builder.on_list_index(99) if kind == "index" else builder.on_list_rank(99)
    try:
        stream = await nav.get_values().execute()
        await stream.first_or_raise()
    except AerospikeError as exc:
        return exc
    pytest.fail("expected the out-of-bounds read to raise AerospikeError")


class TestErrorDetail:
    """Verbosity controls how much failure detail reaches the exception."""

    async def test_default_behavior_yields_no_detail(self, cluster, supports_error_detail):
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.NONE)
        exc = await _read_out_of_bounds(session, _DS.id("none"), kind="index")
        assert exc.sub_code is None
        assert exc.server_message is None

    async def test_verbosity_subcode_sets_subcode(self, cluster, supports_error_detail):
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.SUBCODE)
        exc = await _read_out_of_bounds(session, _DS.id("subcode"), kind="index")
        assert exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS

    async def test_verbosity_message_adds_server_message(self, cluster, supports_error_detail):
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        exc = await _read_out_of_bounds(session, _DS.id("message"), kind="index")
        assert exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS
        assert exc.server_message is not None
        assert "out of bounds" in exc.server_message

    async def test_subcode_is_scoped_to_result_code(self, cluster, supports_error_detail):
        # Distinct conditions under one result code carry distinct subcodes.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        index_exc = await _read_out_of_bounds(session, _DS.id("scoped"), kind="index")
        rank_exc = await _read_out_of_bounds(session, _DS.id("scoped"), kind="rank")
        assert index_exc.sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS
        assert rank_exc.sub_code == _SUB_CDT_RANK_OUT_OF_BOUNDS

    async def test_subcode_absent_is_zero_with_message(self, cluster, supports_error_detail):
        # A failure the result code already fully identifies (appending to an
        # integer bin) carries subcode 0 (SubCode.NONE) — not ``None`` — with a
        # message, in a *different* result-code family than the CDT cases. The
        # "subcode=" suffix must not appear for a NONE subcode.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        key = _DS.id("absent")
        await session.upsert(key).bin("nums").set_to(1).execute()
        try:
            await session.upsert(key).bin("nums").append("bad").execute()
        except AerospikeError as exc:
            assert exc.sub_code == SubCode.NONE  # 0, explicitly, not None
            assert exc.server_message is not None
            assert "subcode=" not in exc.server_message
            return
        pytest.fail("expected appending to an integer bin to raise")

    async def test_success_with_verbosity_returns_record(self, cluster, supports_error_detail):
        # Requesting detail on an operation that succeeds must not break it.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        key = _DS.id("success")
        await session.upsert(key).bin("nums").set_to(42).execute()
        result = await (await session.query(key).execute()).first_or_raise()
        assert result.record.bins["nums"] == 42

    async def test_filtered_out_has_no_subcode(self, cluster, supports_error_detail):
        # A read filtered out by an expression is a distinct result code that
        # carries no subcode (the server's filtered-subcode family was removed):
        # sub_code is NONE with a contextual message and no "subcode=" suffix.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        key = _DS.id("filtered")
        await session.upsert(key).bin("nums").set_to(1).execute()
        never_matches = Exp.eq(Exp.int_bin("nums"), Exp.int_val(99))
        try:
            await (
                await session.query(key).where(never_matches)
                .fail_on_filtered_out().execute()
            ).first_or_raise()
        except AerospikeError as exc:
            assert exc.sub_code == SubCode.NONE
            assert exc.server_message is not None
            assert "subcode=" not in exc.server_message
            return
        pytest.fail("expected a filtered-out read to raise")

    async def test_bin_not_found_family_subcode(self, cluster, supports_error_detail):
        # A subcode in a *third* result-code family (BIN_NOT_FOUND), proving
        # subcode dispatch is not CDT-specific: an HLL refresh-count op on a
        # missing bin cannot auto-create it.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        key = _DS.id("hll-missing")
        await session.upsert(key).bin("other").set_to(1).execute()
        try:
            await session.upsert(key).bin("no_hll").hll_refresh_count().execute()
        except AerospikeError as exc:
            assert exc.sub_code == SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP
            return
        pytest.fail("expected an HLL op on a missing bin to raise")

    async def test_message_verbosity_has_no_expression_trace(self, cluster, supports_error_detail):
        # An expression that fails to build carries PARAMETER_ERROR + no subcode;
        # at verbosity 2 there must be NO trace (trace is additive at verbosity 3).
        # Robust on any 8.1.3 cluster, trace-emitting or not.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        key = _DS.id("exp-v2")
        await session.upsert(key).bin("nums").set_to(1).execute()
        try:
            await (await session.query(key).where(_bad_expression()).execute()).first_or_raise()
        except AerospikeError as exc:
            assert exc.exp_trace is None
            return
        pytest.fail("expected the type-mismatched expression to fail to build")

    async def test_verbosity_3_expression_build_trace(self, cluster, supports_error_detail):
        # At verbosity 3 a build failure carries a structured trace. Requires a
        # server build that emits it (SERVER-1137+); on a base-tier-only build
        # the server returns no trace and the test skips rather than fails.
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.EXPRESSION_TRACE)
        key = _DS.id("exp-v3")
        await session.upsert(key).bin("nums").set_to(1).execute()
        try:
            await (await session.query(key).where(_bad_expression()).execute()).first_or_raise()
        except AerospikeError as exc:
            if exc.exp_trace is None:
                pytest.skip("server build does not emit an expression trace (needs SERVER-1137+)")
            assert exc.exp_trace.phase == ExpressionTrace.PHASE_BUILD
            return
        pytest.fail("expected the type-mismatched expression to fail to build")


class TestBatchErrorDetail:
    """Per-record error detail on batch results.

    A batch reports failures per record rather than raising, so the subcode
    the single-key path puts on the exception travels as data on
    :attr:`RecordResult.sub_code` instead.
    """

    async def test_batch_row_carries_sub_code(self, cluster, supports_error_detail):
        if not supports_error_detail:
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")
        session = _session(cluster, ErrorDetailVerbosity.MESSAGE)
        k_bad = _DS.id("batch-bad")
        k_good = _DS.id("batch-good")
        for k in (k_bad, k_good):
            await session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        rs = await (
            session
            .query(k_bad).bin("nums").on_list_index(99).get_values()
            .query(k_good).bins(["nums"])
            .execute()
        )
        results = await rs.collect()

        assert len(results) == 2
        assert results[0].result_code == ResultCode.OP_NOT_APPLICABLE
        assert results[0].sub_code == _SUB_CDT_INDEX_OUT_OF_BOUNDS
        assert results[1].is_ok
        assert results[1].sub_code is None
