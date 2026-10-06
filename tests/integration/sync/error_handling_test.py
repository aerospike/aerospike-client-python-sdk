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

"""Sync integration tests mirroring async idempotent-op, TTL guard, bad-AEL, unknown-namespace, and query-stream rejection paths."""

import pytest
from aerospike_sdk.exceptions import AerospikeError, InvalidNamespaceError, ResultCode

from aerospike_sdk import DataSet, ErrorStrategy, QueryDuration, QueryHint
from tests.integration.namespace import general_namespace
from tests.pac_compat import (
    assert_dataset_invalid_ael_rejected_sync,
    assert_point_invalid_ael_rejected_sync,
    requires_server_compiled_ael,
)

AEL_ERROR_SET = "sync_ael_errors"


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@pytest.fixture
def ds():
    return DataSet.of(general_namespace(), "sync_error_handling")


def _cleanup(session, *keys):
    for k in keys:
        try:
            session.delete(k).execute()
        except Exception:
            pass


class TestSyncIdempotentOps:

    def test_delete_nonexistent_succeeds(self, cluster, ds):
        k = ds.id("sidm_del_miss")
        _cleanup(cluster.create_session(), k)
        session = cluster.create_session()
        rs = session.delete(k).execute()
        rr = rs.first()
        assert rr is None or rr.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    def test_query_nonexistent_returns_empty(self, cluster, ds):
        k = ds.id("sidm_get_miss")
        _cleanup(cluster.create_session(), k)
        session = cluster.create_session()
        rs = session.query(k).execute()
        assert rs.first() is None

    def test_batch_delete_all_missing_reports_a_row_per_key(self, cluster, ds):
        """Sync mirror: every named key gets a not-found row, not silence."""
        k1 = ds.id("sidm_bd_1")
        k2 = ds.id("sidm_bd_2")
        _cleanup(cluster.create_session(), k1, k2)
        session = cluster.create_session()
        results = session.delete([k1, k2]).execute().collect()
        assert len(results) == 2
        assert all(
            r.result_code == ResultCode.KEY_NOT_FOUND_ERROR for r in results
        )


class TestSyncTtlPreservation:

    def test_no_change_in_expiration_preserves_ttl(self, cluster, ds):
        """Overwrite bins with ``with_no_change_in_expiration``; TTL stays in band."""
        k = ds.id("sidm_ttl_keep")
        _cleanup(cluster.create_session(), k)
        session = cluster.create_session()

        session.upsert(k).expire_record_after_seconds(900).put({"v": 1}).execute()
        r1 = session.query(k).execute().first_or_raise()
        ttl1 = r1.record.ttl
        assert ttl1 is not None and ttl1 > 0

        session.upsert(k).with_no_change_in_expiration().bin("v").set_to(2).execute()
        r2 = session.query(k).execute().first_or_raise()
        ttl2 = r2.record.ttl
        assert ttl2 is not None and ttl2 > 0
        assert abs(ttl1 - ttl2) <= 2
        assert r2.record.bins["v"] == 2

        _cleanup(session, k)


@pytest.fixture(scope="module")
def session_with_ael_row(cluster, sync_wait_for_set_visible):
    """Session over a set holding one row, so invalid AEL reaches the parser.

    Its own set (not the ``ds`` one above) keeps the exact-count visibility wait
    deterministic against the other tests in this module.
    """
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
    key = ds.id("row")
    session.upsert(key).put({"age": 30, "A": 1}).execute()
    sync_wait_for_set_visible(session, general_namespace(), AEL_ERROR_SET, 1)
    yield session
    _cleanup(session, key)


class TestSyncAelErrorHandling:
    """Sync twin of ``async/exp_test.py::TestAelErrorHandling``."""

    @requires_server_compiled_ael
    def test_dataset_invalid_ael_rejected(self, session_with_ael_row):
        """Malformed dataset AEL surfaces as ``PARAMETER_ERROR`` from the server."""
        assert_dataset_invalid_ael_rejected_sync(
            lambda: session_with_ael_row.query(DataSet.of(general_namespace(), AEL_ERROR_SET))
            .where("$.age >")
            .execute()
        )

    @requires_server_compiled_ael
    def test_point_invalid_ael_rejected(self, session_with_ael_row):
        """Malformed point-query AEL uses field **43** and raises ``PARAMETER_ERROR``."""
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        assert_point_invalid_ael_rejected_sync(
            lambda: session_with_ael_row.query(ds.id("row")).where("$.A >").execute()
        )


class TestSyncPointReadStringFilter:
    """A string filter must survive the virgin single-key read bypass.

    Regression: that bypass tested only the materialized ``_filter_expression``,
    so a string filter — which stays unresolved until execute — was dropped and
    the read returned a row the server should have filtered out.
    """

    @requires_server_compiled_ael
    def test_point_read_honors_string_where(self, session_with_ael_row):
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        rs = session_with_ael_row.query(ds.id("row")).where("$.A > 100").execute()
        assert rs.first() is None

    @requires_server_compiled_ael
    def test_point_read_honors_string_default_where(self, session_with_ael_row):
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        rs = session_with_ael_row.query(ds.id("row")).default_where("$.A > 100").execute()
        assert rs.first() is None


class TestSyncAelParamBinding:
    """Sync twin of ``async/exp_test.py::TestAelParamBinding``.

    The seeded row is ``{age: 30, A: 1}``.
    """

    @requires_server_compiled_ael
    def test_int_param_matches(self, session_with_ael_row):
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        rs = session_with_ael_row.query(ds.id("row")).where("$.age == %d", 30).execute()
        assert rs.first() is not None

    @requires_server_compiled_ael
    def test_param_that_does_not_match_filters_out(self, session_with_ael_row):
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        rs = session_with_ael_row.query(ds.id("row")).where("$.age > %d", 100).execute()
        assert rs.first() is None

    @requires_server_compiled_ael
    def test_escaped_modulo_with_param(self, session_with_ael_row):
        """``%%`` reaches the server as AEL's modulo operator, not a format spec."""
        ds = DataSet.of(general_namespace(), AEL_ERROR_SET)
        rs = (
            session_with_ael_row.query(ds.id("row"))
            .where("$.age %% 4 == 2 and $.A == %d", 1)
            .execute()
        )
        assert rs.first() is not None


@pytest.fixture
def bin_name_session(cluster):
    """Plain session for the bin-name diagnostic; no AEL row needed."""
    return cluster.create_session()


class TestBinNameTooLongDiagnostic:
    """The server rejects an over-long bin name without naming the bin."""

    def test_names_the_offending_bin(self, bin_name_session, ds):
        with pytest.raises(AerospikeError) as excinfo:
            bin_name_session.upsert(ds.id("binname-1")).put(
                {"ok": 1, "b" * 20: 2, "fine": 3}
            ).execute()

        err = excinfo.value
        assert err.result_code == ResultCode.BIN_NAME_TOO_LONG
        # The whole point: the caller learns which bin, not just that one is bad.
        assert repr("b" * 20) in err.hint
        assert "'ok'" not in err.hint
        assert err.hint in str(err)


class TestQueryStreamRejection:
    """A query the server rejects after the stream opens raises an SDK error."""

    @pytest.mark.parametrize("chunk_size", [None, 10], ids=["plain", "chunked"])
    def test_rejection_raises_aerospike_error(self, cluster, ds, chunk_size):
        session = cluster.create_session()
        key = ds.id("stream_rejection")
        session.upsert(key).put({"v": 1}).execute()
        builder = (
            session.query(ds)
            .records_per_second(100)
            .with_hint(QueryHint(query_duration=QueryDuration.SHORT))
        )
        if chunk_size is not None:
            builder = builder.chunk_size(chunk_size)
        stream = builder.execute()

        with pytest.raises(AerospikeError) as excinfo:
            for _ in stream:
                pass

        assert excinfo.value.result_code == ResultCode.PARAMETER_ERROR
        stream.close()
        _cleanup(session, key)


def _point_op(session, key, op):
    if op == "read":
        return session.query(key)
    return session.upsert(key).put({"v": 1})


class TestUnknownNamespace:
    """A namespace the cluster lacks fails fast instead of exhausting retries."""

    @pytest.mark.parametrize("op", ["read", "write"])
    def test_raises_invalid_namespace(self, cluster, op):
        key = DataSet.of("no_such_ns", "sync_error_handling").id(1)
        with pytest.raises(InvalidNamespaceError) as excinfo:
            _point_op(cluster.create_session(), key, op).execute().collect()
        assert excinfo.value.result_code == ResultCode.INVALID_NAMESPACE

    @pytest.mark.parametrize("op", ["read", "write"])
    def test_in_stream_row(self, cluster, op):
        key = DataSet.of("no_such_ns", "sync_error_handling").id(1)
        results = _point_op(cluster.create_session(), key, op).execute(
            on_error=ErrorStrategy.IN_STREAM,
        ).collect()

        assert [r.result_code for r in results] == [ResultCode.INVALID_NAMESPACE]
        assert isinstance(results[0].exception, InvalidNamespaceError)
