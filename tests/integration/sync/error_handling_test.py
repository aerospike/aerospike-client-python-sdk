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

"""Sync integration tests mirroring async idempotent-op, TTL guard, and bad-AEL paths."""

import pytest
from aerospike_sdk.exceptions import ResultCode

from aerospike_sdk import DataSet
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
            lambda: session_with_ael_row.query(general_namespace(), AEL_ERROR_SET)
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
