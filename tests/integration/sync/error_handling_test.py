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

"""Sync integration tests mirroring async idempotent-op and TTL guard paths."""

import pytest
from aerospike_sdk.exceptions import ResultCode

from aerospike_sdk import DataSet
from tests.integration.namespace import general_namespace


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

    def test_batch_delete_all_missing_returns_empty(self, cluster, ds):
        k1 = ds.id("sidm_bd_1")
        k2 = ds.id("sidm_bd_2")
        _cleanup(cluster.create_session(), k1, k2)
        session = cluster.create_session()
        results = session.delete([k1, k2]).execute().collect()
        assert len(results) == 0


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
