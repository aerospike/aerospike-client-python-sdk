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

"""Tabular writes through the sync runtime.

The row builder is shared; what this protects is the sync entry point
(``session.upsert(dataset)``) and the sync ``execute()`` on the rows.
"""

from __future__ import annotations

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import ResultCode
from tests.integration.namespace import general_namespace

SET_NAME = "row_write_sync_test"


@pytest.fixture
def sync_session(aerospike_host, make_cluster_definition):
    cluster_def = make_cluster_definition(aerospike_host, sync=True)
    with cluster_def.connect() as cluster:
        yield cluster.create_session()


class TestSyncRowWrites:
    def test_rows_write_and_read_back(self, sync_session):
        users = DataSet.of(general_namespace(), SET_NAME)
        sync_session.delete(users.ids(1, 2)).execute()

        results = list(
            sync_session.upsert(users).bins("name", "age").rows([(1, "Tim", 312), (2, "Bob", 25)])
            .execute()
        )
        assert [r.is_ok for r in results] == [True, True]

        rows = list(sync_session.query(users.ids(1, 2)).execute())
        assert {r.key.value: r.record.bins for r in rows} == {
            1: {"name": "Tim", "age": 312}, 2: {"name": "Bob", "age": 25},
        }

    def test_insert_reports_an_existing_record_per_row(self, sync_session):
        users = DataSet.of(general_namespace(), SET_NAME)
        sync_session.delete(users.ids(10, 11)).execute()
        sync_session.upsert(users.id(10)).put({"name": "taken"}).execute()

        results = list(
            sync_session.insert(users).bins("name").row(10, "Tim").row(11, "Bob").execute()
        )
        by_id = {r.key.value: r for r in results}
        assert by_id[10].result_code == ResultCode.KEY_EXISTS_ERROR
        assert by_id[11].is_ok
