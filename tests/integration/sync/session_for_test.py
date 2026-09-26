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

"""``session_for`` through the sync runtime: the derived session reads live."""

from __future__ import annotations

from aerospike_sdk import Behavior, DataSet
from tests.integration.namespace import general_namespace


def test_session_for_derives_a_working_session(aerospike_host, make_cluster_definition):
    cluster_def = make_cluster_definition(aerospike_host, sync=True)
    with cluster_def.connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        key = DataSet.of(general_namespace(), "session_for_sync").id(1)
        session.upsert(key).put({"v": 1}).execute()

        fast = session.session_for(Behavior.READ_FAST)
        assert fast.behavior.name == "READ_FAST"
        assert session.behavior.name == "DEFAULT"
        row = fast.query(key).execute().first_or_raise()
        assert row.record.bins == {"v": 1}
        session.delete(key).execute()
