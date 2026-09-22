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

"""The fluent CDT path surface, reached through the sync runtime.

Deliberately a subset of the async suite rather than a mirror of it. The
builders are genuinely shared -- ``query_shared.py`` imports them from
``aio/operations/cdt_read.py`` and ``aerospike_sdk/sync`` has no CDT builder of
its own -- so re-running all 36 cases would re-exercise the same code through a
different entry point and little else.

What this file *does* protect is that the entry point works at all: the path
builder is new code, and sync reaches it through its own session and query
builders. One case per terminal is enough to catch a wiring break, which no
amount of async coverage would.
"""

from __future__ import annotations

import pytest

from aerospike_sdk import DataSet, Exp, LoopVarPart
from tests.integration.namespace import general_namespace

SET_NAME = "cdt_path_sync_test"


@pytest.fixture
def sync_session(aerospike_host, make_cluster_definition):
    cluster_def = make_cluster_definition(aerospike_host, sync=True)
    with cluster_def.connect() as cluster:
        yield cluster.create_session()


def _key(n: int):
    return DataSet.of(general_namespace(), SET_NAME).id(n)


def _over(threshold: int):
    return Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(threshold))


class TestSyncCdtPath:
    """One case per terminal, to prove the sync entry point reaches them."""

    def test_modify_by_on_every_child(self, sync_session):
        k = _key(1)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
        sync_session.update(k).bin("nums").on_each_child().modify_by(add_10).execute()

        assert sync_session.get(k).bins["nums"] == [11, 12, 13]

    def test_remove_matches_on_a_predicate(self, sync_session):
        k = _key(2)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"nums": [1, 7, 3, 9]}).execute()

        (
            sync_session.update(k)
            .bin("nums").on_each_child_where(_over(5)).remove_matches()
            .execute()
        )

        assert sync_session.get(k).bins["nums"] == [1, 3]

    def test_collect_values(self, sync_session):
        k = _key(3)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"nums": [4, 5, 6]}).execute()

        rows = list(
            sync_session.query(k)
            .bin("nums").on_each_child().collect_values()
            .execute()
        )
        assert rows[0].record.bins["nums"] == [4, 5, 6]

    def test_collect_map_keys_with_a_key_predicate(self, sync_session):
        k = _key(4)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        rows = list(
            sync_session.query(k)
            .bin("m")
            .on_each_child_where(
                Exp.eq(Exp.string_loop_var(LoopVarPart.MAP_KEY), Exp.val("b"))
            )
            .collect_map_keys()
            .execute()
        )
        assert rows[0].record.bins["m"] == ["b"]

    def test_a_mixed_path_navigates_and_then_walks(self, sync_session):
        """map_key then each_child: the handoff between the two builder kinds."""
        k = _key(5)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"d": {"x": [1, 9, 2]}}).execute()

        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        (
            sync_session.update(k)
            .bin("d").on_map_key("x").on_each_child().modify_by(add_1)
            .execute()
        )

        assert sync_session.get(k).bins["d"]["x"] == [2, 10, 3]

    def test_collect_values_tolerates_an_empty_collection(self, sync_session):
        k = _key(6)
        sync_session.delete(k).execute()
        sync_session.upsert(k).put({"d": {"empty": []}}).execute()

        rows = list(
            sync_session.query(k)
            .bin("d").on_map_key("empty").on_each_child()
            .collect_values(no_fail=True)
            .execute()
        )
        assert rows[0].record.bins["d"] == []
