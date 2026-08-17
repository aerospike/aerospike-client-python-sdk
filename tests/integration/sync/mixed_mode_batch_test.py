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

"""Sync mixed-mode batches: per-row mode resolution on the blocking chain.

Mirror of the async suite (see that module's docstring for the observables);
the sync chain has its own dispatch plumbing (``query_dispatch``), so the
per-row resolution and reroute need their own end-to-end run.
"""

import pytest

from aerospike_sdk.dataset import DataSet


@pytest.fixture(scope="module")
def cluster_mm(aerospike_host_sc, make_cluster_definition):
    """One shared sync connection to the SC seed (dual-namespace cluster)."""
    with make_cluster_definition(aerospike_host_sc, auth=True, sync=True).connect() as cluster:
        yield cluster


@pytest.fixture(scope="module")
def mode_namespaces(cluster_mm):
    """(ap_namespace, sc_namespace) served by the same cluster, else skip."""
    session = cluster_mm.create_session()
    try:
        names = sorted(session.info().namespaces())
    except Exception as exc:
        pytest.skip(f"could not enumerate namespaces: {exc}")
    ap_ns = sc_ns = None
    for ns in names:
        status = session.namespace_sc_status(ns)
        if status.is_sc and sc_ns is None:
            sc_ns = ns
        elif not status.is_sc and ap_ns is None:
            ap_ns = ns
    if ap_ns is None or sc_ns is None:
        pytest.skip(
            "mixed-mode batches need one AP and one SC namespace on the same "
            f"cluster; found namespaces {names!r}",
        )
    return ap_ns, sc_ns


def _recreate(session, *keys):
    for key in keys:
        session.upsert(key).put({"v": 1}).execute()


def _generation(session, key) -> int:
    result = session.query(key).with_no_bins().execute()
    return result.first_or_raise().record_or_raise().generation


class TestSyncMixedBatchDelete:
    """Per-row durable-delete resolution, pinned in both key orderings."""

    def _run_ordering(self, session, first, second, ap_key, sc_key):
        _recreate(session, ap_key, sc_key)
        results = list(session.delete(first, second).execute())
        assert all(r.is_ok for r in results), (
            f"mixed batch delete failed per-row: "
            f"{[(r.key.namespace, r.result_code) for r in results if not r.is_ok]}"
        )
        _recreate(session, ap_key, sc_key)
        # Non-durable on the AP row: re-create starts at generation 1.
        assert _generation(session, ap_key) == 1

    def test_ap_key_first(self, cluster_mm, mode_namespaces):
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch_sync").id("del_ap_first_a")
        sc_key = DataSet.of(sc_ns, "mm_batch_sync").id("del_ap_first_s")
        self._run_ordering(session, ap_key, sc_key, ap_key, sc_key)

    def test_sc_key_first(self, cluster_mm, mode_namespaces):
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch_sync").id("del_sc_first_a")
        sc_key = DataSet.of(sc_ns, "mm_batch_sync").id("del_sc_first_s")
        self._run_ordering(session, sc_key, ap_key, ap_key, sc_key)
