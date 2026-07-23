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
# License for the specific language governing permissions and limitations
# under the License.

"""Integration tests for ``list_indexes`` (sync)."""

from __future__ import annotations

import time

from aerospike_sdk.sync import ClusterDefinition

NS = "test"
SET = "list_indexes_integ_sync"
BIN = "age"
IDX = "psdk_list_indexes_age_sync"


def _wait_visible(lister, name, present=True, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        names = {i["name"] for i in lister()}
        if (name in names) == present:
            return True
        time.sleep(0.25)
    return False


def test_sync_list_indexes_via_cluster_and_session(aerospike_host):
    """Sync ``list_indexes`` reflects create + drop via Cluster and Session."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname, port = aerospike_host, 3000

    cluster = ClusterDefinition(hostname, port).connect()
    try:
        session = cluster.create_session()

        try:
            session.index(NS, SET).named(IDX).drop()
        except Exception:
            pass
        assert _wait_visible(cluster.list_indexes, IDX, present=False)

        session.index(NS, SET).on_bin(BIN).named(IDX).numeric().create()
        assert _wait_visible(cluster.list_indexes, IDX, present=True)

        mine = [i for i in cluster.list_indexes() if i["name"] == IDX]
        assert mine, "index not listed after create"
        (entry,) = mine
        assert entry["namespace"] == NS
        assert entry["set"] == SET
        assert entry["bin"] == BIN

        assert any(i["name"] == IDX for i in session.list_indexes())

        session.index(NS, SET).named(IDX).drop()
        assert _wait_visible(session.list_indexes, IDX, present=False)
    finally:
        cluster.close()


def _build_at_least(session, floor) -> bool:
    """Best-effort probe: True when every node's build is >= *floor*."""
    for raw in session.info("build").values():
        parts = raw.strip().split(".")
        try:
            triple = (int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, IndexError):
            return False
        if triple < floor:
            return False
    return True


def test_sync_expression_index_create_and_drop(aerospike_host):
    """Sync smoke for expression-based index creation (blocking PAC entry)."""
    import pytest
    from aerospike_sdk import Exp

    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname, port = aerospike_host, 3000

    idx = "psdk_exp_idx_sync"
    cluster = ClusterDefinition(hostname, port).connect()
    try:
        session = cluster.create_session()
        if not _build_at_least(session, (8, 1, 2)):
            pytest.skip("expression-based indexes require server 8.1.2+")

        try:
            session.index(NS, SET).named(idx).drop()
        except Exception:
            pass
        assert _wait_visible(cluster.list_indexes, idx, present=False)

        (
            session.index(NS, SET)
            .on_expression(Exp.int_bin(BIN))
            .named(idx)
            .numeric()
            .create()
        )
        assert _wait_visible(cluster.list_indexes, idx, present=True)

        session.index(NS, SET).named(idx).drop()
        assert _wait_visible(session.list_indexes, idx, present=False)
    finally:
        cluster.close()
