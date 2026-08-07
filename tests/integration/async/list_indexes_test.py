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

"""Integration tests for ``list_indexes`` (async)."""

from __future__ import annotations

import asyncio

from aerospike_sdk import ClusterDefinition
from tests.integration.namespace import general_namespace
from tests.integration.general_auth import apply_general_auth

NS = general_namespace()
SET = "list_indexes_integ"
BIN = "age"
IDX = "psdk_list_indexes_age"


async def _wait_visible(lister, name, present=True, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        names = {i["name"] for i in await lister()}
        if (name in names) == present:
            return True
        await asyncio.sleep(0.25)
    return False


async def test_list_indexes_via_cluster_and_session(aerospike_host):
    """``list_indexes`` reports a created index and reflects create + drop.

    Exercises the standardized ClusterDefinition -> Cluster -> Session path:
    create via the session builder, list via both Cluster and Session.
    """
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname, port = aerospike_host, 3000

    cluster = await apply_general_auth(ClusterDefinition(hostname, port)).connect()
    try:
        session = cluster.create_session()

        try:
            await session.index(NS, SET).named(IDX).drop()
        except Exception:
            pass
        assert await _wait_visible(cluster.list_indexes, IDX, present=False)

        await session.index(NS, SET).on_bin(BIN).named(IDX).numeric().create()
        assert await _wait_visible(cluster.list_indexes, IDX, present=True)

        mine = [i for i in await cluster.list_indexes() if i["name"] == IDX]
        assert mine, "index not listed after create"
        (entry,) = mine
        assert entry["namespace"] == NS
        assert entry["set"] == SET
        assert entry["bin"] == BIN

        # Reachable via the Session delegator too.
        assert any(i["name"] == IDX for i in await session.list_indexes())

        await session.index(NS, SET).named(IDX).drop()
        assert await _wait_visible(session.list_indexes, IDX, present=False)
    finally:
        await cluster.close()
