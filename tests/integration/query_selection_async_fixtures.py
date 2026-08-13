# Copyright 2025-2026 Aerospike, Inc.
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

"""Session-scoped query-selection cluster fixture (async)."""

from __future__ import annotations

import pytest_asyncio

from tests.integration.query_selection_seed import (
    QuerySelectionClusterState,
    seed_query_selection_async,
    teardown_query_selection_async,
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def query_selection_cluster(
    aerospike_host,
    make_cluster_definition,
    wait_for_index,
    wait_for_set_visible,
):
    """One connect + seed for all async query-selection integration modules."""
    cluster_def = make_cluster_definition(aerospike_host)
    async with await cluster_def.connect() as cluster:
        client = cluster._sdk_client
        session = cluster.create_session()
        await seed_query_selection_async(
            client, session, wait_for_index, wait_for_set_visible,
        )
        state = QuerySelectionClusterState(client=client, session=session)
        yield state
        await teardown_query_selection_async(client, session)
