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

"""A dataset with no set covers the whole namespace.

Such a dataset reaches every set *and* the null set, and can be served by a
namespace-wide secondary index -- one created without a set, which since server
6.1 indexes every record in the namespace.

Index use is asserted rather than assumed, and without reading server counters:
a query with ``allow_scans_with_where=False`` is refused rather than fall back
to a primary-index scan, so such a query that merely *succeeds* here proves the
server chose the index. No set-scoped index could serve these queries, because
none can span two named sets and the null set at once.
"""

from __future__ import annotations

import pytest_asyncio

from aerospike_sdk import DataSet, QueryHint
from tests.integration.namespace import general_namespace
from tests.pac_compat import requires_server_compiled_ael

NS = general_namespace()
BIN = "nsw_score"
INDEX = "nsw_score_idx"

WHOLE_NAMESPACE = DataSet.of(NS)
ALPHA = DataSet.of(NS, "nsw_alpha")
BETA = DataSet.of(NS, "nsw_beta")
NULL_SET = DataSet.of(NS, None)

# One in range and one out, in each of the two named sets and the null set.
# Scores are unique, so a returned row names the set it came from.
SEEDS = [
    (ALPHA, "a_hit", 50), (ALPHA, "a_miss", 5),
    (BETA, "b_hit", 60), (BETA, "b_miss", 6),
    (NULL_SET, "n_hit", 70), (NULL_SET, "n_miss", 7),
]
IN_RANGE = {50, 60, 70}


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    cluster_def = make_cluster_definition(aerospike_host)
    async with await cluster_def.connect() as connected:
        session = connected.create_session()
        index_task = await session.index(WHOLE_NAMESPACE).on_bin(BIN).named(INDEX).integer().create()
        assert await index_task.wait_till_complete()
        for dataset, key, score in SEEDS:
            await session.upsert(dataset.id(key)).bin(BIN).set_to(score).execute()
        yield connected
        for dataset, key, _ in SEEDS:
            await session.delete(dataset.id(key)).execute()
        await session.index(WHOLE_NAMESPACE).named(INDEX).drop()


async def _scores(stream) -> set[int]:
    """Identify rows by the indexed bin: the server omits user keys unless asked."""
    rows = [rr async for rr in stream]
    assert all(rr.is_ok for rr in rows)
    return {rr.record.bins[BIN] for rr in rows}


@requires_server_compiled_ael
async def test_a_set_less_dataset_reaches_every_set_and_the_null_set(cluster):
    """The distinguishing case: one query, rows from both named sets and no set."""
    stream = await cluster.create_session().query(WHOLE_NAMESPACE).where(
        f"$.{BIN} >= 40").execute()
    assert await _scores(stream) == IN_RANGE


@requires_server_compiled_ael
async def test_a_set_scoped_query_sees_only_its_own_set(cluster):
    """The contrast that proves the set-less form widens rather than merely works."""
    stream = await cluster.create_session().query(ALPHA).where(
        f"$.{BIN} >= 40").execute()
    assert await _scores(stream) == {50}


@requires_server_compiled_ael
async def test_the_namespace_index_is_what_served_it(cluster):
    """No counters needed: disallowing scans refuses a primary-index fallback.

    So this query cannot have fallen back to the primary index, and no
    set-scoped index exists that could reach the null set.
    """
    stream = await cluster.create_session().query(WHOLE_NAMESPACE).where(
        f"$.{BIN} >= 65").with_hint(QueryHint(allow_scans_with_where=False)).execute()
    assert await _scores(stream) == {70}
