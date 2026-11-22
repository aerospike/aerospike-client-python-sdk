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

"""Behaviors derived with ``on`` selector entries carry live traffic on AP and SC namespaces."""

from datetime import timedelta

import pytest
import pytest_asyncio

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import CommitLevel, Mode, OpKind, OpShape, ReadModeAP, Selector, Settings

from integration.namespace import general_namespace
from integration.sc_namespace_resolve import (
    MultipleScNamespacesError,
    NoStrongConsistencyNamespace,
    resolve_sc_namespace,
    skip_reason_no_sc_namespace,
)

SET = "selector_psdk"

_CRITICAL = dict(
    total_timeout=timedelta(seconds=5),
    retry_delay=timedelta(milliseconds=50),
    commit_level=CommitLevel.COMMIT_ALL,
)

_ON_ENTRIES = {
    "batch-ap-reads-and-retryable-point-ap-writes": [
        (Selector.reads().batch().ap(), Settings(read_mode_ap=ReadModeAP.ALL)),
        (Selector.writes().retryable().point().ap(), Settings(commit_level=CommitLevel.COMMIT_ALL)),
        (Selector.writes().ap().retryable().point(), Settings(total_timeout=timedelta(seconds=2))),
    ],
    "point-ap-writes-by-retryability": [
        (Selector.writes().retryable().point().ap(), Settings(**_CRITICAL, max_retries=4)),
        (Selector.writes().non_retryable().point().ap(), Settings(**_CRITICAL, max_retries=0)),
    ],
    "point-and-batch-ap-reads": [
        (Selector.reads().point().ap(), Settings(
            total_timeout=timedelta(milliseconds=100), max_retries=1,
            retry_delay=timedelta(milliseconds=5), read_mode_ap=ReadModeAP.ALL,
        )),
        (Selector.reads().batch().ap(), Settings(
            total_timeout=timedelta(milliseconds=200), max_retries=2,
        )),
    ],
    "retryable-point-ap-writes": [
        (Selector.writes().retryable().point().ap(), Settings(max_retries=4)),
    ],
}


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def sc_namespace(cluster_sc):
    try:
        return await resolve_sc_namespace(cluster_sc.create_session())
    except NoStrongConsistencyNamespace as e:
        pytest.skip(skip_reason_no_sc_namespace(e.namespace_names))
    except MultipleScNamespacesError as e:
        pytest.skip(str(e))


async def _exercise(session, dataset: DataSet, tag: str) -> None:
    """Point, batch and query traffic, retryable and not, through one session."""
    one = dataset.id(f"{tag}_1")
    several = dataset.ids(f"{tag}_2", f"{tag}_3")
    try:
        await session.upsert(one).put({"n": 1, "tag": tag}).execute()
        await session.update(one).bin("n").add(1).execute()
        await session.upsert(several).put({"n": 5, "tag": tag}).execute()

        point = await (await session.query(one).execute()).first_or_raise()
        assert point.record.bins["n"] == 2

        batch = [row async for row in await session.query(dataset.ids(
            f"{tag}_1", f"{tag}_2", f"{tag}_3",
        )).execute()]
        assert sorted(row.record.bins["n"] for row in batch) == [2, 5, 5]

        scanned = [row async for row in await session.query(dataset).execute()]
        assert sum(1 for row in scanned if row.record.bins.get("tag") == tag) == 3
    finally:
        await session.delete(dataset.ids(f"{tag}_1", f"{tag}_2", f"{tag}_3")).execute()


class TestSelectorBehaviorsCarryTraffic:

    @pytest.mark.parametrize("name", list(_ON_ENTRIES))
    async def test_on_an_ap_namespace(self, cluster, name):
        behavior = Behavior.DEFAULT.derive_with_changes(f"ap-{name}", on=_ON_ENTRIES[name])
        session = cluster.create_session(behavior)
        await _exercise(session, DataSet.of(general_namespace(), SET), f"ap_{name}")

    @pytest.mark.parametrize("name", list(_ON_ENTRIES))
    async def test_on_an_sc_namespace(self, cluster_sc, sc_namespace, name):
        behavior = Behavior.DEFAULT.derive_with_changes(f"sc-{name}", on=_ON_ENTRIES[name])
        session = cluster_sc.create_session(behavior)
        await _exercise(session, DataSet.of(sc_namespace, SET), f"sc_{name}")


class TestPointWriteEntriesLeaveBackgroundWritesAlone:

    async def test_ap_background_query_write_keeps_its_timeout(self, cluster):
        """Entries that name point writes do not shorten a background job's timeout."""
        behavior = Behavior.DEFAULT.derive_with_changes(
            "point-writes-only", on=_ON_ENTRIES["point-ap-writes-by-retryability"],
        )
        default = Behavior.DEFAULT.get_settings(OpKind.WRITE_RETRYABLE, OpShape.QUERY, Mode.AP)
        resolved = behavior.get_settings(OpKind.WRITE_RETRYABLE, OpShape.QUERY, Mode.AP)
        assert resolved.total_timeout == default.total_timeout

        session = cluster.create_session(behavior)
        dataset = DataSet.of(general_namespace(), "selector_bg_psdk")
        keys = dataset.ids("bg_1", "bg_2")
        try:
            await session.upsert(keys).put({"n": 1}).execute()
            task = await session.background_task().update(dataset).bin("done").set_to(1).execute()
            await task.wait_till_complete()
            rows = [row async for row in await session.query(keys).execute()]
            assert [row.record.bins["done"] for row in rows] == [1, 1]
        finally:
            await session.delete(keys).execute()
