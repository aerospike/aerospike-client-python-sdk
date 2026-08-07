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

"""Mixed-mode batches against a cluster hosting both an AP and an SC namespace.

A batch spanning namespaces with different consistency modes must resolve
mode-scoped settings per key: SC rows get the durable-delete default, AP rows
do not — regardless of key order. Requires a cluster serving at least one AP
and one SC namespace (the strong-consistency test seed); skips elsewhere.

Observables: a non-durable delete is rejected outright on SC, and a durable
delete leaves a tombstone that retains generation across a re-create, while a
non-durable delete expunges the record so a re-create starts at generation 1.
"""

import pytest
import pytest_asyncio

from aerospike_sdk.dataset import DataSet

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster_mm(aerospike_host_sc, make_cluster_definition):
    """One shared connection to the SC seed (the dual-namespace cluster)."""
    async with await make_cluster_definition(aerospike_host_sc, auth=True).connect() as cluster:
        yield cluster


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def mode_namespaces(cluster_mm):
    """(ap_namespace, sc_namespace) served by the same cluster, else skip."""
    session = cluster_mm.create_session()
    try:
        names = sorted(await session.info().namespaces())
    except Exception as exc:
        pytest.skip(f"could not enumerate namespaces: {exc}")
    ap_ns = sc_ns = None
    for ns in names:
        status = await session.namespace_sc_status(ns)
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


async def _recreate(session, *keys):
    """(Re-)create the records; returns cleanly whether or not they existed."""
    for key in keys:
        await session.upsert(key).put({"v": 1}).execute()


async def _generation(session, key) -> int:
    result = await session.query(key).with_no_bins().execute()
    return (await result.first_or_raise()).record_or_raise().generation


class TestMixedBatchDelete:
    """Per-row durable-delete resolution, pinned in both key orderings."""

    async def _run_ordering(self, session, first, second, ap_key, sc_key):
        await _recreate(session, ap_key, sc_key)
        stream = await session.delete(first, second).execute()
        results = [r async for r in stream]
        assert all(r.is_ok for r in results), (
            f"mixed batch delete failed per-row: "
            f"{[(r.key.namespace, r.result_code) for r in results if not r.is_ok]}"
        )
        # AP row must have been a plain (non-durable) delete: a re-create
        # starts over at generation 1. A durable delete would leave a
        # tombstone that carries the generation forward.
        await _recreate(session, ap_key, sc_key)
        assert await _generation(session, ap_key) == 1
        # SC row was accepted at all only because it resolved durable.

    async def test_ap_key_first(self, cluster_mm, mode_namespaces):
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch").id("del_ap_first_a")
        sc_key = DataSet.of(sc_ns, "mm_batch").id("del_ap_first_s")
        await self._run_ordering(session, ap_key, sc_key, ap_key, sc_key)

    async def test_sc_key_first(self, cluster_mm, mode_namespaces):
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch").id("del_sc_first_a")
        sc_key = DataSet.of(sc_ns, "mm_batch").id("del_sc_first_s")
        await self._run_ordering(session, sc_key, ap_key, ap_key, sc_key)


class TestMixedBatchWriteWithRecordDelete:

    async def test_operate_delete_record_rows_resolve_per_mode(
        self, cluster_mm, mode_namespaces,
    ):
        """A multi-key write chain carrying delete_record spans both modes."""
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch").id("op_del_a")
        sc_key = DataSet.of(sc_ns, "mm_batch").id("op_del_s")
        await _recreate(session, ap_key, sc_key)
        stream = await (
            session.upsert(ap_key, sc_key)
            .bin("v").get()
            .delete_record()
            .execute()
        )
        results = [r async for r in stream]
        assert all(r.is_ok for r in results), (
            f"{[(r.key.namespace, r.result_code) for r in results if not r.is_ok]}"
        )
        await _recreate(session, ap_key, sc_key)
        assert await _generation(session, ap_key) == 1


class TestMixedBatchReadSmoke:

    async def test_mixed_read_returns_all_rows(self, cluster_mm, mode_namespaces):
        """Reads have no mode-scoped row policy; the batch must still span."""
        ap_ns, sc_ns = mode_namespaces
        session = cluster_mm.create_session()
        ap_key = DataSet.of(ap_ns, "mm_batch").id("read_a")
        sc_key = DataSet.of(sc_ns, "mm_batch").id("read_s")
        await _recreate(session, ap_key, sc_key)
        stream = await session.query(sc_key, ap_key).execute()
        results = [r async for r in stream]
        assert len(results) == 2 and all(r.is_ok for r in results)
        assert {r.key.namespace for r in results} == {ap_ns, sc_ns}
