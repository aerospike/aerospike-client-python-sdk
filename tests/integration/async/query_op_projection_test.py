# Copyright 2026 Aerospike, Inc.
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

"""Integration tests for the SDK ``QueryBuilder.with_op_projection`` facade,
exercised through the high-level fluent builder. Covers:

- Basic ``Operation.get_bin`` projection.
- Extended reads: ``ExpOperation.read`` / CDT reads accepted in ops projection.
- Negative cases: write / touch / delete in foreground queries rejected.

Note: The SDK's ``with_op_projection`` is a thin façade over the PNC
``Statement.set_operations`` underneath. Native ``ExpOperation`` /
``CdtOperation`` are imported from ``aerospike_native`` directly.
"""


import pytest
import pytest_asyncio
from aerospike_sdk import CdtOperation, CTX, Exp, Filter
from aerospike_native import ExpOperation, ExpReadFlags, ExpWriteFlags, Operation
from aerospike_sdk import DataSet
# The rejects tests catch both the PSDK error type and the raw PNC error
# (streams can propagate the PNC type unconverted); PncAerospikeError is
# the PNC alias the exceptions module binds.
from aerospike_sdk.exceptions import AerospikeError as SdkAerospikeError
from aerospike_sdk.exceptions import PncAerospikeError
from tests.integration.namespace import general_namespace

# Errors raised by the core's wire encoder during stream iteration surface as
# raw PNC ``AerospikeError`` (not yet wrapped by the SDK command pipeline).
# Tests accept either to stay robust as the wrapping moves forward.
_AnyAerospikeError = (SdkAerospikeError, PncAerospikeError)


_NS = general_namespace()
_SET = "qopproj"
_DS = DataSet.of(_NS, _SET)
_KEY_PREFIX = "qopproj_"
_BIN1 = "tqobin1"
_BIN2 = "tqobin2"
_BIN3 = "tqobin3"
_MAP_BIN = "tqomapbin"
_SIZE = 20


async def _seed_qopproj_dataset(c, wait_for_set_visible):
    """Seed the 20-record dataset and SI used by both ``cluster`` fixtures."""
    session = c.create_session()
    ds = _DS

    # Best-effort cleanup so reruns are deterministic.
    for i in range(1, _SIZE + 1):
        try:
            await session.delete(ds.id(f"{_KEY_PREFIX}{i}")).execute()
        except Exception:
            pass

    for i in range(1, _SIZE + 1):
        await session.upsert(ds.id(f"{_KEY_PREFIX}{i}")).put({
            _BIN1: i,
            _BIN2: i * 10,
            _BIN3: i * 100,
            _MAP_BIN: {"a": i, "b": i * 10},
        }).execute()

    # Wait for all writes to be visible to a set scan before creating the SI
    # — otherwise a still-populating SI can be flagged "readable" before all
    # records have indexed entries, causing range queries to return short.
    await wait_for_set_visible(session, _NS, _SET, _SIZE)

    try:
        index_task = await (
            session.index(_DS).on_bin(_BIN1).named("qopproj_idx_b1").integer().create()
        )
    except Exception:
        # Already present from an earlier run: its build is done, no task to await.
        index_task = None
    if index_task is not None:
        await index_task.wait_till_complete()


async def _drop_qopproj_index(c):
    try:
        await c.create_session().index(_DS).named("qopproj_idx_b1").drop()
    except Exception:
        pass


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition, wait_for_set_visible):
    """Cluster + 20-record dataset on the default seed."""
    async with make_cluster_definition(aerospike_host).connect() as c:
        await _seed_qopproj_dataset(c, wait_for_set_visible)
        yield c
        await _drop_qopproj_index(c)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(cluster):
    return cluster.create_session()


async def _drain(stream):
    out = []
    async for result in stream:
        out.append(result.record_or_raise())
    return out


# =====================================================================
# Basic projection
# =====================================================================


class TestSdkOpsProjBackwardCompat:

    async def test_get_bin_projection(self, session):
        """``with_op_projection(Operation.get_bin)`` over a SI range."""
        stream = await (
            session.query(_DS)
            .filter(Filter.range(_BIN1, 1, 5))
            .with_op_projection(Operation.get_bin(_BIN1))
            .execute()
        )
        records = await _drain(stream)
        assert len(records) == 5
        for rec in records:
            assert 1 <= rec.bins[_BIN1] <= 5
            # Other bins shouldn't surface.
            assert rec.bins.get(_BIN2) is None


# =====================================================================
# Extended reads via the SDK facade
# =====================================================================


class TestSdkOpsProjExtendedReads:

    async def test_exp_read_projection(self, session):
        """Projecting via ``ExpOperation.read`` returns the computed value."""
        stream = await (
            session.query(_DS)
            .filter(Filter.range(_BIN1, 1, 5))
            .with_op_projection(
                Operation.get_bin(_BIN1),
                ExpOperation.read(
                    "doubled",
                    Exp.num_mul([Exp.int_bin(_BIN1), Exp.int_val(2)]),
                    ExpReadFlags.DEFAULT,
                ),
            )
            .execute()
        )
        records = await _drain(stream)
        assert len(records) == 5
        for rec in records:
            assert rec.bins["doubled"] == rec.bins[_BIN1] * 2

    async def test_cdt_select_values_projection(self, session):
        """Path-form CDT read alongside a basic projection."""
        stream = await (
            session.query(_DS)
            .filter(Filter.range(_BIN1, 1, 5))
            .with_op_projection(
                Operation.get_bin(_BIN1),
                CdtOperation.select_values(_MAP_BIN, [CTX.map_key("a")]),
            )
            .execute()
        )
        records = await _drain(stream)
        assert len(records) == 5
        for rec in records:
            v1 = rec.bins[_BIN1]
            # `select_values` returns the value(s) at path-resolved leaves
            # as a list, even when the path resolves to a single node. For
            # the configured map ``{"a": i, "b": i*10}`` the resolved value
            # is ``[i]``.
            assert rec.bins[_MAP_BIN] == [v1]


# =====================================================================
# Negative cases (always run)
# =====================================================================


class TestSdkOpsProjRejects:

    async def test_write_op_in_foreground_rejected(self, session):
        """``Operation.put`` in a foreground query is rejected."""
        with pytest.raises(_AnyAerospikeError) as excinfo:
            stream = await (
                session.query(_DS)
                .filter(Filter.range(_BIN1, 1, 5))
                .with_op_projection(Operation.put("foo", "bar"))
                .execute()
            )
            await _drain(stream)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg

    async def test_exp_write_in_foreground_rejected(self, session):
        """``ExpOperation.write`` in a foreground query is rejected."""
        with pytest.raises(_AnyAerospikeError) as excinfo:
            stream = await (
                session.query(_DS)
                .filter(Filter.range(_BIN1, 1, 5))
                .with_op_projection(
                    ExpOperation.write("foo", Exp.string_val("bar"), ExpWriteFlags.DEFAULT)
                )
                .execute()
            )
            await _drain(stream)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg
