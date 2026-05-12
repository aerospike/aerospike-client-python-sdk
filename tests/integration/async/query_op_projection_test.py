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

"""Integration tests for the SDK ``QueryBuilder.with_op_projection`` facade.

Mirrors the reference ``TestQueryOperations`` suite via the SDK's high-level
fluent builder. Covers:

- Backward-compat: basic ``Operation.get_bin`` projection works on any 8.1.x.
- 8.1.2+: ``ExpOperation.read`` / CDT reads accepted in ops projection.
- Pre-8.1.2 client-side gate: extended reads rejected by the core's wire
  encoder with a clear error message (mirrors CLIENT-4609).
- Negative cases: write / touch / delete in foreground queries rejected.

Note: The SDK's ``with_op_projection`` is a thin façade over the PAC
``Statement.set_operations`` underneath, so the per-node version gate
applies transparently. Native ``ExpOperation`` / ``CdtOperation`` are
imported from ``aerospike_async`` directly.
"""

import asyncio

import pytest
import pytest_asyncio
from aerospike_async import (
    CTX,
    CdtOperation,
    ExpOperation,
    ExpReadFlags,
    ExpType,
    ExpWriteFlags,
    Filter,
    FilterExpression as Exp,
    LoopVarPart,
    MapReturnType,
    Operation,
    SelectFlags,
)
from aerospike_sdk import Client, DataSet
from aerospike_sdk.exceptions import AerospikeError as SdkAerospikeError
from aerospike_async.exceptions import AerospikeError as PacAerospikeError

# Errors raised by the core's wire encoder during stream iteration surface as
# raw PAC ``AerospikeError`` (not yet wrapped by the SDK command pipeline).
# Tests accept either to stay robust as the wrapping moves forward.
_AnyAerospikeError = (SdkAerospikeError, PacAerospikeError)


_NS = "test"
_SET = "qopproj"
_KEY_PREFIX = "qopproj_"
_BIN1 = "tqobin1"
_BIN2 = "tqobin2"
_BIN3 = "tqobin3"
_MAP_BIN = "tqomapbin"
_SIZE = 20


async def _seed_qopproj_dataset(c, wait_for_index, wait_for_set_visible):
    """Seed the 20-record dataset and SI used by both ``client`` fixtures."""
    session = c.create_session()
    ds = DataSet.of(_NS, _SET)

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
        await c.index(_NS, _SET).on_bin(_BIN1).named("qopproj_idx_b1").numeric().create()
    except Exception:
        pass
    await wait_for_index(c, _NS, _SET, Filter.range(_BIN1, 1, _SIZE))


async def _drop_qopproj_index(c):
    try:
        await c.index(_NS, _SET).named("qopproj_idx_b1").drop()
    except Exception:
        pass


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy, wait_for_index, wait_for_set_visible):
    """SDK client + 20-record dataset on the broad-surface seed.

    Tests that exercise server-8.1.2-only ops projection should consume
    ``client_812`` instead so they auto-route to the 8.1.2+ cluster when
    one is available and skip cleanly otherwise.
    """
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        await _seed_qopproj_dataset(c, wait_for_index, wait_for_set_visible)
        yield c
        await _drop_qopproj_index(c)


@pytest.fixture
async def client_812(
    aerospike_host_812_required, client_policy, wait_for_index, wait_for_set_visible,
):
    """SDK client + 20-record dataset on the 8.1.2+ seed (function-scoped: pairs with skip fixture).

    The dependent ``aerospike_host_812_required`` fixture skips the dependent test
    cleanly when ``AEROSPIKE_HOST_8_1_2`` is unset.
    """
    async with Client(seeds=aerospike_host_812_required, policy=client_policy) as c:
        await _seed_qopproj_dataset(c, wait_for_index, wait_for_set_visible)
        yield c
        await _drop_qopproj_index(c)


async def _drain(stream):
    out = []
    async for result in stream:
        out.append(result.record_or_raise())
    return out


# =====================================================================
# Backward-compat (any 8.1.x)
# =====================================================================


class TestSdkOpsProjBackwardCompat:

    async def test_get_bin_projection(self, client):
        """``with_op_projection(Operation.get_bin)`` over a SI range."""
        stream = await (
            client.query(_NS, _SET)
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
# 8.1.2+ extended reads via the SDK facade
# =====================================================================


class TestSdkOpsProjExt812:

    async def test_exp_read_projection(self, client_812):
        """Projecting via ``ExpOperation.read`` requires 8.1.2+."""
        stream = await (
            client_812.query(_NS, _SET)
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

    async def test_cdt_select_values_projection(self, client_812):
        """Path-form CDT read alongside a basic projection."""
        stream = await (
            client_812.query(_NS, _SET)
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

    async def test_write_op_in_foreground_rejected(self, client):
        """``Operation.put`` in a foreground query is rejected."""
        with pytest.raises(_AnyAerospikeError) as excinfo:
            stream = await (
                client.query(_NS, _SET)
                .filter(Filter.range(_BIN1, 1, 5))
                .with_op_projection(Operation.put("foo", "bar"))
                .execute()
            )
            await _drain(stream)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg

    async def test_exp_write_in_foreground_rejected(self, client):
        """``ExpOperation.write`` in a foreground query is rejected."""
        with pytest.raises(_AnyAerospikeError) as excinfo:
            stream = await (
                client.query(_NS, _SET)
                .filter(Filter.range(_BIN1, 1, 5))
                .with_op_projection(
                    ExpOperation.write(
                        "foo", Exp.string_val("bar"), ExpWriteFlags.DEFAULT
                    )
                )
                .execute()
            )
            await _drain(stream)
        msg = str(excinfo.value).lower()
        assert "read-only" in msg or "parameter" in msg


# =====================================================================
# Pre-8.1.2 client-side gate (driven by the core via PAC)
# =====================================================================


class TestSdkOpsProjPre812Gate:

    async def test_extended_read_rejected_on_pre_812(
        self, client, server_version, supports_query_ops_projection_ext
    ):
        """The core's wire encoder rejects extended reads on pre-8.1.2."""
        if server_version is None:
            pytest.skip("Could not detect server version")
        if supports_query_ops_projection_ext:
            pytest.skip(
                "Server >= 8.1.2 accepts extended reads; "
                "this test exercises the pre-8.1.2 gate"
            )

        with pytest.raises(_AnyAerospikeError) as excinfo:
            stream = await (
                client.query(_NS, _SET)
                .filter(Filter.range(_BIN1, 1, 5))
                .with_op_projection(
                    ExpOperation.read(
                        "computed", Exp.int_bin(_BIN1), ExpReadFlags.DEFAULT
                    )
                )
                .execute()
            )
            await _drain(stream)
        msg = str(excinfo.value)
        assert "basic read operations" in msg.lower()
        assert "8.1.2" in msg
