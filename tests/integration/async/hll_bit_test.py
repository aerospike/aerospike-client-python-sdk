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
# distributed under the License is distributed on an "AS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Integration tests for HyperLogLog and bit-operation fluent builders."""

import base64

import pytest
import pytest_asyncio

from aerospike_sdk import BinOpInvalidError, BitwiseOverflowActions, Exp, HllConfig
from aerospike_sdk.dataset import DataSet
from tests.integration.namespace import general_namespace


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        test_ds = DataSet.of(general_namespace(), "test")
        for suffix in ("fluent_1", "fluent_2", "b64_1", "b64_2", "b64_3", "table"):
            await session.delete(test_ds.id(f"hll_bit_{suffix}")).execute()
        yield c


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()


async def test_hll_init_add_and_get_count(cluster):
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_fluent_1")
    await (
        session.upsert(k)
        .bin("hll")
        .hll_init(HllConfig.of(12))
        .bin("hll")
        .hll_add(["alpha", "beta", "gamma"])
        .execute()
    )
    rs = await session.query(k).bin("hll").hll_get_count().execute()
    first = await rs.first_or_raise()
    assert first.is_ok
    count = first.record_or_raise().bins["hll"]
    assert isinstance(count, int)
    assert count >= 1


async def test_bit_resize_set_and_get(cluster):
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_fluent_2")
    await (
        session.upsert(k)
        .bin("bits")
        .bit_resize(2)
        .bin("bits")
        .bit_set(0, 8, b"\xab")
        .execute()
    )
    rs = await session.query(k).bin("bits").bit_get(0, 8).execute()
    first = await rs.first_or_raise()
    assert first.is_ok
    raw = first.record_or_raise().bins["bits"]
    assert raw == b"\xab"


async def test_bit_b64_encode_whole_and_ranges(cluster, supports_bit_b64_encode):
    """Whole-blob and byte-range base64 reads in one multi-op call.

    Multiple ops on the same bin return positional results as a list on
    ``record.bins[bin]``. The span is in bytes, not bits, unlike every other
    bit read op; with ``invert_size`` the size counts back from the end, so
    an inverted size of 0 means "to the end".
    """
    if not supports_bit_b64_encode:
        pytest.skip("bit b64_encode requires server >= 8.2.0")
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_b64_1")
    initial = b"\x01\x42\x03"
    await session.upsert(k).bin("bits").set_to(initial).execute()
    rs = await (
        session.query(k)
        .bin("bits").bit_b64_encode()
        .bin("bits").bit_b64_encode(0, 2)
        .bin("bits").bit_b64_encode(1, 0, invert_size=True)
        .bin("bits").bit_b64_encode(-1, 1)
        .execute()
    )
    first = await rs.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins["bits"] == [
        _b64(initial),
        _b64(b"\x01\x42"),
        _b64(b"\x42\x03"),
        _b64(b"\x03"),
    ]


async def test_bit_b64_encode_round_trips_through_str_b64_decode(
    cluster, supports_bit_b64_encode,
):
    """Encode a blob to base64 text, then decode it back via the string op."""
    if not supports_bit_b64_encode:
        pytest.skip("bit b64_encode requires server >= 8.2.0")
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_b64_2")
    initial = b"\xde\xad\xbe\xef"
    await session.upsert(k).bin("bits").set_to(initial).execute()
    rs = await session.query(k).bin("bits").bit_b64_encode().execute()
    text = (await rs.first_or_raise()).record_or_raise().bins["bits"]
    assert text == _b64(initial)
    await session.upsert(k).bin("b64txt").set_to(text).execute()
    rs = await session.query(k).bin("b64txt").str_b64_decode().execute()
    decoded = (await rs.first_or_raise()).record_or_raise().bins["b64txt"]
    assert decoded == initial


async def test_bit_b64_encode_expression_reads(cluster, supports_bit_b64_encode):
    """Whole, span, inverted, and negative-offset expression forms via select_from."""
    if not supports_bit_b64_encode:
        pytest.skip("bit b64_encode requires server >= 8.2.0")
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_b64_3")
    blob = b"\x01\x42\x03\x04\x05"
    await session.upsert(k).bin("bits").set_to(blob).execute()
    bin_exp = Exp.blob_bin("bits")
    rs = await (
        session.query(k)
        .bin("whole").select_from(Exp.bit_b64_encode(bin_exp))
        .bin("span").select_from(
            Exp.bit_b64_encode_range(Exp.val(1), Exp.val(2), False, bin_exp))
        .bin("inverted").select_from(
            Exp.bit_b64_encode_range(Exp.val(1), Exp.val(0), True, bin_exp))
        .bin("negoff").select_from(
            Exp.bit_b64_encode_range(Exp.val(-2), Exp.val(2), False, bin_exp))
        .execute()
    )
    bins = (await rs.first_or_raise()).record_or_raise().bins
    assert bins["whole"] == _b64(blob)
    assert bins["span"] == _b64(b"\x42\x03")
    assert bins["inverted"] == _b64(b"\x42\x03\x04\x05")
    assert bins["negoff"] == _b64(b"\x04\x05")


# Bits, most significant first: 00000001 01000010 00000011 00000100 00000101.
# Offsets and sizes are deliberately unequal and off byte boundaries so that a
# swapped or dropped argument lands on different bits and changes the result.
_BIT_SEED = b"\x01\x42\x03\x04\x05"
_WRAP = BitwiseOverflowActions.WRAP
_SATURATE = BitwiseOverflowActions.SATURATE


@pytest.mark.parametrize(
    ("apply", "expected"),
    [
        pytest.param(lambda b: b.bit_insert(1, b"\xff"), b"\x01\xff\x42\x03\x04\x05", id="insert"),
        pytest.param(lambda b: b.bit_remove(1, 2), b"\x01\x04\x05", id="remove"),
        pytest.param(lambda b: b.bit_set(13, 3, b"\xe0"), b"\x01\x47\x03\x04\x05", id="set"),
        pytest.param(lambda b: b.bit_or(17, 6, b"\xa8"), b"\x01\x42\x57\x04\x05", id="or"),
        pytest.param(lambda b: b.bit_xor(1, 7, b"\xfe"), b"\x7e\x42\x03\x04\x05", id="xor"),
        pytest.param(lambda b: b.bit_and(12, 4, b"\x00"), b"\x01\x40\x03\x04\x05", id="and"),
        pytest.param(lambda b: b.bit_not(25, 6), b"\x01\x42\x03\x7a\x05", id="not"),
        pytest.param(lambda b: b.bit_lshift(32, 8, 3), b"\x01\x42\x03\x04\x28", id="lshift"),
        # The low bit of byte 0 crosses into the top bit of byte 1.
        pytest.param(lambda b: b.bit_rshift(0, 9, 1), b"\x00\xc2\x03\x04\x05", id="rshift"),
        pytest.param(lambda b: b.bit_set_int(8, 16, 0x1234), b"\x01\x12\x34\x04\x05", id="set_int"),
        pytest.param(
            lambda b: b.bit_add(24, 16, 128, False, BitwiseOverflowActions.FAIL),
            b"\x01\x42\x03\x04\x85", id="add",
        ),
        pytest.param(
            lambda b: b.bit_add(0, 8, 255, False, _WRAP), b"\x00\x42\x03\x04\x05", id="add_wrap",
        ),
        pytest.param(
            lambda b: b.bit_add(0, 8, 255, False, _SATURATE), b"\xff\x42\x03\x04\x05",
            id="add_saturate",
        ),
        # 66 + 100 saturates at the signed maximum; unsigned it would be 0xa6.
        pytest.param(
            lambda b: b.bit_add(8, 8, 100, True, _SATURATE), b"\x01\x7f\x03\x04\x05",
            id="add_signed_saturate",
        ),
        pytest.param(
            lambda b: b.bit_subtract(8, 8, 0x43, False, _WRAP), b"\x01\xff\x03\x04\x05",
            id="subtract_wrap",
        ),
        pytest.param(
            lambda b: b.bit_subtract(8, 8, 0x43, False, _SATURATE), b"\x01\x00\x03\x04\x05",
            id="subtract_saturate",
        ),
    ],
)
async def test_bit_write_ops_against_server(cluster, apply, expected):
    """Each write op changes exactly the bits its arguments name."""
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_table")
    await session.upsert(k).bin("bits").set_to(_BIT_SEED).execute()
    await apply(session.update(k).bin("bits")).execute()
    rs = await session.query(k).bins(["bits"]).execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["bits"] == expected


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        # Bits 9-13 are 10000, returned left-aligned in a byte.
        pytest.param(lambda b: b.bit_get(9, 5), b"\x80", id="get"),
        pytest.param(lambda b: b.bit_count(20, 4), 2, id="count"),
        pytest.param(lambda b: b.bit_lscan(24, 8, True), 5, id="lscan"),
        pytest.param(lambda b: b.bit_rscan(32, 8, False), 6, id="rscan"),
        pytest.param(lambda b: b.bit_get_int(8, 8, False), 0x42, id="get_int"),
        # Bits 9-11 are 100: -4 as a 3-bit signed integer, 4 unsigned.
        pytest.param(lambda b: b.bit_get_int(9, 3, True), -4, id="get_int_signed"),
        pytest.param(lambda b: b.bit_get_int(9, 3, False), 4, id="get_int_unsigned"),
    ],
)
async def test_bit_read_ops_against_server(cluster, read, expected):
    """Each read op reports on exactly the bits its arguments name."""
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_table")
    await session.upsert(k).bin("bits").set_to(_BIT_SEED).execute()
    rs = await read(session.query(k).bin("bits")).execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["bits"] == expected


async def test_bit_add_overflow_fail_raises_and_leaves_the_bin(cluster):
    """The FAIL action refuses an overflowing add instead of writing it."""
    session = cluster.create_session()
    k = DataSet.of(general_namespace(), "test").id("hll_bit_table")
    await session.upsert(k).bin("bits").set_to(_BIT_SEED).execute()
    with pytest.raises(BinOpInvalidError):
        await (
            session.update(k).bin("bits")
            .bit_add(0, 8, 255, False, BitwiseOverflowActions.FAIL)
            .execute()
        )
    rs = await session.query(k).bins(["bits"]).execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["bits"] == _BIT_SEED
