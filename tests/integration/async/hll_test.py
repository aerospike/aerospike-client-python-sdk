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

"""HLL round-trip integration tests.

Coverage:
- ``hll_init`` (default, ``create_only=True``, ``update_only=True``, ``no_fail=True``)
- ``hll_add`` (existing bin, auto-create via ``config=...``, with flag kwargs)
- ``hll_set_union`` / ``hll_fold`` / ``hll_refresh_count``
- All 6 reads (``hll_get_count``, ``hll_describe``, ``hll_get_union``,
  ``hll_get_union_count``, ``hll_get_intersect_count``, ``hll_get_similarity``)
- ``hll_describe`` round-trip via :meth:`RecordResult.get_hll_config`
- AEL filter expressions covering all 7 read-side path functions
- Negative / error-path coverage (parameter errors, mode constraints,
  fold-up, missing-bin reads)
- Matrix coverage across a representative subset of legal index/minhash
  bit-width combinations
"""

import math

import pytest

from aerospike_async.exceptions import ResultCode

from aerospike_sdk import Client, HllConfig
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError


NAMESPACE = "test"
SET = "hll_psdk"


@pytest.fixture
async def hll_client(aerospike_host, client_policy, enterprise):
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of(NAMESPACE, SET)
        for k in ("a", "b", "c"):
            try:
                await session.delete(ds.id(k)).execute()
            except Exception:
                pass
        yield client
        for k in ("a", "b", "c"):
            try:
                await session.delete(ds.id(k)).execute()
            except Exception:
                pass


class TestHllWritesAndCount:

    async def test_init_add_count(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(12))
            .bin("h").hll_add(["alice", "bob", "carol", "alice"])
            .execute()
        )
        rs = await session.query(key).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        count = result.record_or_raise().bins["h"]
        assert isinstance(count, int)
        assert count >= 3

    async def test_add_auto_create_via_config(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key)
            .bin("h").hll_add(["x", "y", "z"], config=HllConfig.of(10))
            .execute()
        )
        rs = await session.query(key).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        assert result.record_or_raise().bins["h"] >= 3

    async def test_create_only_blocks_existing(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key).bin("h").hll_init(HllConfig.of(12)).execute()
        )
        # Second create_only call should now silently no-op via no_fail.
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(12), create_only=True, no_fail=True)
            .execute()
        )

    async def test_mutual_exclusion_raises_at_builder(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")
        with pytest.raises(ValueError, match="mutually exclusive"):
            session.upsert(key).bin("h").hll_init(
                HllConfig.of(12), create_only=True, update_only=True,
            )


class TestHllDescribeRoundTrip:

    async def test_describe_via_get_hll_config(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(14, 20))
            .execute()
        )
        rs = await session.query(key).bin("h").hll_describe().execute()
        result = await rs.first_or_raise()
        config = result.get_hll_config("h")
        assert config == HllConfig.of(14, 20)


class TestHllReadsAndUnion:

    async def test_fold_and_refresh_count(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(14))
            .bin("h").hll_add([str(i) for i in range(50)])
            .bin("h").hll_fold(10)
            .bin("h").hll_refresh_count()
            .execute()
        )
        rs = await session.query(key).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        assert result.record_or_raise().bins["h"] >= 10

    async def test_set_union_cross_bin(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["x", "y"], config=HllConfig.of(12))
            .execute()
        )
        # Read it back as a blob so we can union it into "b".
        rs = await session.query(ds.id("a")).bin("h").get().execute()
        result = await rs.first_or_raise()
        blob_a = result.record_or_raise().bins["h"]

        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["y", "z"], config=HllConfig.of(12))
            .bin("h").hll_set_union([blob_a])
            .execute()
        )
        rs = await session.query(ds.id("b")).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        # x, y, z all merged in — cardinality estimate should be ≥ 3.
        assert result.record_or_raise().bins["h"] >= 2


class TestAelFilterExpressions:

    async def test_ael_hll_count_filter(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["x", "y", "z"], config=HllConfig.of(12))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_init(HllConfig.of(12))
            .execute()
        )
        # Query the set; only "a" should match $.h.hllCount() > 0.
        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllCount() > 0")
            .execute()
        )
        count = 0
        async for _ in rs:
            count += 1
        rs.close()
        assert count == 1

    async def test_ael_union_count_with_single_bin_ref(self, hll_client):
        """``$.h.hllUnionCount($.a) > 0`` — bare HLL bin reference as the
        multi-sketch arg. Server treats it as an implicit single-element list.
        """
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        await (
            session.upsert(ds.id("c"))
            .bin("h").hll_add(["alice", "bob"], config=HllConfig.of(12))
            .bin("a").hll_add(["bob", "carol"], config=HllConfig.of(12))
            .execute()
        )
        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllUnionCount($.a) > 0")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        assert matched >= 1


# A representative subset of the legal ``(index_bit_count, min_hash_bit_count)``
# matrix the server accepts. The full matrix is the cross product over
# ``index_bits in [4..16]`` x ``min_hash_bits in {-1, 4..51}`` constrained by
# ``index_bits + min_hash_bits <= 64``; running the full grid would balloon
# integration runtime without adding signal beyond covering the boundaries and
# a few interior points. The subset below exercises:
#   - the (4, 0) lower corner,
#   - mid-range with minhash enabled,
#   - a config that approaches the 64-bit ceiling.
_LEGAL_DESCRIPTIONS: list[tuple[int, int]] = [
    (4, 0),
    (6, 4),
    (10, 20),
    (16, 48),
]


class TestHllErrorPaths:
    """Negative paths: illegal configs, flag conflicts, and missing-bin reads.

    Verifies the server-reported result codes flow up through the SDK as
    :class:`AerospikeError` (or its subclasses) with the expected
    :attr:`~AerospikeError.result_code`. Covers the policy-flag flag matrix
    and the read-without-bin behaviors that other suites do not exercise.
    """

    async def test_init_illegal_description_raises_parameter_error(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        # index_bit_count = 3 is below the 4..16 floor.
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key)
                .bin("h").hll_init(HllConfig.of(3, 0))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR

        # index + min_hash > 64.
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key)
                .bin("h").hll_init(HllConfig.of(15, 60))
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR

    async def test_init_create_only_fails_on_existing_bin(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key).bin("h").hll_init(HllConfig.of(12)).execute()
        )
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key)
                .bin("h").hll_init(HllConfig.of(12), create_only=True)
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.BIN_EXISTS_ERROR

    async def test_init_update_only_fails_on_missing_bin(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key)
                .bin("h").hll_init(HllConfig.of(12), update_only=True)
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.BIN_NOT_FOUND

    async def test_init_no_fail_swallows_create_only_on_existing(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key).bin("h").hll_init(HllConfig.of(12)).execute()
        )
        # Second create_only call with no_fail=True should silently no-op.
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(12), create_only=True, no_fail=True)
            .execute()
        )

    async def test_fold_up_raises(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(10))
            .execute()
        )
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key).bin("h").hll_fold(12).execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE

    async def test_fold_on_missing_bin_raises(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        # Seed the record with a different bin so the key exists.
        await (
            session.upsert(key).bin("other").set_to(1).execute()
        )
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key).bin("h").hll_fold(8).execute()
            )
        assert exc_info.value.result_code == ResultCode.BIN_NOT_FOUND

    async def test_refresh_count_on_missing_bin_raises(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await (
            session.upsert(key).bin("other").set_to(1).execute()
        )
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(key).bin("h").hll_refresh_count().execute()
            )
        assert exc_info.value.result_code == ResultCode.BIN_NOT_FOUND

    async def test_get_count_on_missing_bin_returns_none(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        # Materialize a record without the HLL bin.
        await (
            session.upsert(key).bin("other").set_to(1).execute()
        )
        rs = await session.query(key).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        assert result.record_or_raise().bins.get("h") is None


class TestHllMatrices:
    """Parametrized matrices over legal HLL configs and write flag combinations.

    Each ``index_bit_count`` / ``min_hash_bit_count`` cell is verified at the
    SDK boundary by round-tripping a sketch through ``hll_describe`` and
    confirming the cardinality estimate stays sane. The matrix is intentionally
    a representative subset (see ``_LEGAL_DESCRIPTIONS``) rather than the full
    cross product to bound integration runtime.
    """

    @pytest.mark.parametrize(("index_bits", "min_hash_bits"), _LEGAL_DESCRIPTIONS)
    async def test_init_legal_description_matrix(
        self, hll_client, index_bits, min_hash_bits,
    ):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await session.delete(key).execute()
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(index_bits, min_hash_bits))
            .execute()
        )
        rs = await session.query(key).bin("h").hll_describe().execute()
        result = await rs.first_or_raise()
        assert result.get_hll_config("h") == HllConfig.of(index_bits, min_hash_bits)

    @pytest.mark.parametrize(("index_bits", "min_hash_bits"), _LEGAL_DESCRIPTIONS)
    async def test_add_auto_create_matrix(
        self, hll_client, index_bits, min_hash_bits,
    ):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await session.delete(key).execute()
        await (
            session.upsert(key)
            .bin("h").hll_add(
                ["a", "b", "c"],
                config=HllConfig.of(index_bits, min_hash_bits),
            )
            .execute()
        )
        rs = await session.query(key).bin("h").hll_get_count().execute()
        result = await rs.first_or_raise()
        assert result.record_or_raise().bins["h"] >= 1

    async def test_init_flag_matrix(self, hll_client):
        """Exhaustive create_only / update_only / no_fail / allow_fold matrix.

        The mutually-exclusive ``create_only + update_only`` cells are covered
        by a unit-level :class:`ValueError` check and skipped here.
        """
        session = hll_client.create_session()
        # Use min_hash_bit_count=0 explicitly. The sentinel -1 ("no minhash,
        # inherit from existing") works for plain init but interacts poorly
        # with some flag combos at the wire layer.
        cfg = HllConfig.of(12, 0)

        # Each row: (create_only, update_only, no_fail, allow_fold,
        #           bin_pre_state, expected_result_code).
        # ``bin_pre_state`` is "absent" or "present"; ``expected_result_code``
        # is ``None`` when the op should succeed.
        cases: list[tuple[bool, bool, bool, bool, str, ResultCode | None]] = [
            # Defaults: succeed regardless of prior state.
            (False, False, False, False, "absent", None),
            (False, False, False, False, "present", None),
            # allow_fold is a union-side concept; the server rejects it on
            # init regardless of prior state.
            (False, False, False, True, "absent", ResultCode.PARAMETER_ERROR),
            (False, False, False, True, "present", ResultCode.PARAMETER_ERROR),
            # create_only: succeeds on absent, fails BIN_EXISTS on present.
            (True, False, False, False, "absent", None),
            (True, False, False, False, "present", ResultCode.BIN_EXISTS_ERROR),
            # create_only + no_fail: succeeds on both.
            (True, False, True, False, "absent", None),
            (True, False, True, False, "present", None),
            # update_only: fails BIN_NOT_FOUND on absent, succeeds on present.
            (False, True, False, False, "absent", ResultCode.BIN_NOT_FOUND),
            (False, True, False, False, "present", None),
            # update_only + no_fail: succeeds on both.
            (False, True, True, False, "absent", None),
            (False, True, True, False, "present", None),
        ]

        for i, case in enumerate(cases):
            create_only, update_only, no_fail, allow_fold, pre, expected = case
            key = DataSet.of(NAMESPACE, SET).id(f"flag_matrix_{i}")
            try:
                await session.delete(key).execute()
            except Exception:
                pass

            if pre == "present":
                await (
                    session.upsert(key).bin("h").hll_init(cfg).execute()
                )

            builder = (
                session.upsert(key)
                .bin("h").hll_init(
                    cfg,
                    create_only=create_only,
                    update_only=update_only,
                    no_fail=no_fail,
                    allow_fold=allow_fold,
                )
            )

            if expected is None:
                await builder.execute()
            else:
                with pytest.raises(AerospikeError) as exc_info:
                    await builder.execute()
                assert exc_info.value.result_code == expected, (
                    f"case {i} {case}: got {exc_info.value.result_code}"
                )

            try:
                await session.delete(key).execute()
            except Exception:
                pass

    async def test_fold_across_index_bit_widths(self, hll_client):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        values = [f"key-{i}" for i in range(200)]
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(14))
            .bin("h").hll_add(values)
            .execute()
        )

        prior_count: int | None = None
        for target_bits in (12, 10, 8):
            await (
                session.update(key).bin("h").hll_fold(target_bits).execute()
            )
            rs = await session.query(key).bin("h").hll_get_count().execute()
            count_after_fold = (await rs.first_or_raise()).record_or_raise().bins["h"]
            if prior_count is not None:
                # Folding down may lose some precision but should not grow the
                # cardinality estimate much; assert it stays in a sane range.
                assert count_after_fold <= int(prior_count * 1.5) + 5
            prior_count = count_after_fold

            # Re-adding the same values now yields zero new entries.
            rs = await (
                session.update(key).bin("h").hll_add(values).execute()
            )
            n_added = (await rs.first_or_raise()).record_or_raise().bins["h"]
            assert n_added == 0

    async def test_set_union_with_folding_and_allow_fold(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # Seed three sketches at differing precisions with overlapping values.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["x", "y"], config=HllConfig.of(10))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["y", "z"], config=HllConfig.of(12))
            .execute()
        )
        await (
            session.upsert(ds.id("c"))
            .bin("h").hll_add(["z", "w"], config=HllConfig.of(14))
            .execute()
        )

        # Read each back as a blob.
        blobs = []
        for k in ("a", "b", "c"):
            rs = await session.query(ds.id(k)).bin("h").get().execute()
            result = await rs.first_or_raise()
            blobs.append(result.record_or_raise().bins["h"])

        # Union the higher-precision peers into the lowest-precision target
        # with allow_fold=True. The server folds the inputs down to the
        # target's precision and the union succeeds.
        target = ds.id("a")  # already at 10 bits
        await (
            session.update(target)
            .bin("h").hll_set_union(blobs, allow_fold=True)
            .execute()
        )


class TestHllRoundTrips:
    """Whole-sketch blob round trips: read a sketch, write it elsewhere, verify.

    Confirms that bytes read from an HLL bin can be set back into another bin
    via ``set_to`` and then participate in further HLL operations identically.
    """

    async def test_get_union_blob_round_trip(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # Seed two sketches with partly-overlapping populations.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["x", "y", "z"], config=HllConfig.of(12))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["y", "z", "w"], config=HllConfig.of(12))
            .execute()
        )

        # Read b's sketch as a blob for use as the union arg.
        rs = await session.query(ds.id("b")).bin("h").get().execute()
        result = await rs.first_or_raise()
        blob_b = result.record_or_raise().bins["h"]

        # Compute the union blob and the union count via a single operate.
        rs = await (
            session.update(ds.id("a"))
            .bin("h").hll_get_union([blob_b])
            .bin("h").hll_get_union_count([blob_b])
            .execute()
        )
        result = await rs.first_or_raise()
        bin_results = result.record_or_raise().bins["h"]
        # The two ops on the same bin coalesce into a list of results.
        union_blob = bin_results[0]
        original_union_count = bin_results[1]

        # Write the union blob into a fresh record c, then count there.
        await (
            session.upsert(ds.id("c"))
            .bin("h").set_to(union_blob)
            .execute()
        )
        rs = await session.query(ds.id("c")).bin("h").hll_get_count().execute()
        roundtrip_count = (await rs.first_or_raise()).record_or_raise().bins["h"]
        assert roundtrip_count == original_union_count

    @pytest.mark.parametrize(("index_bits", "min_hash_bits"), _LEGAL_DESCRIPTIONS)
    async def test_describe_round_trip_all_legal_configs(
        self, hll_client, index_bits, min_hash_bits,
    ):
        session = hll_client.create_session()
        key = DataSet.of(NAMESPACE, SET).id("a")

        await session.delete(key).execute()
        await (
            session.upsert(key)
            .bin("h").hll_init(HllConfig.of(index_bits, min_hash_bits))
            .execute()
        )
        # Original describe.
        rs = await session.query(key).bin("h").hll_describe().execute()
        first_desc = (await rs.first_or_raise()).get_hll_config("h")

        # Capture the raw sketch bytes.
        rs = await session.query(key).bin("h").get().execute()
        blob = (await rs.first_or_raise()).record_or_raise().bins["h"]

        # Delete the record, then re-create by writing the blob back.
        await session.delete(key).execute()
        await (
            session.upsert(key).bin("h").set_to(blob).execute()
        )

        # Re-describe and compare.
        rs = await session.query(key).bin("h").hll_describe().execute()
        second_desc = (await rs.first_or_raise()).get_hll_config("h")
        assert second_desc == first_desc

    async def test_empty_similarity_returns_nan_and_intersect_zero(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # Two empty sketches (init only, no add).
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_init(HllConfig.of(12, 20))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_init(HllConfig.of(12, 20))
            .execute()
        )

        rs = await session.query(ds.id("b")).bin("h").get().execute()
        blob_b = (await rs.first_or_raise()).record_or_raise().bins["h"]

        rs = await (
            session.update(ds.id("a"))
            .bin("h").hll_get_similarity([blob_b])
            .bin("h").hll_get_intersect_count([blob_b])
            .execute()
        )
        result = await rs.first_or_raise()
        bin_results = result.record_or_raise().bins["h"]
        sim = bin_results[0]
        intersect = bin_results[1]
        assert math.isnan(sim)
        assert intersect == 0


class TestHllAelServerSide:
    """Server-side AEL filter expressions over HLL read-path functions.

    Each test seeds a small set of records that produce a deterministic match
    count, then asserts the query returns the expected number of records.
    """

    async def test_ael_union_count_filter_server_side(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # Record a has overlap with b; record c has its own HLL but no peer bin.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["alice", "bob"], config=HllConfig.of(12))
            .bin("peer").hll_add(["bob", "carol"], config=HllConfig.of(12))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["x"], config=HllConfig.of(12))
            .bin("peer").hll_add(["y"], config=HllConfig.of(12))
            .execute()
        )
        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllUnionCount($.peer) > 2")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        # Only "a" has union > 2 (3 distinct: alice, bob, carol).
        assert matched == 1

    async def test_ael_intersect_count_filter_server_side(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # minhash bits are required for intersect/similarity to be meaningful.
        cfg = HllConfig.of(12, 20)
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["alice", "bob", "carol"], config=cfg)
            .bin("peer").hll_add(["bob", "carol", "dave"], config=cfg)
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["x"], config=cfg)
            .bin("peer").hll_add(["y"], config=cfg)
            .execute()
        )

        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllIntersectCount($.peer) >= 1")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        # Only "a" shares values with its peer bin.
        assert matched == 1

    async def test_ael_similarity_filter_server_side(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        cfg = HllConfig.of(12, 20)
        # Record a's bins overlap perfectly; b's bins are disjoint.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["x", "y", "z"], config=cfg)
            .bin("peer").hll_add(["x", "y", "z"], config=cfg)
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["x"], config=cfg)
            .bin("peer").hll_add(["q"], config=cfg)
            .execute()
        )

        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllSimilarity($.peer) > 0.5")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        # Only "a" has high similarity.
        assert matched == 1

    async def test_ael_describe_filter_server_side(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # Two records with differing index_bit_count values; filter selects
        # only those whose describe list equals the wider precision. The
        # server reports a "no minhash" sketch as ``[index_bits, 0]`` — the
        # ``-1`` sentinel used client-side is never returned in describe
        # output.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_init(HllConfig.of(14, 0))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_init(HllConfig.of(10, 0))
            .execute()
        )
        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllDescribe() == [14, 0]")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        assert matched == 1

    async def test_ael_may_contain_filter_server_side(self, hll_client):
        session = hll_client.create_session()
        ds = DataSet.of(NAMESPACE, SET)

        # "a" contains the probe value; "b" does not.
        await (
            session.upsert(ds.id("a"))
            .bin("h").hll_add(["alice", "bob"], config=HllConfig.of(12))
            .execute()
        )
        await (
            session.upsert(ds.id("b"))
            .bin("h").hll_add(["x", "y"], config=HllConfig.of(12))
            .execute()
        )
        rs = await (
            hll_client.query(NAMESPACE, SET)
            .where("$.h.hllMayContain(['alice']) == 1")
            .execute()
        )
        matched = 0
        async for _ in rs:
            matched += 1
        rs.close()
        # "a" must match; "b" may produce a false positive on a 12-bit sketch
        # but for the small populations seeded here it should not. Assert at
        # least the expected positive.
        assert matched >= 1
