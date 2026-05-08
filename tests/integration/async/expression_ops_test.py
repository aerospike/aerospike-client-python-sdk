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

"""Integration tests for expression bin operations.

Coverage:
  - Read expressions (select_from) with various return types
  - Write expressions (insert_from, update_from, upsert_from)
  - Policy error handling (BIN_NOT_FOUND, BIN_EXISTS_ERROR)
  - Eval failure handling (ignore_eval_failure)
  - Combined read + write expression ops
  - Mixed expression + regular ops
  - Dataset query guard
  - Batch key query + select_from
  - Multiple select_from in same execute
"""

import asyncio

import pytest

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode, ServerError
from aerospike_sdk import Client
from aerospike_sdk.exceptions import AerospikeError


NS = "test"
SET = "exp_ops"
KEY_A = "exp_A"
KEY_B = "exp_B"


@pytest.fixture
async def client(aerospike_host, client_policy):
    """Function-scoped seed: writers mutate KEY_A/KEY_B; tests assume a clean slate each run."""
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        # Clean slate
        try:
            await session.delete(_key(KEY_A)).execute()
        except Exception:
            pass
        try:
            await session.delete(_key(KEY_B)).execute()
        except Exception:
            pass

        # Seed: keyA has A=1, D=2; keyB has B=2, D=2
        await session.upsert(_key(KEY_A)).put({"A": 1, "D": 2}).execute()
        await session.upsert(_key(KEY_B)).put({"B": 2, "D": 2}).execute()

        # Brief pause so the query scan index reflects the committed writes under CI load
        await asyncio.sleep(0.1)

        yield c


def _key(name: str) -> Key:
    return Key(NS, SET, name)


# ===================================================================
# Read expression tests (select_from)
# ===================================================================

class TestSelectFrom:

    async def test_select_from_returns_int(self, client):
        """select_from evaluating an integer AEL expression."""
        rs = await (
            client.query(_key(KEY_A)).bin("ev").select_from("$.A + 4")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["ev"] == 5

    async def test_select_from_returns_string(self, client):
        """select_from evaluating a string AEL expression."""
        rs = await (
            client.query(_key(KEY_A)).bin("ev").select_from("'hello'")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["ev"] == "hello"

    async def test_select_from_returns_boolean(self, client):
        """select_from evaluating a boolean AEL expression."""
        rs = await (
            client.query(_key(KEY_A)).bin("ev").select_from("$.A == 1")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["ev"] is True

    async def test_select_from_eval_error(self, client):
        """select_from referencing a missing bin raises server error."""
        with pytest.raises((AerospikeError, ServerError)):
            rs = await (
                client.query(_key(KEY_B)).bin("ev").select_from("$.A + 4")
                    .execute()
            )
            await rs.first_or_raise()

    async def test_select_from_ignore_eval_failure(self, client):
        """select_from with ignore_eval_failure returns None on missing bin."""
        rs = await (
            client.query(_key(KEY_B)).bin("ev").select_from("$.A + 4", ignore_eval_failure=True)
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins.get("ev") is None

    async def test_select_from_returns_nil(self, client):
        """select_from on missing bin with ignore_eval_failure returns None."""
        rs = await (
            client.query(_key(KEY_B)).bin("ev").select_from("$.A", ignore_eval_failure=True)
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins.get("ev") is None

    async def test_multiple_select_from(self, client):
        """Multiple select_from in same execute (expMerge pattern)."""
        rs = await (
            client.query(_key(KEY_A))
                .bin("r1").select_from("$.A == 0 and $.D == 2")
                .bin("r2").select_from("$.A == 0 or $.D == 2")
                .execute()
        )
        result = await rs.first_or_raise()
        assert result.record.bins["r1"] is False
        assert result.record.bins["r2"] is True


# ===================================================================
# Write expression tests
# ===================================================================

class TestUpsertFrom:

    async def test_upsert_from_creates_bin(self, client):
        """upsert_from writes computed value to a new bin."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("C").upsert_from("$.A + 4")
                .execute()
        )
        rec = await session.query(_key(KEY_A)).bin("C").get().execute()
        result = await rec.first_or_raise()
        assert result.record.bins["C"] == 5

    async def test_upsert_from_overwrites_bin(self, client):
        """upsert_from overwrites an existing bin."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("D").upsert_from("$.A + 10")
                .execute()
        )
        rec = await session.query(_key(KEY_A)).bin("D").get().execute()
        result = await rec.first_or_raise()
        assert result.record.bins["D"] == 11


class TestUpdateFrom:

    async def test_update_from_existing_bin(self, client):
        """update_from on an existing bin succeeds."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("D").update_from("$.A + 100")
                .execute()
        )
        rec = await session.query(_key(KEY_A)).bin("D").get().execute()
        result = await rec.first_or_raise()
        assert result.record.bins["D"] == 101

    async def test_update_from_missing_bin_raises(self, client):
        """update_from on non-existent bin raises server error."""
        session = client.create_session()
        with pytest.raises((AerospikeError, ServerError)):
            await (
                session.update(_key(KEY_A)).bin("C").update_from("$.A + 4")
                    .execute()
            )

    async def test_update_from_missing_bin_ignore_op_failure(self, client):
        """update_from with ignore_op_failure silently skips."""
        session = client.create_session()
        stream = await (
            session.update(_key(KEY_A)).bin("C").update_from("$.A + 4", ignore_op_failure=True)
                .execute()
        )
        result = await stream.first_or_raise()
        assert result is not None
        bins = result.record.bins if result.record else {}
        assert bins.get("C") is None


class TestInsertFrom:

    async def test_insert_from_new_bin(self, client):
        """insert_from creates a new bin."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("C").insert_from("$.A + 4")
                .execute()
        )
        rec = await session.query(_key(KEY_A)).bin("C").get().execute()
        result = await rec.first_or_raise()
        assert result.record.bins["C"] == 5

    async def test_insert_from_existing_bin_raises(self, client):
        """insert_from on existing bin raises server error."""
        session = client.create_session()
        # First insert succeeds
        await (
            session.update(_key(KEY_A)).bin("C").insert_from("$.A + 4")
                .execute()
        )
        # Second insert fails
        with pytest.raises((AerospikeError, ServerError)):
            await (
                session.update(_key(KEY_A)).bin("C").insert_from("$.A + 4")
                    .execute()
            )

    async def test_insert_from_existing_bin_ignore_op_failure(self, client):
        """insert_from with ignore_op_failure silently skips."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("C").insert_from("$.A + 4")
                .execute()
        )
        stream = await (
            session.update(_key(KEY_A)).bin("C").insert_from("$.A + 99", ignore_op_failure=True)
                .execute()
        )
        result = await stream.first_or_raise()
        assert result is not None


# ===================================================================
# Combined read + write expression tests
# ===================================================================

class TestCombinedExpression:

    async def test_upsert_from_and_select_from(self, client):
        """upsert_from + select_from in same execute."""
        session = client.create_session()
        stream = await (
            session.update(_key(KEY_A))
                .bin("D").upsert_from("$.D + 10")
                .bin("ev").select_from("$.A")
                .execute()
        )
        result = await stream.first_or_raise()
        assert result is not None
        assert result.record.bins["ev"] == 1

    async def test_upsert_from_and_get(self, client):
        """upsert_from + .get() in same execute."""
        session = client.create_session()
        await (
            session.update(_key(KEY_A)).bin("C").upsert_from("$.A + 4").execute()
        )
        rec = await session.query(_key(KEY_A)).bin("C").get().execute()
        result = await rec.first_or_raise()
        assert result is not None
        assert result.record.bins["C"] == 5

    async def test_write_eval_error_with_ignore(self, client):
        """upsert_from + select_from with ignore_eval_failure on both."""
        session = client.create_session()
        stream = await (
            session.update(_key(KEY_B))
                .bin("C").upsert_from("$.A + 4", ignore_eval_failure=True)
                .bin("ev").select_from("$.A", ignore_eval_failure=True)
                .execute()
        )
        result = await stream.first_or_raise()
        assert result is not None
        bins = result.record.bins if result.record else {}
        assert bins.get("ev") is None


# ===================================================================
# Mixed expression + regular ops
# ===================================================================

class TestMixedOps:

    async def test_set_to_and_upsert_from(self, client):
        """set_to + upsert_from in same execute."""
        session = client.create_session()
        await (
            session.upsert(_key(KEY_A))
                .bin("name").set_to("Alice")
                .bin("computed").upsert_from("$.A * 2")
                .execute()
        )
        rec = await (
            session.query(_key(KEY_A))
                .bin("name").get()
                .bin("computed").get()
                .execute()
        )
        result = await rec.first_or_raise()
        assert result.record.bins["name"] == "Alice"
        assert result.record.bins["computed"] == 2


# ===================================================================
# Guard tests
# ===================================================================

class TestGuards:

    async def test_dataset_query_select_from_raises(self, client):
        """select_from on dataset query raises OP_NOT_APPLICABLE."""
        session = client.create_session()
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.query(namespace=NS, set_name=SET).bin("ev").select_from("$.A + 4")
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE

    async def test_batch_key_query_select_from_works(self, client):
        """select_from on batch key query works (no guard)."""
        rs = await (
            client.query([_key(KEY_A), _key(KEY_B)]).bin("ev").select_from("$.D * 3")
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        for r in results:
            assert r.record.bins["ev"] == 6
