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

"""Tests for the Exp alias and val() convenience function.

Tests expression building and usage with actual database operations.
"""

import asyncio
import inspect

import pytest
import pytest_asyncio
from aerospike_async import FilterExpression
from aerospike_async.exceptions import InvalidRequest

from aerospike_sdk import AelParseException, Exp, Client, in_list, map_keys, map_values, val
from aerospike_sdk.dataset import DataSet

from tests.cluster_version import min_active_server_version_tuple
from tests.version_xfail import ServerVersionGte, ServerVersionLt, server_version_gte


class TestExpAlias:
    """Test that Exp is properly aliased to FilterExpression."""

    def test_exp_is_filter_expression(self):
        assert Exp is FilterExpression

    def test_exp_methods_available(self):
        assert hasattr(Exp, "int_bin")
        assert hasattr(Exp, "string_bin")
        assert hasattr(Exp, "float_bin")
        assert hasattr(Exp, "eq")
        assert hasattr(Exp, "ne")
        assert hasattr(Exp, "gt")
        assert hasattr(Exp, "ge")
        assert hasattr(Exp, "lt")
        assert hasattr(Exp, "le")
        assert hasattr(Exp, "and_")
        assert hasattr(Exp, "or_")
        assert hasattr(Exp, "not_")


class TestVal:
    """Test the val() convenience function."""

    def test_val_bool_true(self):
        expr = val(True)
        assert expr is not None

    def test_val_bool_false(self):
        expr = val(False)
        assert expr is not None

    def test_val_int(self):
        expr = val(42)
        assert expr is not None

    def test_val_int_negative(self):
        expr = val(-100)
        assert expr is not None

    def test_val_float(self):
        expr = val(3.14)
        assert expr is not None

    def test_val_string(self):
        expr = val("hello")
        assert expr is not None

    def test_val_bytes(self):
        expr = val(b"\x01\x02\x03")
        assert expr is not None

    def test_val_bytearray(self):
        expr = val(bytearray([1, 2, 3]))
        assert expr is not None

    def test_val_list(self):
        expr = val([1, 2, 3])
        assert expr is not None

    def test_val_dict(self):
        expr = val({"key": "value"})
        assert expr is not None

    def test_val_none(self):
        expr = val(None)
        assert expr is not None

    def test_val_unsupported_type(self):
        with pytest.raises(TypeError, match="Unsupported type"):
            val(object())


class TestExpComposition:
    """Test composing expressions using Exp and val()."""

    def test_eq_with_val(self):
        expr = Exp.eq(Exp.int_bin("age"), val(30))
        assert expr is not None

    def test_gt_with_val(self):
        expr = Exp.gt(Exp.int_bin("count"), val(100))
        assert expr is not None

    def test_string_comparison(self):
        expr = Exp.eq(Exp.string_bin("name"), val("Alice"))
        assert expr is not None

    def test_and_expression(self):
        expr = Exp.and_([
            Exp.eq(Exp.string_bin("country"), val("US")),
            Exp.gt(Exp.int_bin("age"), val(21))
        ])
        assert expr is not None

    def test_or_expression(self):
        expr = Exp.or_([
            Exp.eq(Exp.string_bin("status"), val("active")),
            Exp.eq(Exp.string_bin("status"), val("pending"))
        ])
        assert expr is not None

    def test_not_expression(self):
        expr = Exp.not_(Exp.eq(Exp.int_bin("age"), val(0)))
        assert expr is not None

    def test_complex_nested_expression(self):
        expr = Exp.and_([
            Exp.eq(Exp.string_bin("country"), val("US")),
            Exp.or_([
                Exp.gt(Exp.int_bin("age"), val(21)),
                Exp.eq(Exp.string_bin("vip"), val("true"))
            ])
        ])
        assert expr is not None


class TestArithmeticExpressions:
    """Test arithmetic expression building."""

    def test_num_add(self):
        expr = Exp.eq(
            Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")]),
            val(10)
        )
        assert expr is not None

    def test_num_sub(self):
        expr = Exp.eq(
            Exp.num_sub([Exp.int_bin("a"), Exp.int_bin("b")]),
            val(5)
        )
        assert expr is not None

    def test_num_mul(self):
        expr = Exp.eq(
            Exp.num_mul([Exp.int_bin("a"), val(2)]),
            val(20)
        )
        assert expr is not None

    def test_num_div(self):
        expr = Exp.eq(
            Exp.num_div([Exp.int_bin("a"), val(2)]),
            val(5)
        )
        assert expr is not None

    def test_num_mod(self):
        expr = Exp.eq(
            Exp.num_mod(Exp.int_bin("a"), val(2)),
            val(0)
        )
        assert expr is not None

    def test_num_abs(self):
        expr = Exp.eq(
            Exp.num_abs(Exp.int_bin("negative")),
            val(5)
        )
        assert expr is not None

    def test_num_floor(self):
        expr = Exp.eq(
            Exp.num_floor(Exp.float_bin("f")),
            val(2.0)
        )
        assert expr is not None

    def test_num_ceil(self):
        expr = Exp.eq(
            Exp.num_ceil(Exp.float_bin("f")),
            val(3.0)
        )
        assert expr is not None


class TestBitwiseExpressions:
    """Test bitwise expression building."""

    def test_int_and(self):
        expr = Exp.eq(
            Exp.int_and([Exp.int_bin("a"), val(0xFF)]),
            val(1)
        )
        assert expr is not None

    def test_int_or(self):
        expr = Exp.eq(
            Exp.int_or([Exp.int_bin("a"), val(0xFF)]),
            val(0xFF)
        )
        assert expr is not None

    def test_int_xor(self):
        expr = Exp.eq(
            Exp.int_xor([Exp.int_bin("a"), val(0xFF)]),
            val(0xFE)
        )
        assert expr is not None

    def test_int_not(self):
        expr = Exp.eq(
            Exp.int_not(Exp.int_bin("a")),
            val(-2)
        )
        assert expr is not None

    def test_int_lshift(self):
        expr = Exp.eq(
            Exp.int_lshift(Exp.int_bin("a"), val(2)),
            val(4)
        )
        assert expr is not None

    def test_int_rshift(self):
        expr = Exp.eq(
            Exp.int_rshift(Exp.int_bin("a"), val(2)),
            val(0)
        )
        assert expr is not None


class TestComparisonExpressions:
    """Test all comparison operators."""

    def test_eq(self):
        expr = Exp.eq(Exp.int_bin("a"), val(1))
        assert expr is not None

    def test_ne(self):
        expr = Exp.ne(Exp.int_bin("a"), val(0))
        assert expr is not None

    def test_gt(self):
        expr = Exp.gt(Exp.int_bin("a"), val(0))
        assert expr is not None

    def test_ge(self):
        expr = Exp.ge(Exp.int_bin("a"), val(1))
        assert expr is not None

    def test_lt(self):
        expr = Exp.lt(Exp.int_bin("a"), val(10))
        assert expr is not None

    def test_le(self):
        expr = Exp.le(Exp.int_bin("a"), val(10))
        assert expr is not None


class TestTypeConversions:
    """Test type conversion expressions."""

    def test_to_int(self):
        expr = Exp.eq(
            Exp.to_int(Exp.float_bin("f")),
            val(2)
        )
        assert expr is not None

    def test_to_float(self):
        expr = Exp.eq(
            Exp.to_float(Exp.int_bin("a")),
            val(2.0)
        )
        assert expr is not None


class TestMetadataExpressions:
    """Test metadata expression building."""

    def test_ttl(self):
        expr = Exp.gt(Exp.ttl(), val(0))
        assert expr is not None

    def test_last_update(self):
        expr = Exp.gt(Exp.last_update(), val(0))
        assert expr is not None

    def test_since_update(self):
        expr = Exp.gt(Exp.since_update(), val(0))
        assert expr is not None

    def test_void_time(self):
        expr = Exp.gt(Exp.void_time(), val(0))
        assert expr is not None

    def test_is_tombstone(self):
        expr = Exp.eq(Exp.is_tombstone(), val(False))
        assert expr is not None

    def test_set_name(self):
        expr = Exp.eq(Exp.set_name(), val("test_set"))
        assert expr is not None

    def test_digest_modulo(self):
        expr = Exp.eq(Exp.digest_modulo(3), val(1))
        assert expr is not None

    def test_key_exists(self):
        expr = Exp.key_exists()
        assert expr is not None

    def test_bin_exists(self):
        expr = Exp.bin_exists("mybin")
        assert expr is not None

    def test_bin_type(self):
        expr = Exp.bin_type("mybin")
        assert expr is not None


class TestBinExpressions:
    """Test different bin type expressions."""

    def test_int_bin(self):
        expr = Exp.int_bin("int_bin")
        assert expr is not None

    def test_string_bin(self):
        expr = Exp.string_bin("string_bin")
        assert expr is not None

    def test_float_bin(self):
        expr = Exp.float_bin("float_bin")
        assert expr is not None

    def test_blob_bin(self):
        expr = Exp.blob_bin("blob_bin")
        assert expr is not None

    def test_list_bin(self):
        expr = Exp.list_bin("list_bin")
        assert expr is not None

    def test_map_bin(self):
        expr = Exp.map_bin("map_bin")
        assert expr is not None

    def test_geo_bin(self):
        expr = Exp.geo_bin("geo_bin")
        assert expr is not None

    def test_hll_bin(self):
        expr = Exp.hll_bin("hll_bin")
        assert expr is not None


# Integration tests with actual database operations

@pytest.fixture
async def client(aerospike_host, client_policy):
    """Connected SDK client (shared by data fixtures; used by version-xfail autouse)."""
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest_asyncio.fixture(autouse=True)
async def _runtime_server_version_xfail(
    request: pytest.FixtureRequest,
    client: Client,
) -> None:
    """Apply ``@pytest.mark.xfail(condition=server_version_*(...))`` before async DB tests.

    ``client`` is a declared dependency so pytest-asyncio does not call
    ``getfixturevalue("client")`` from inside this async autouse (that nests
    ``asyncio.Runner`` and fails on Python 3.14 + uvloop).
    """
    if not inspect.iscoroutinefunction(request.function):
        return
    mark = request.node.get_closest_marker("xfail")
    if mark is None:
        return
    cond = mark.kwargs.get("condition")
    if not isinstance(cond, (ServerVersionLt, ServerVersionGte)):
        return
    cluster_min = await min_active_server_version_tuple(client)
    if cond.should_xfail(cluster_min):
        pytest.xfail(reason=mark.kwargs.get("reason", "server version xfail"))


@pytest.fixture
async def client_with_data(client, enterprise):
    """Setup test data for expression tests."""
    session = client.create_session()
    ds = DataSet.of("test", "exp_test")

    for key in ["A", "B", "C"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("A")).put({"A": 1, "B": 1.1, "C": "abcde", "D": 1, "E": -1}).execute()
    await session.upsert(ds.id("B")).put({"A": 2, "B": 2.2, "C": "abcdeabcde", "D": 1, "E": -2}).execute()
    await session.upsert(ds.id("C")).put({"A": 0, "B": -1.0, "C": "1", "D": 0, "E": 0}).execute()

    await asyncio.sleep(0.5 if not enterprise else 0.01)

    yield client

    for key in ["A", "B", "C"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestExpWithQuery:
    """Test Exp expressions with actual query operations."""

    async def test_query_with_eq_filter(self, client_with_data):
        """Test query with equality filter expression."""
        filter_exp = Exp.eq(Exp.int_bin("A"), val(1))

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 1

    async def test_query_with_gt_filter(self, client_with_data):
        """Test query with greater-than filter expression."""
        filter_exp = Exp.gt(Exp.int_bin("A"), val(1))

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 2

    async def test_query_with_and_filter(self, client_with_data):
        """Test query with AND filter expression."""
        filter_exp = Exp.and_([
            Exp.eq(Exp.int_bin("A"), val(1)),
            Exp.eq(Exp.int_bin("D"), val(1))
        ])

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 1
        assert records[0].bins["D"] == 1

    async def test_query_with_or_filter(self, client_with_data):
        """Test query with OR filter expression."""
        filter_exp = Exp.or_([
            Exp.eq(Exp.int_bin("A"), val(1)),
            Exp.eq(Exp.int_bin("A"), val(2))
        ])

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2

    async def test_query_with_not_filter(self, client_with_data):
        """Test query with NOT filter expression."""
        filter_exp = Exp.not_(Exp.eq(Exp.int_bin("A"), val(0)))

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert rec.bins["A"] != 0

    async def test_query_with_arithmetic_filter(self, client_with_data):
        """Test query with arithmetic filter: A + D == 2."""
        filter_exp = Exp.eq(
            Exp.num_add([Exp.int_bin("A"), Exp.int_bin("D")]),
            val(2)
        )

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] + records[0].bins["D"] == 2

    async def test_query_with_string_filter(self, client_with_data):
        """Test query filtering on string bin."""
        filter_exp = Exp.eq(Exp.string_bin("C"), val("abcde"))

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["C"] == "abcde"

    async def test_query_no_matches(self, client_with_data):
        """Test query that matches no records."""
        filter_exp = Exp.eq(Exp.int_bin("A"), val(999))

        stream = await (
            client_with_data.query("test", "exp_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 0


class TestExpWithAel:
    """Test AEL string expressions with where() method.

    Type inference: Bin types are automatically inferred from comparison operands.
    For example, `$.A == 1` infers A is an int_bin because 1 is an integer.
    Explicit casts like .asInt() are still supported but typically not needed.
    """

    async def test_where_eq_int(self, client_with_data):
        """Test AEL equality with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.A == 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 1

    async def test_where_gt_int(self, client_with_data):
        """Test AEL greater-than with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.A > 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 2

    async def test_where_and_int(self, client_with_data):
        """Test AEL AND expression with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.A == 1 and $.D == 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["A"] == 1

    async def test_where_or_int(self, client_with_data):
        """Test AEL OR expression with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.A == 1 or $.A == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2

    async def test_where_not_int(self, client_with_data):
        """Test AEL NOT expression with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("not ($.A == 0)")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2

    async def test_where_arithmetic_int(self, client_with_data):
        """Test AEL arithmetic expression with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("($.A + $.D) == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1

    async def test_where_string(self, client_with_data):
        """Test AEL string comparison with automatic string inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.C == 'abcde'")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["C"] == "abcde"

    async def test_where_complex_int(self, client_with_data):
        """Test complex AEL expression with automatic int inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("($.A > 0 and $.D == 1) or $.A == 0")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 3

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="Server-side asInt() cast emits invalid msgpack (ParameterError at eval time) "
        "— server bug, pending fix",
        strict=True,
    )
    async def test_where_explicit_cast_still_works(self, client_with_data):
        """Test that asInt() casts a float bin to int for comparison."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.B.asInt() == 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["B"] == 1.1

    async def test_where_float_comparison(self, client_with_data):
        """Test AEL float comparison with automatic float inference."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("$.B > 1.0")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert rec.bins["B"] > 1.0

    async def test_where_invalid_ael(self, client_with_data):
        """Test that invalid AEL raises ParameterError."""
        stream = await (
            client_with_data.query("test", "exp_test")
            .where("this is not valid AEL !!!")
            .execute()
        )
        with pytest.raises(InvalidRequest, match="ParameterError"):
            async for result in stream:
                pass


# CDT Path Access Tests

@pytest.fixture
async def client_with_cdt_data(client, enterprise):
    """Setup test data with lists and maps for CDT path tests."""
    session = client.create_session()
    ds = DataSet.of("test", "cdt_test")

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("rec1")).put({
        "numbers": [10, 20, 30, 40, 50],
        "names": ["alice", "bob", "charlie"],
        "info": {"name": "Alice", "age": 30, "city": "NYC"},
        "nested": [{"id": 1, "value": 100}, {"id": 2, "value": 200}],
    }).execute()
    await session.upsert(ds.id("rec2")).put({
        "numbers": [5, 15, 25, 35, 45],
        "names": ["dave", "eve"],
        "info": {"name": "Bob", "age": 25, "city": "LA"},
        "nested": [{"id": 3, "value": 300}],
    }).execute()
    await session.upsert(ds.id("rec3")).put({
        "numbers": [100, 200, 300],
        "names": ["frank"],
        "info": {"name": "Charlie", "age": 40, "city": "NYC"},
        "nested": [{"id": 4, "value": 400}, {"id": 5, "value": 500}],
    }).execute()

    await asyncio.sleep(0.5 if not enterprise else 0.01)

    yield client

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestCdtPathWithExp:
    """Test CDT path expressions using the Exp builder."""

    async def test_list_get_by_index(self, client_with_cdt_data):
        """Test list_get_by_index using Exp builder."""
        from aerospike_async import ExpType, ListReturnType

        # Filter: numbers[0] == 10
        filter_exp = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("numbers"),
                [],
            ),
            val(10),
        )

        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["numbers"][0] == 10

    async def test_map_get_by_key(self, client_with_cdt_data):
        """Test map_get_by_key using Exp builder."""
        from aerospike_async import ExpType, MapReturnType

        # Filter: info.age == 30
        filter_exp = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("age"),
                Exp.map_bin("info"),
                [],
            ),
            val(30),
        )

        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["info"]["age"] == 30


class TestCdtPathWithAel:
    """Test CDT path expressions using the AEL parser."""

    async def test_list_index_access(self, client_with_cdt_data):
        """Test AEL list index access: $.numbers.[0] == 10
        
        Note: The AEL grammar requires a dot before brackets: .[0] not [0]
        """
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.[0] == 10")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["numbers"][0] == 10

    async def test_list_negative_index(self, client_with_cdt_data):
        """Test AEL list negative index access: $.numbers.[-1] == 50
        
        Note: The AEL grammar requires a dot before brackets: .[-1] not [-1]
        """
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.[-1] == 50")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["numbers"][-1] == 50

    async def test_map_key_access(self, client_with_cdt_data):
        """Test AEL map key access: $.info.age == 30"""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.info.age == 30")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["info"]["age"] == 30

    async def test_map_key_string_comparison(self, client_with_cdt_data):
        """Test AEL map key access with string comparison: $.info.city == 'NYC'"""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.info.city == 'NYC'")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert rec.bins["info"]["city"] == "NYC"

    async def test_list_index_greater_than(self, client_with_cdt_data):
        """Test AEL list index with greater than: $.numbers.[0] > 50
        
        Note: The AEL grammar requires a dot before brackets: .[0] not [0]
        """
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.[0] > 50")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["numbers"][0] > 50

    async def test_map_key_with_and(self, client_with_cdt_data):
        """Test AEL map key with AND: $.info.age > 25 and $.info.city == 'NYC'"""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.info.age > 25 and $.info.city == 'NYC'")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert rec.bins["info"]["age"] > 25
            assert rec.bins["info"]["city"] == "NYC"


class TestExistsAndCount:
    """Test exists() and count() AEL functions."""

    async def test_bin_exists(self, client_with_cdt_data):
        """Test $.binName.exists() for checking bin existence."""
        # All records have "numbers" bin
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.exists()")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 3

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
    )
    async def test_list_count_comparison(self, client_with_cdt_data):
        """Test $.listBin.count() for getting list size."""
        # rec1 has 5 numbers, rec2 has 5 numbers, rec3 has 3 numbers
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.count() > 3")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert len(rec.bins["numbers"]) > 3

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
        raises=InvalidRequest,
    )
    async def test_list_count_equals(self, client_with_cdt_data):
        """Test $.listBin.count() == value."""
        # rec3 has exactly 3 numbers
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.count() == 3")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert len(records[0].bins["numbers"]) == 3

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
        raises=InvalidRequest,
    )
    async def test_names_list_count(self, client_with_cdt_data):
        """Test count on names list."""
        # rec1: 3 names, rec2: 2 names, rec3: 1 name
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.names.count() >= 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert len(rec.bins["names"]) >= 2

    async def test_exists_with_and(self, client_with_cdt_data):
        """Test exists() combined with other conditions."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.exists() and $.info.age > 30")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["info"]["age"] > 30

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
        raises=InvalidRequest,
    )
    async def test_count_with_arithmetic(self, client_with_cdt_data):
        """Test count() in arithmetic expressions."""
        # Count of numbers + count of names > 5
        # rec1: 5+3=8, rec2: 5+2=7, rec3: 3+1=4
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("($.numbers.count() + $.names.count()) > 5")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2


@pytest.fixture
async def client_with_list_data(client, enterprise):
    """Setup test data with various lists for advanced list AEL tests."""
    session = client.create_session()
    ds = DataSet.of("test", "list_ael_test")

    for key in ["rec1", "rec2", "rec3", "rec4"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("rec1")).put({
        "values": [10, 20, 30, 40, 50],
        "tags": ["alpha", "beta", "gamma"],
    }).execute()
    await session.upsert(ds.id("rec2")).put({
        "values": [5, 15, 25, 35, 45],
        "tags": ["alpha", "delta"],
    }).execute()
    await session.upsert(ds.id("rec3")).put({
        "values": [100, 30, 200],
        "tags": ["beta", "epsilon"],
    }).execute()
    await session.upsert(ds.id("rec4")).put({
        "values": [1, 2, 3, 4, 5],
        "tags": ["zeta"],
    }).execute()

    await asyncio.sleep(0.5 if not enterprise else 0.01)

    yield client

    for key in ["rec1", "rec2", "rec3", "rec4"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestAdvancedListAel:
    """Test advanced list AEL features."""

    async def test_list_by_rank_largest(self, client_with_list_data):
        """Test $.list.[#-1] to get largest value (by rank)."""
        # [#-1] gets the largest value
        # rec1: 50, rec2: 45, rec3: 200, rec4: 5
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[#-1] > 100")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert max(records[0].bins["values"]) > 100

    async def test_list_by_rank_smallest(self, client_with_list_data):
        """Test $.list.[#0] to get smallest value (by rank)."""
        # [#0] gets the smallest value
        # rec1: 10, rec2: 5, rec3: 30, rec4: 1
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[#0] < 5")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert min(records[0].bins["values"]) < 5

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
    )
    async def test_list_by_value(self, client_with_list_data):
        """Test $.list.[=value] to find items containing specific value."""
        # rec1 and rec3 have 30 in their values list
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[=30].count() > 0")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert 30 in rec.bins["values"]

    async def test_list_index_range(self, client_with_list_data):
        """Test $.list.[1:3] to get a range of indices."""
        # [1:3] gets indices 1 and 2 (count=2)
        # We can't directly compare the returned list in AEL,
        # but we can verify it parses and executes without error
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[1:3].count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # All records should have at least 3 elements, so [1:3] returns 2 items
        assert len(records) == 4

    async def test_list_index_range_from_start(self, client_with_list_data):
        """Test $.list.[2:] to get from index 2 to end."""
        # All 5-element lists have 3 items from index 2
        # rec1: [30, 40, 50] (3 items)
        # rec2: [25, 35, 45] (3 items)
        # rec3: [200] (1 item - only 3 elements total)
        # rec4: [3, 4, 5] (3 items)
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[2:].count() == 3")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 3

    async def test_list_value_range(self, client_with_list_data):
        """Test $.list.[=10:40] to get values in range."""
        # [=10:40] gets values >= 10 and < 40
        # rec1: [10, 20, 30, 40, 50] -> [10, 20, 30] (3 items)
        # rec2: [5, 15, 25, 35, 45] -> [15, 25, 35] (3 items)
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[=10:40].count() == 3")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2

    async def test_list_rank_range(self, client_with_list_data):
        """Test $.list.[#0:2] to get smallest 2 items by rank."""
        # [#0:2] gets rank 0 and 1 (2 smallest items)
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.values.[#0:2].count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # All records have at least 2 items
        assert len(records) == 4

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
    )
    async def test_list_value_list(self, client_with_list_data):
        """Test $.list.[=a,b,c] to find items matching value list."""
        # Find records where tags contain "alpha"
        stream = await (
            client_with_list_data.query("test", "list_ael_test")
            .where("$.tags.[=alpha].count() > 0")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 2
        for rec in records:
            assert "alpha" in rec.bins["tags"]


@pytest.fixture
async def client_with_map_data(client, enterprise):
    """Setup test data with maps for advanced map AEL tests."""
    session = client.create_session()
    ds = DataSet.of("test", "map_ael_test")

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("rec1")).put({
        "scores": {"alice": 90, "bob": 85, "charlie": 95},
        "metadata": {"type": "premium", "level": 3},
    }).execute()
    await session.upsert(ds.id("rec2")).put({
        "scores": {"dave": 75, "eve": 80},
        "metadata": {"type": "basic", "level": 1},
    }).execute()
    await session.upsert(ds.id("rec3")).put({
        "scores": {"frank": 100, "grace": 70, "heidi": 88},
        "metadata": {"type": "premium", "level": 2},
    }).execute()

    await asyncio.sleep(0.25 if not enterprise else 0.01)

    yield client

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestAdvancedMapAel:
    """Test advanced map AEL features."""

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="Server-side count() cast emits invalid msgpack (ParameterError at eval time) "
        "— server bug, pending fix",
        strict=True,
    )
    async def test_map_by_value(self, client_with_map_data):
        """Test $.map.{=value} to find entries with specific value."""
        # Find records where scores contains value 100
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{=100}.count() > 0")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert 100 in records[0].bins["scores"].values()

    async def test_map_index_range(self, client_with_map_data):
        """Test $.map.{0:2} to get first 2 entries by index."""
        # Get first 2 entries (count=2)
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{0:2}.count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # rec2 has only 2 entries, others have 3
        assert len(records) == 3

    async def test_map_value_range(self, client_with_map_data):
        """Test $.map.{=80:95} to get values in range."""
        # Get values >= 80 and < 95
        # rec1: bob=85, alice=90 (2 items)
        # rec2: eve=80 (1 item)
        # rec3: heidi=88 (1 item)
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{=80:95}.count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1

    async def test_map_rank_range(self, client_with_map_data):
        """Test $.map.{#0:2} to get smallest 2 values by rank."""
        # Get 2 smallest values
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{#0:2}.count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 3


# =============================================================================
# Nested operations, key range/list, etc.
# =============================================================================

@pytest.fixture
async def client_with_nested_data(client, enterprise):
    """Setup test data with deeply nested structures."""
    session = client.create_session()
    ds = DataSet.of("test", "nested_ael_test")

    for key in ["rec1", "rec2"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("rec1")).put({
        "nested_list": [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        "nested_map": {
            "a": {"aa": 100, "ab": 200},
            "b": {"ba": 300, "bb": 400},
        },
        "simple_list": [1, 2, 3, 4, 5],
    }).execute()
    await session.upsert(ds.id("rec2")).put({
        "nested_list": [[5, 10], [15, 20], [25, 30]],
        "nested_map": {
            "a": {"aa": 50, "ab": 60},
            "b": {"ba": 70, "bb": 80},
        },
        "simple_list": [10, 20, 30],
    }).execute()

    await asyncio.sleep(0.25 if not enterprise else 0.01)

    yield client

    for key in ["rec1", "rec2"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestNestedCdtAel:
    """Tests for nested CDT operations."""

    async def test_nested_list_access(self, client_with_nested_data):
        """Test $.list.[0].[1] - nested list index access."""
        # nested_list[0][1] = 20 for rec1, 10 for rec2
        stream = await (
            client_with_nested_data.query("test", "nested_ael_test")
            .where("$.nested_list.[0].[1] == 20")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["nested_list"][0][1] == 20

    async def test_nested_map_access(self, client_with_nested_data):
        """Test $.map.a.aa - nested map key access."""
        # nested_map.a.aa = 100 for rec1, 50 for rec2
        stream = await (
            client_with_nested_data.query("test", "nested_ael_test")
            .where("$.nested_map.a.aa == 100")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert records[0].bins["nested_map"]["a"]["aa"] == 100

    async def test_nested_list_count(self, client_with_nested_data):
        """Test $.list.[0].count() - count of nested list."""
        # nested_list[0] has 3 elements for rec1, 2 for rec2
        stream = await (
            client_with_nested_data.query("test", "nested_ael_test")
            .where("$.nested_list.[0].count() == 3")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1
        assert len(records[0].bins["nested_list"][0]) == 3

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="Server-side bin.count() emits invalid msgpack (ParameterError at eval time) "
        "— server bug, pending fix",
        strict=True,
    )
    async def test_list_size_simple(self, client_with_nested_data):
        """Test $.list.count() - basic list size."""
        stream = await (
            client_with_nested_data.query("test", "nested_ael_test")
            .where("$.simple_list.count() == 5")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1

    async def test_nested_list_with_rank(self, client_with_nested_data):
        """Test $.list.[0].[#-1] - rank in nested list."""
        # nested_list[0] largest: 30 for rec1, 10 for rec2
        stream = await (
            client_with_nested_data.query("test", "nested_ael_test")
            .where("$.nested_list.[0].[#-1] == 30")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) == 1


class TestMapKeyOperationsAel:
    """Tests for map key range and key list operations."""

    async def test_map_key_list(self, client_with_map_data):
        """Test $.map.{a,b,c} - get entries by key list."""
        # Get entries for keys alice and bob from scores
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{alice,bob}.count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # Only rec1 has both alice and bob
        assert len(records) == 1

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="$.bin.count() on bare bin emits invalid msgpack (ParameterError at eval time) — server bug, pending fix",
        strict=True,
    )
    async def test_map_key_range(self, client_with_map_data):
        """Test $.map.{a-d} - get entries by key range."""
        # Get entries with keys from 'a' to 'd' (alice, bob, charlie)
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{alice-dave}.count() >= 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # rec1 has alice, bob, charlie (3 in range a-d)
        # rec2 has dave (1 in range - boundary)
        assert len(records) >= 1


@pytest.fixture
async def client_with_relative_range_data(client, enterprise):
    """Setup test data for relative range operations."""
    session = client.create_session()
    ds = DataSet.of("test", "rel_range_test")

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("rec1")).put({
        "numbers": [0, 4, 5, 9, 11, 15],
        "scores": {"alice": 70, "bob": 80, "charlie": 90, "dave": 100},
    }).execute()
    await session.upsert(ds.id("rec2")).put({
        "numbers": [1, 3, 7, 12, 20],
        "scores": {"alice": 60, "bob": 75, "charlie": 85},
    }).execute()
    await session.upsert(ds.id("rec3")).put({
        "numbers": [2, 6, 10, 14, 18],
        "scores": {"alice": 55, "bob": 65, "charlie": 95, "dave": 105},
    }).execute()

    await asyncio.sleep(0.25 if not enterprise else 0.01)

    yield client

    for key in ["rec1", "rec2", "rec3"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestRelativeRangeAel:
    """Tests for relative rank/index range operations."""

    async def test_list_rank_range_relative(self, client_with_relative_range_data):
        """Test $.list.[#rank:end~value] - list value-relative rank range."""
        # Get items with rank 0 to 2 (count=2) relative to value 5
        # For rec1 [0, 4, 5, 9, 11, 15]: value 5 is at index 2, rank 0-2 relative gets [5,9]
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.numbers.[#0:2~5].count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # Just verify it executes without error - relative rank semantics are complex
        assert isinstance(records, list)

    async def test_list_rank_range_relative_no_count(self, client_with_relative_range_data):
        """Test $.list.[#rank:~value] - list value-relative rank range without end count."""
        # Get all items from rank 0 relative to value 5
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.numbers.[#0:~5].count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # Just verify it executes without error
        assert isinstance(records, list)

    async def test_list_rank_range_relative_inverted(self, client_with_relative_range_data):
        """Test $.list.[!#rank:end~value] - inverted list value-relative rank range."""
        # Get items NOT in rank range
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.numbers.[!#0:2~5].count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        # Just verify it executes without error
        assert isinstance(records, list)

    async def test_map_rank_range_relative(self, client_with_relative_range_data):
        """Test $.map.{#rank:end~value} - map value-relative rank range."""
        # Get map entries with rank relative to value 80
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{#-1:1~80}.count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1

    async def test_map_rank_range_relative_no_count(self, client_with_relative_range_data):
        """Test $.map.{#rank:~value} - map value-relative rank range without end count."""
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{#-2:~80}.count() >= 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1

    async def test_map_rank_range_relative_inverted(self, client_with_relative_range_data):
        """Test $.map.{!#rank:end~value} - inverted map value-relative rank range."""
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{!#-1:1~80}.count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1

    async def test_map_index_range_relative(self, client_with_relative_range_data):
        """Test $.map.{start:end~key} - map key-relative index range."""
        # Get map entries at index 0 to 1 relative to key "bob"
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{0:1~bob}.count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1

    async def test_map_index_range_relative_no_count(self, client_with_relative_range_data):
        """Test $.map.{start:~key} - map key-relative index range without end count."""
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{0:~bob}.count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1

    async def test_map_index_range_relative_inverted(self, client_with_relative_range_data):
        """Test $.map.{!start:end~key} - inverted map key-relative index range."""
        stream = await (
            client_with_relative_range_data.query("test", "rel_range_test")
            .where("$.scores.{!0:1~bob}.count() >= 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()

        assert len(records) >= 1


class TestAelErrorHandling:
    """Tests for AEL error handling."""

    async def test_invalid_ael_syntax(self, client_with_cdt_data):
        """Test that invalid AEL raises AelParseException."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("this is not valid AEL !!!")
            .execute()
        )
        with pytest.raises(InvalidRequest, match="ParameterError"):
            async for result in stream:
                pass

    async def test_invalid_list_syntax(self, client_with_cdt_data):
        """Test invalid list syntax raises AelParseException."""
        # [stringValue] is not valid - should be [=stringValue] or ["stringValue"]
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("$.numbers.[invalidSyntax] == 100")
            .execute()
        )
        with pytest.raises(InvalidRequest, match="ParameterError"):
            async for result in stream:
                pass



# =============================================================================
# Advanced expression filter tests (JFC FilterExpTest equivalents)
# =============================================================================

@pytest.fixture
async def filter_session(client, enterprise):
    """Session with test data matching JFC FilterExpTest setUp.

    Key "A": A=1, B=1.1, C="abcde",      D=1, E=-1
    Key "B": A=2, B=2.2, C="abcdeabcde",  D=1, E=-2
    Key "C": A=0, B=-1.0, C="1"
    """
    session = client.create_session()
    ds = DataSet.of("test", "filter_exp_test")

    for key in ["A", "B", "C"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass

    await session.upsert(ds.id("A")).put({"A": 1, "B": 1.1, "C": "abcde", "D": 1, "E": -1}).execute()
    await session.upsert(ds.id("B")).put({"A": 2, "B": 2.2, "C": "abcdeabcde", "D": 1, "E": -2}).execute()
    await session.upsert(ds.id("C")).put({"A": 0, "B": -1.0, "C": "1"}).execute()

    await asyncio.sleep(0.25 if not enterprise else 0.01)

    yield session, ds

    for key in ["A", "B", "C"]:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass


class TestAdvancedExpFilters:
    """Integration tests for advanced expression filter functions.

    Covers arshift, countOneBits, findBitLeft, findBitRight, min, max,
    and when (conditional). Each test verifies both the positive (matching)
    and negative (filtered-out) paths using AEL expressions with
    fail_on_filtered_out().
    """

    async def _assert_filtered_out(self, session, key, ael):
        """Query with AEL filter that should NOT match, expect FILTERED_OUT."""
        from aerospike_async.exceptions import ResultCode
        from aerospike_sdk.exceptions import AerospikeError

        with pytest.raises(AerospikeError) as exc_info:
            rs = await (
                session.query(key)
                    .where(ael)
                    .fail_on_filtered_out()
                    .execute()
            )
            await rs.first_or_raise()
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

    async def _assert_matches(self, session, key, ael, bin_name, expected_value):
        """Query with AEL filter that SHOULD match, validate returned bin."""
        rs = await (
            session.query(key)
                .bins([bin_name])
                .where(ael)
                .fail_on_filtered_out()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.record.bins[bin_name] == expected_value

    async def test_filter_arshift(self, filter_session):
        """Arithmetic right shift: arshift(-2, 62) == -1 for key B."""
        session, ds = filter_session
        key = ds.id("B")
        await self._assert_filtered_out(session, key, "not (($.E >> 62) == -1)")
        await self._assert_matches(session, key, "($.E >> 62) == -1", "E", -2)

    async def test_filter_bit_count(self, filter_session):
        """Bit count (popcount): countOneBits(1) == 1 for key A."""
        session, ds = filter_session
        key = ds.id("A")
        await self._assert_filtered_out(session, key, "not (countOneBits($.A) == 1)")
        await self._assert_matches(session, key, "countOneBits($.A) == 1", "A", 1)

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="findBitLeft() emits invalid msgpack (ParameterError at eval time) — server codegen bug, pending fix",
        strict=True,
    )
    async def test_filter_lscan(self, filter_session):
        """Left scan: findBitLeft($.A, true) == 0 for key A (integer 1, MSB is at position 0)."""
        session, ds = filter_session
        key = ds.id("A")
        await self._assert_filtered_out(session, key, "not (findBitLeft($.A, true) == 0)")
        await self._assert_matches(session, key, "findBitLeft($.A, true) == 0", "A", 1)

    @pytest.mark.xfail(
        condition=server_version_gte("8.1.2"),
        reason="findBitRight() emits invalid msgpack (ParameterError at eval time) — server codegen bug, pending fix",
        strict=True,
    )
    async def test_filter_rscan(self, filter_session):
        """Right scan: findBitRight($.A, true) == 63 for key A (integer 1, LSB at position 0, result = 63 - 0 = 63)."""
        session, ds = filter_session
        key = ds.id("A")
        await self._assert_filtered_out(session, key, "not (findBitRight($.A, true) == 63)")
        await self._assert_matches(session, key, "findBitRight($.A, true) == 63", "A", 1)

    async def test_filter_min(self, filter_session):
        """Min of bins: min(1, 1, -1) == -1 for key A."""
        session, ds = filter_session
        key = ds.id("A")
        await self._assert_filtered_out(session, key, "not (min($.A, $.D, $.E) == -1)")
        await self._assert_matches(session, key, "min($.A, $.D, $.E) == -1", "A", 1)

    async def test_filter_max(self, filter_session):
        """Max of bins: max(1, 1, -1) == 1 for key A."""
        session, ds = filter_session
        key = ds.id("A")
        await self._assert_filtered_out(session, key, "not (max($.A, $.D, $.E) == 1)")
        await self._assert_matches(session, key, "max($.A, $.D, $.E) == 1", "A", 1)

    async def test_filter_cond(self, filter_session):
        """Conditional: when A==1 => D-E == 2 for key A."""
        session, ds = filter_session
        key = ds.id("A")
        when_expr = (
            "when($.A:INT == 0 => $.D:INT + $.E:INT, "
            "$.A:INT == 1 => $.D:INT - $.E:INT, "
            "$.A:INT == 2 => $.D:INT * $.E:INT, "
            "default => -1)"
        )
        cond_ael = f"({when_expr}) == 2"
        await self._assert_filtered_out(session, key, f"not ({cond_ael})")
        await self._assert_matches(session, key, cond_ael, "A", 1)


class TestInExpression:
    """Test the IN operator: expression in expression → boolean.

    Uses the client_with_cdt_data fixture which has:
      rec1: names=["alice","bob","charlie"], numbers=[10,20,30,40,50]
      rec2: names=["dave","eve"], numbers=[5,15,25,35,45]
      rec3: names=["frank"], numbers=[100,200,300]
    """

    async def test_string_in_list_bin_with_exp(self, client_with_cdt_data):
        """Filter: "bob" in $.names — should match rec1 only."""
        from aerospike_async import ListReturnType
        filter_exp = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("bob"),
            Exp.list_bin("names"),
            [],
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filter_exp)
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 1
        assert "bob" in records[0].bins["names"]

    async def test_string_in_list_bin_with_ael(self, client_with_cdt_data):
        """Filter via AEL: "bob" in $.names — should match rec1 only."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where('"bob" in $.names')
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 1
        assert "bob" in records[0].bins["names"]

    async def test_int_in_list_bin_with_ael(self, client_with_cdt_data):
        """Filter via AEL: 20 in $.numbers — matches rec1 only (numbers=[10,20,30,40,50])."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where("20 in $.numbers")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 1
        assert 20 in records[0].bins["numbers"]

    async def test_in_combined_with_and(self, client_with_cdt_data):
        """Filter: "alice" in $.names and $.info.age == 30 — rec1 only."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where('"alice" in $.names and $.info.age == 30')
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 1
        assert records[0].bins["info"]["name"] == "Alice"
        assert records[0].bins["info"]["age"] == 30

    async def test_in_no_match(self, client_with_cdt_data):
        """Filter: "nonexistent" in $.names — should match nothing."""
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .where('"nonexistent" in $.names')
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 0


class TestConvenienceWrappers:
    """Tests for in_list(), map_keys(), map_values() convenience functions."""

    async def test_in_list_string_match(self, client_with_cdt_data):
        """in_list finds "bob" in the names list bin (rec1 only)."""
        filt = Exp.eq(
            in_list(Exp.string_val("bob"), Exp.list_bin("names")),
            Exp.bool_val(True),
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filt)
            .execute()
        )
        records = [r.record async for r in stream]
        stream.close()
        assert len(records) == 1
        assert "bob" in records[0].bins["names"]

    async def test_in_list_int_match(self, client_with_cdt_data):
        """in_list finds 200 in the numbers list bin (rec3 only)."""
        filt = Exp.eq(
            in_list(Exp.int_val(200), Exp.list_bin("numbers")),
            Exp.bool_val(True),
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filt)
            .execute()
        )
        records = [r.record async for r in stream]
        stream.close()
        assert len(records) == 1
        assert 200 in records[0].bins["numbers"]

    async def test_in_list_no_match(self, client_with_cdt_data):
        """in_list returns false when the value is not present."""
        filt = Exp.eq(
            in_list(Exp.string_val("zzz"), Exp.list_bin("names")),
            Exp.bool_val(True),
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filt)
            .execute()
        )
        records = [r.record async for r in stream]
        stream.close()
        assert len(records) == 0

    async def test_map_keys(self, client_with_cdt_data):
        """map_keys returns the key set of the info map."""
        filt = Exp.eq(
            Exp.list_size(map_keys(Exp.map_bin("info")), []),
            Exp.int_val(3),
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filt)
            .execute()
        )
        records = [r.record async for r in stream]
        stream.close()
        assert len(records) == 3

    async def test_map_values(self, client_with_cdt_data):
        """map_values returns values; filter on list size == 3."""
        filt = Exp.eq(
            Exp.list_size(map_values(Exp.map_bin("info")), []),
            Exp.int_val(3),
        )
        stream = await (
            client_with_cdt_data.query("test", "cdt_test")
            .filter_expression(filt)
            .execute()
        )
        records = [r.record async for r in stream]
        stream.close()
        assert len(records) == 3


class TestAelMapBlobIntegrationQueries:
    """Extra map and blob AEL filters exercised against a live server."""

    async def test_map_ael_numeric_field_filters_tier(self, client_with_map_data):
        """Map value filter on ``level`` (``type`` is reserved in the AEL grammar)."""
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.metadata.level != 1")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 2
        for rec in records:
            assert rec.bins["metadata"]["level"] in (2, 3)

    async def test_map_ael_key_list_count_on_server(self, client_with_map_data):
        """Map key list slice: ``$.scores.{alice,bob}``."""
        stream = await (
            client_with_map_data.query("test", "map_ael_test")
            .where("$.scores.{alice,bob}.count() == 2")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record)
        stream.close()
        assert len(records) == 1
        assert "alice" in records[0].bins["scores"]
        assert "bob" in records[0].bins["scores"]

    async def test_blob_bin_ael_equality_on_server(
            self,
            aerospike_host,
            client_policy,
            enterprise,
    ):
        """BLOB bin filter using a hex blob literal in AEL."""
        async with Client(seeds=aerospike_host, policy=client_policy) as client:
            session = client.create_session()
            k = DataSet.of("test", "ael_blob_srv_it").id("blob_row")
            payload = bytes([1, 2, 254])
            try:
                await session.delete(k).execute()
            except Exception:
                pass

            await session.upsert(k).put({"payload": payload}).execute()
            await asyncio.sleep(0.25 if not enterprise else 0.01)

            hex_str = payload.hex()  # '0102fe'
            stream = await (
                session.query("test", "ael_blob_srv_it")
                .where(f"$.payload:BLOB == X'{hex_str}'")
                .execute()
            )
            rows = [r.record async for r in stream]
            stream.close()
            assert len(rows) == 1
            assert rows[0].bins["payload"] == payload

            await session.delete(k).execute()
