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

"""Integration tests for query Filter with CDT context (nested map path).

:meth:`~aerospike_sdk.aio.client.Client.query` /
:class:`~aerospike_sdk.aio.operations.query.QueryBuilder` accept native
``Filter`` objects, including ``Filter.equal(...).context([...])``. Secondary
indexes on CDT paths are created here via :meth:`Client.create_index` because
the SDK :class:`~aerospike_sdk.aio.operations.index.IndexBuilder` does
not expose ``ctx`` yet.
"""

import pytest
from aerospike_async import CTX, Filter, IndexType

from aerospike_sdk import DataSet, Client

_NS = "test"
_SET = "cdt_filter_ctx_test"
_INDEX = "pfc_cdt_fctx_map_num"
_BIN = "mapbin"
_OUTER = "outer"
_INNER = "inner"


def _require_filter_context() -> None:
    """Skip when PAC lacks :meth:`~Filter.context` (CDT path on secondary-index filters)."""
    probe = Filter.equal("__bin", 1)
    if not hasattr(probe, "context"):
        pytest.skip(
            "aerospike_async Filter.context is required; upgrade the native async client."
        )


async def _cleanup_records(session, keys):
    for k in keys:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


def _user_keys_from_stream(results):
    keys = []
    for res in results:
        if res.is_ok and res.record is not None:
            keys.append(res.record.key.value)
    return keys


async def test_query_filter_equal_with_map_nested_context(client, enterprise, wait_for_index):
    """Query with ``Filter.equal(...).context([...])`` on a nested map value (indexed path).

    Records store ``mapbin`` as ``{outer: {inner: <int>, ...}}``. A numeric index on
    ``mapbin`` with context ``[CTX.map_key(outer), CTX.map_key(inner)]`` targets that
    nested integer. The query filter uses the same path so only matching keys return.
    """
    _require_filter_context()

    ds = DataSet.of(_NS, _SET)
    key_hi = ds.id("cdt_ctx_hi")
    key_lo = ds.id("cdt_ctx_lo")
    key_missing_inner = ds.id("cdt_ctx_no_inner")
    keys = (key_hi, key_lo, key_missing_inner)

    session = client.create_session()
    pac = client.underlying_client

    await _cleanup_records(session, keys)
    try:
        await pac.drop_index(_NS, _SET, _INDEX)
    except Exception:
        pass

    target = 4242
    other_inner = 7

    await (
        session.upsert(key_hi)
        .put({_BIN: {_OUTER: {_INNER: target, "noise": other_inner}}})
        .execute()
    )
    await (
        session.upsert(key_lo)
        .put({_BIN: {_OUTER: {_INNER: 9999, "noise": 1}}})
        .execute()
    )
    await (
        session.upsert(key_missing_inner)
        .put({_BIN: {_OUTER: {"noise": 3}}})
        .execute()
    )

    try:
        await pac.create_index(
            _NS,
            _SET,
            _BIN,
            _INDEX,
            IndexType.NUMERIC,
            None,
            ctx=[CTX.map_key(_OUTER), CTX.map_key(_INNER)],
        )
    except Exception as e:
        pytest.skip(f"Could not create nested-map secondary index: {e}")

    flt = Filter.equal(_BIN, target).context(
        [CTX.map_key(_OUTER), CTX.map_key(_INNER)]
    )
    await wait_for_index(client, _NS, _SET, flt)

    try:
        stream = await client.query(_NS, _SET).filter(flt).bins([_BIN]).execute()
        found = []
        try:
            async for res in stream:
                found.append(res)
        finally:
            stream.close()

        user_keys = sorted(_user_keys_from_stream(found))
        assert user_keys == ["cdt_ctx_hi"]

        flt2 = Filter.equal(_BIN, 9999).context(
            [CTX.map_key(_OUTER), CTX.map_key(_INNER)]
        )
        stream2 = await client.query(_NS, _SET).filter(flt2).bins([_BIN]).execute()
        found2 = []
        try:
            async for res in stream2:
                found2.append(res)
        finally:
            stream2.close()

        assert sorted(_user_keys_from_stream(found2)) == ["cdt_ctx_lo"]
    finally:
        try:
            await pac.drop_index(_NS, _SET, _INDEX)
        except Exception:
            pass
        await _cleanup_records(session, keys)


async def test_query_filter_equal_single_map_key_context(client, enterprise, wait_for_index):
    """``Filter.equal(bin, value).context([CTX.map_key(...)])`` on a scalar under one map key."""
    _require_filter_context()

    ds = DataSet.of(_NS, _SET)
    key_match = ds.id("cdt_ctx_flat_a")
    key_other = ds.id("cdt_ctx_flat_b")
    keys = (key_match, key_other)

    session = client.create_session()
    pac = client.underlying_client
    index_name = f"{_INDEX}_flat"
    val = 5150

    await _cleanup_records(session, keys)
    try:
        await pac.drop_index(_NS, _SET, index_name)
    except Exception:
        pass

    await (
        session.upsert(key_match)
        .put({_BIN: {_INNER: val, "other": 1}})
        .execute()
    )
    await (
        session.upsert(key_other)
        .put({_BIN: {_INNER: val + 1, "other": 2}})
        .execute()
    )

    try:
        await pac.create_index(
            _NS,
            _SET,
            _BIN,
            index_name,
            IndexType.NUMERIC,
            None,
            ctx=[CTX.map_key(_INNER)],
        )
    except Exception as e:
        pytest.skip(f"Could not create CDT-path numeric index: {e}")

    flt = Filter.equal(_BIN, val).context([CTX.map_key(_INNER)])
    await wait_for_index(client, _NS, _SET, flt)

    try:
        stream = await client.query(_NS, _SET).filter(flt).bins([_BIN]).execute()
        found = []
        try:
            async for res in stream:
                found.append(res)
        finally:
            stream.close()

        assert sorted(_user_keys_from_stream(found)) == ["cdt_ctx_flat_a"]
    finally:
        try:
            await pac.drop_index(_NS, _SET, index_name)
        except Exception:
            pass
        await _cleanup_records(session, keys)
