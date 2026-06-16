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

"""Integration tests for server string operations (8.1.3+)."""

import asyncio
import os

import pytest
import pytest_asyncio

from aerospike_async import AuthMode, ClientPolicy

from aerospike_sdk import (
    Client,
    CTX,
    Exp,
    StringOperation,
    StringWriteFlags,
)
from aerospike_sdk.dataset import DataSet


_TEST_DS = DataSet.of("test", "test")


def _build_813_policy() -> ClientPolicy:
    """Build a ClientPolicy for the 8.1.3+ seed honoring per-host overrides.

    The 8.1.3+ seed may sit at a different network topology from the rest
    of the test rig (e.g. external bench host at a public IP vs containerized
    localhost), so this fixture honors a per-host services-alternate override
    and per-host auth env vars when present.
    """
    policy = ClientPolicy()
    sa_override = os.environ.get('AEROSPIKE_HOST_8_1_3_USE_SERVICES_ALTERNATE', '').strip().lower()
    if sa_override in ('true', '1', 'yes', 'false', '0', 'no'):
        policy.use_services_alternate = sa_override in ('true', '1', 'yes')
    else:
        policy.use_services_alternate = os.environ.get(
            'AEROSPIKE_USE_SERVICES_ALTERNATE', 'true'
        ).strip().lower() in ('true', '1', 'yes')
    user = os.environ.get('AEROSPIKE_HOST_8_1_3_USER', '')
    password = os.environ.get('AEROSPIKE_HOST_8_1_3_PASSWORD', '')
    if user and password:
        policy.set_auth_mode(AuthMode.INTERNAL, user=user, password=password)
    return policy


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host_8_1_3):
    if not aerospike_host_8_1_3:
        pytest.skip(
            "AEROSPIKE_HOST_8_1_3 is unset; this suite requires an 8.1.3+ "
            "cluster. Set AEROSPIKE_HOST_8_1_3 in aerospike.env to enable."
        )
    async with Client(seeds=aerospike_host_8_1_3, policy=_build_813_policy()) as c:
        await asyncio.sleep(2)
        sess = c.create_session()
        for suffix in (
            "reads", "modify", "append_ops", "exp_query",
            "no_fail", "concat_flag", "list_ctx", "map_ctx",
        ):
            await sess.delete(_TEST_DS.id(f"strop_{suffix}")).execute()
        yield c


# ---------------------------------------------------------------------------
# Smoke — basic chainable + low-level + Exp paths
# ---------------------------------------------------------------------------

async def test_str_reads_via_builder(client):
    """Chained string reads via ``WriteBinBuilder.str_*`` — single multi-op call.

    Multiple ops targeting the same bin return positional results as a list
    on ``record.bins[bin]`` (PAC's ``Value::MultiResult``).
    """
    sess = client.create_session()
    k = _TEST_DS.id("strop_reads")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    rs = await (sess.query(k)
                .bin("s").str_strlen()
                .bin("s").str_substr(1, 4)
                .bin("s").str_find("ll")
                .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [5, "ell", 2]


async def test_str_modify_and_read(client):
    """``str_upper`` chained with ``get`` in a single execute. Asserts both:

    * ``bins["s"] == "AB"`` (by-name access shows the trailing read result)
    * ``results == [None, "AB"]`` (positional access shows the modify op
      contributing ``None`` at op-index 0 and the read returning ``"AB"`` at
      op-index 1 — the server returns nil for STRING_MODIFY ops on the wire,
      which the positional accessor surfaces faithfully)
    """
    sess = client.create_session()
    k = _TEST_DS.id("strop_modify")
    await sess.upsert(k).bin("s").set_to("ab").execute()

    result = await (await sess.upsert(k)
                    .bin("s").str_upper()
                    .bin("s").get()
                    .execute()).first_or_raise()
    rec = result.record_or_raise()
    assert rec.bins["s"] == "AB"
    assert rec.results == [None, "AB"]
    assert result.operation_result(0) is None
    assert result.operation_result(1) == "AB"


async def test_str_reads_via_add_operation(client):
    """Low-level ``StringOperation`` factories via chained ``add_operation``."""
    sess = client.create_session()
    k = _TEST_DS.id("strop_append_ops")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    rs = await (sess.query(k)
                .add_operation(StringOperation.strlen("s"))
                .add_operation(StringOperation.substr("s", 1, 4))
                .add_operation(StringOperation.substr("s", 3))
                .add_operation(StringOperation.find("s", "ll"))
                .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [5, "ell", "lo", 2]


async def test_str_projection_via_exp_on_query(client):
    """Query projection using ``Exp.string_*`` filter expressions."""
    sess = client.create_session()
    k = _TEST_DS.id("strop_exp_query")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    rs = await sess.query(k) \
        .bin("slen").select_from(Exp.string_strlen(Exp.string_bin("s"))) \
        .bin("sfind").select_from(Exp.string_find(Exp.val("ll"), Exp.string_bin("s"))) \
        .execute()
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["slen"] == 5
    assert rec.bins["sfind"] == 2


# ---------------------------------------------------------------------------
# Spot tests — flag paths
# ---------------------------------------------------------------------------

async def test_str_upper_with_no_fail_flag(client):
    """``StringWriteFlags.NO_FAIL`` suppresses missing-bin errors (not type-mismatch).

    Record exists with a sibling bin; the target bin does not. Without NO_FAIL
    the server returns BIN_NOT_FOUND; with NO_FAIL the op is a no-op success.
    """
    sess = client.create_session()
    k = _TEST_DS.id("strop_no_fail")
    await sess.upsert(k).bin("other").set_to("x").execute()

    await sess.upsert(k) \
        .bin("missing_bin").str_upper(flags=StringWriteFlags.NO_FAIL) \
        .execute()

    rs = await sess.query(k).bin("other").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["other"] == "x"


async def test_str_concat_with_flag(client):
    """``str_concat`` accepts a flags kwarg; default flags produce simple appending."""
    sess = client.create_session()
    k = _TEST_DS.id("strop_concat_flag")
    await sess.upsert(k).bin("s").set_to("foo").execute()

    await sess.upsert(k).bin("s").str_concat("bar", flags=StringWriteFlags.DEFAULT).execute()

    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "foobar"


# ---------------------------------------------------------------------------
# Spot tests — CTX paths (chainable on_list_index / on_map_key not yet added;
# users drop to low-level StringOperation with ctx=[...] for nested ops)
# ---------------------------------------------------------------------------

async def test_str_upper_with_list_ctx(client):
    """``StringOperation.upper`` with a ``ctx=[CTX.list_index(...)]`` upper-cases one list element."""
    sess = client.create_session()
    k = _TEST_DS.id("strop_list_ctx")
    await sess.upsert(k).bin("lst").set_to(["one", "two", "three"]).execute()

    await sess.upsert(k) \
        .add_operation(StringOperation.upper("lst", ctx=[CTX.list_index(1)])) \
        .execute()

    rs = await sess.query(k).bin("lst").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["lst"] == ["one", "TWO", "three"]


async def test_str_strlen_with_map_ctx(client):
    """``StringOperation.strlen`` with ``ctx=[CTX.map_key(...)]`` measures one map value."""
    sess = client.create_session()
    k = _TEST_DS.id("strop_map_ctx")
    await sess.upsert(k).bin("m").set_to({"k1": "abcd", "k2": "xyz"}).execute()

    rs = await sess.upsert(k) \
        .add_operation(StringOperation.strlen("m", ctx=[CTX.map_key("k1")])) \
        .execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["m"] == 4
