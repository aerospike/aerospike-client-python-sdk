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

import pytest
import pytest_asyncio

from aerospike_sdk import (
    CTX,
    Exp,
    StringOperation,
    StringWriteFlags,
)
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError
from tests.integration.namespace import general_namespace


_TEST_DS = DataSet.of(general_namespace(), "test")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, supports_string_operations, make_cluster_definition):
    """Module-scoped cluster for server-side string ops (server >= 8.1.3).

    Single-host model: connects to the default ``AEROSPIKE_HOST`` and skips
    cleanly via ``supports_string_operations`` unless it is 8.1.3+. Point
    ``AEROSPIKE_HOST`` at an 8.1.3+ build to run these; CI covers the version
    spread via a server matrix.
    """
    if not supports_string_operations:
        pytest.skip(
            "string operations require server >= 8.1.3; point AEROSPIKE_HOST "
            "at an 8.1.3+ build to run these"
        )
    async with await make_cluster_definition(aerospike_host).connect() as c:
        await asyncio.sleep(2)
        sess = c.create_session()
        for suffix in (
            "reads", "modify", "append_ops", "exp_query",
            "transform_noop_missing", "create_from_missing",
            "concat_flag", "list_ctx", "map_ctx",
        ):
            await sess.delete(_TEST_DS.id(f"strop_{suffix}")).execute()
        yield c


# ---------------------------------------------------------------------------
# Smoke — basic chainable + low-level + Exp paths
# ---------------------------------------------------------------------------

async def test_str_reads_via_builder(cluster):
    """Chained string reads via ``WriteBinBuilder.str_*`` — single multi-op call.

    Multiple ops targeting the same bin return positional results as a list
    on ``record.bins[bin]`` (PAC's ``Value::MultiResult``).
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_reads")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    rs = await (sess.query(k)
        .bin("s").str_strlen()
        .bin("s").str_substr(1, 4)
        .bin("s").str_find("ll")
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [5, "ell", 2]


async def test_str_modify_and_read(cluster):
    """``str_upper`` chained with ``get`` in a single execute. Asserts both:

    * ``bins["s"] == "AB"`` (by-name access shows the trailing read result)
    * ``results == [None, "AB"]`` (positional access shows the modify op
      contributing ``None`` at op-index 0 and the read returning ``"AB"`` at
      op-index 1 — the server returns nil for STRING_MODIFY ops on the wire,
      which the positional accessor surfaces faithfully)
    """
    sess = cluster.create_session()
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


async def test_str_append_and_prepend(cluster):
    """``str_append`` (sub-op 67) adds to the end; ``str_prepend`` (sub-op 68)
    adds to the start — distinct from ``str_concat`` (list form) and
    ``str_insert(0, …)``."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_append_prepend")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    result = await (await sess.upsert(k)
        .bin("s").str_append(" world")
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "hello world"

    result = await (await sess.upsert(k)
        .bin("s").str_prepend("oh ")
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "oh hello world"


async def test_str_reads_via_add_operation(cluster):
    """Low-level ``StringOperation`` factories via chained ``add_operation``."""
    sess = cluster.create_session()
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


async def test_str_projection_via_exp_on_query(cluster):
    """Query projection using ``Exp.string_*`` filter expressions."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_query")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    rs = await sess.query(k) \
        .bin("slen").select_from(Exp.string_strlen(Exp.string_bin("s"))) \
        .bin("sfind").select_from(Exp.string_find(Exp.val("ll"), Exp.string_bin("s"))) \
        .execute()
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["slen"] == 5
    assert rec.bins["sfind"] == 2


async def test_to_string_projection_via_exp(cluster):
    """``Exp.to_string`` coerces any type to its string representation.

    Exercises the dedicated TO_STRING expression opcode (server 8.1.3+) through
    the renamed ``Exp.to_string`` surface (was ``string_to_string``).
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_to_string_exp")
    await sess.upsert(k).bin("n").set_to(42).execute()

    rs = await sess.query(k) \
        .bin("as_str").select_from(Exp.to_string(Exp.int_bin("n"))) \
        .execute()
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["as_str"] == "42"


async def test_str_to_string_op_via_query(cluster):
    """Fluent ``str_to_string`` op coerces the bin value to its string form."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_str_to_string")
    await sess.upsert(k).bin("n").set_to(42).execute()

    rs = await sess.query(k).bin("n").str_to_string().execute()
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["n"] == "42"


async def _survives_filter(sess, key, filter_exp) -> bool:
    """Whether *key* survives *filter_exp* (evaluated server-side)."""
    rs = await sess.query(key).filter_expression(filter_exp).execute()
    return len([r async for r in rs]) == 1


async def test_string_to_integer_evaluates_in_filter(cluster):
    """``Exp.string_to_integer`` parses a string bin to an int server-side.

    Sibling of ``to_string`` but on the ``CALL_STRING`` path (not
    ``CALL_REPR``); a positive match plus a negative control prove the
    coercion actually evaluated rather than being ignored (no ParameterError).
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_to_int_exp")
    await sess.upsert(k).bin("snum").set_to("123").execute()

    coerced = Exp.string_to_integer(Exp.string_bin("snum"))
    assert await _survives_filter(sess, k, Exp.eq(coerced, Exp.val(123)))
    assert not await _survives_filter(sess, k, Exp.eq(coerced, Exp.val(999)))


async def test_string_to_double_evaluates_in_filter(cluster):
    """``Exp.string_to_double`` parses a string bin to a double server-side."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_to_double_exp")
    await sess.upsert(k).bin("sf").set_to("1.5").execute()

    coerced = Exp.string_to_double(Exp.string_bin("sf"))
    assert await _survives_filter(sess, k, Exp.eq(coerced, Exp.val(1.5)))
    assert not await _survives_filter(sess, k, Exp.eq(coerced, Exp.val(9.9)))


# ---------------------------------------------------------------------------
# Spot tests — flag paths
# ---------------------------------------------------------------------------

async def test_str_upper_silently_noops_on_missing_bin(cluster):
    """Transform / subtractive ops on a missing bin: silent no-op.

    Per the string-ops spec (§4.1, server 8.1.3+), the missing-bin path is
    op-class-dependent, not flag-dependent. ``str_upper`` is a transform op:
    on a missing bin it succeeds, does not create the bin, and leaves
    siblings untouched. Behavior is independent of the NO_FAIL flag
    (NO_FAIL now only suppresses in-op execution failures, not missing-bin).
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_transform_noop_missing")
    await sess.delete(k).execute()
    await sess.upsert(k).bin("other").set_to("x").execute()

    await sess.upsert(k).bin("missing_bin").str_upper().execute()

    rs = await sess.query(k).execute()
    bins = (await rs.first_or_raise()).record_or_raise().bins
    assert bins["other"] == "x"
    assert "missing_bin" not in bins


async def test_str_insert_creates_bin_from_empty_on_missing_bin(cluster):
    """Additive / create ops on a missing bin: bin is created from empty.

    Per the string-ops spec (§4.1, server 8.1.3+), the eight additive
    create-ops {INSERT, OVERWRITE, CONCAT, APPEND, PREPEND, PAD_START,
    PAD_END, REPEAT} treat the absent bin as the empty string ``""``,
    apply themselves, and create the bin with the result.
    ``str_insert(0, "hello")`` on a missing bin therefore creates the
    bin holding ``"hello"``.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_create_from_missing")
    await sess.delete(k).execute()
    await sess.upsert(k).bin("other").set_to("x").execute()

    await sess.upsert(k).bin("missing_bin").str_insert(0, "hello").execute()

    rs = await sess.query(k).execute()
    bins = (await rs.first_or_raise()).record_or_raise().bins
    assert bins["missing_bin"] == "hello"
    assert bins["other"] == "x"


async def test_str_concat_with_flag(cluster):
    """``str_concat`` accepts a flags kwarg; default flags produce simple appending."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_concat_flag")
    await sess.upsert(k).bin("s").set_to("foo").execute()

    await sess.upsert(k).bin("s").str_concat("bar", flags=StringWriteFlags.DEFAULT).execute()

    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "foobar"


# ---------------------------------------------------------------------------
# Spot tests — CTX paths (chainable on_list_index / on_map_key not yet added;
# users drop to low-level StringOperation with ctx=[...] for nested ops)
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    raises=AerospikeError,
    reason=(
        "String-op CTX envelope changed from flat to nested server-side. The "
        "server now expects [0xFF, ctx_list, [inner_op, args...]] with a fixed "
        "outer length of 3; the core client still emits the flat "
        "[0xFF, ctx_flat_list, inner_op, args...] and is rejected with "
        "PARAMETER_ERROR. Passes on server builds without the change (verified "
        "on 8.1.3.0-75), fails on the 8.1.3.0 RC. Tracked as CLIENT-5329, "
        "blocked on the server change; promote back to a plain test once the "
        "core client emits the nested envelope."
    ),
)
async def test_str_upper_with_list_ctx(cluster):
    """``StringOperation.upper`` with a ``ctx=[CTX.list_index(...)]`` upper-cases one list element."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_list_ctx")
    await sess.upsert(k).bin("lst").set_to(["one", "two", "three"]).execute()

    await sess.upsert(k) \
        .add_operation(StringOperation.upper("lst", ctx=[CTX.list_index(1)])) \
        .execute()

    rs = await sess.query(k).bin("lst").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["lst"] == ["one", "TWO", "three"]


@pytest.mark.xfail(
    raises=AerospikeError,
    reason=(
        "String-op CTX envelope changed from flat to nested server-side. The "
        "server now expects [0xFF, ctx_list, [inner_op, args...]] with a fixed "
        "outer length of 3; the core client still emits the flat "
        "[0xFF, ctx_flat_list, inner_op, args...] and is rejected with "
        "PARAMETER_ERROR. Passes on server builds without the change (verified "
        "on 8.1.3.0-75), fails on the 8.1.3.0 RC. Tracked as CLIENT-5329, "
        "blocked on the server change; promote back to a plain test once the "
        "core client emits the nested envelope."
    ),
)
async def test_str_strlen_with_map_ctx(cluster):
    """``StringOperation.strlen`` with ``ctx=[CTX.map_key(...)]`` measures one map value."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_map_ctx")
    await sess.upsert(k).bin("m").set_to({"k1": "abcd", "k2": "xyz"}).execute()

    rs = await sess.upsert(k) \
        .add_operation(StringOperation.strlen("m", ctx=[CTX.map_key("k1")])) \
        .execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["m"] == 4


# ---------------------------------------------------------------------------
# Self-overlapping needle tests
#
# The spec (CLIENTS / "String Operations", §4.1 find / §4.2 replaceAll)
# defines the contract as **overlap-skip**: after matching at index N the
# scan resumes from N + len(needle), NOT from N + 1. A self-overlapping
# needle (prefix == suffix, e.g. "aa", "👋👋") never matches at a position
# inside a prior match. The spec's canonical examples — `find("aaaa","aa",2)
# → 2` and `replaceAll("aaaa","aa","X") → "XX"` — are pinned below.
# ---------------------------------------------------------------------------

async def test_str_find_nth_overlap_skip(cluster):
    """``find_nth("aa")`` over ``"aaaaa"`` returns positions 0, 2 only.

    Overlap-skip scan per spec §4.1: after matching "aa" at index 0 the scan
    advances by needle length (2) to index 2, matches again, advances to
    index 4 where only one 'a' remains and no further match is possible.
    Two matches total; occurrences 3+ return -1. (A naive overlap-aware
    advance-by-one impl would return 1 for the 2nd occurrence — explicitly
    called out as a wrong-cluster behavior in the spec.)
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_find_nth_overlap")
    await sess.upsert(k).bin("s").set_to("aaaaa").execute()

    rs = await (sess.query(k)
        .bin("s").str_find("aa")              # 1st (default occurrence)
        .bin("s").str_find("aa", occurrence=2)
        .bin("s").str_find("aa", occurrence=3)  # absent → -1
        .bin("s").str_find("aa", occurrence=4)  # absent → -1
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [0, 2, -1, -1]


async def test_str_find_nth_overlap_skip_longer(cluster):
    """``find_nth("abab")`` over ``"abababab"`` returns positions 0, 4 only.

    Overlap-skip per spec §4.1: match at 0, advance by needle length 4,
    match at 4, advance to end of string. Two matches. An overlap-aware
    impl would yield three matches at 0, 2, 4.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_find_nth_longer")
    await sess.upsert(k).bin("s").set_to("abababab").execute()

    rs = await (sess.query(k)
        .bin("s").str_find("abab")
        .bin("s").str_find("abab", occurrence=2)
        .bin("s").str_find("abab", occurrence=3)  # absent → -1
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [0, 4, -1]


async def test_str_replace_all_overlapping_needle(cluster):
    """``replace_all("aa", "X")`` over ``"aaaa"`` yields ``"XX"`` — the spec's
    canonical overlap-skip example (§4.2): ``"XX"``, NOT ``"XaX"``.

    Matches at index 0 and 2 (both pairs disjoint under overlap-skip), each
    replaced. Original needles/replacements widened to ``"b"`` and a longer
    replacement in companion tests to surface advance-direction bugs.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_replace_overlap")
    await sess.upsert(k).bin("s").set_to("aaaa").execute()

    await sess.upsert(k).bin("s").str_replace_all("aa", "b").execute()

    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "bb"


async def test_str_replace_all_overlap_with_longer_replacement(cluster):
    """``replace_all("aa", "xxx")`` over ``"aaaa"`` yields ``"xxxxxx"``.

    Two non-overlapping "aa" matches in "aaaa" (positions 0 and 2) each get
    replaced by "xxx" → "xxx" + "xxx" = "xxxxxx" (6 chars). The scan does
    NOT rescan into emitted replacement output, so the result is exactly
    two "xxx" segments concatenated.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_replace_overlap_long")
    await sess.upsert(k).bin("s").set_to("aaaa").execute()

    await sess.upsert(k).bin("s").str_replace_all("aa", "xxx").execute()

    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "xxxxxx"


async def test_str_find_needle_equals_haystack(cluster):
    """``find("aaa")`` over ``"aaa"`` returns 0; ``find_nth`` 2nd occurrence returns -1."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_find_equal")
    await sess.upsert(k).bin("s").set_to("aaa").execute()

    rs = await (sess.query(k)
        .bin("s").str_find("aaa")
        .bin("s").str_find("aaa", occurrence=2)
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [0, -1]


async def test_str_contains_overlapping_pattern(cluster):
    """``contains`` is overlap-agnostic — returns True whenever ANY match exists.

    Sanity guard that contains/starts_with/ends_with behave correctly on
    haystacks where the only matches are overlapping ones.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_contains_overlap")
    await sess.upsert(k).bin("s").set_to("aaaa").execute()

    rs = await (sess.query(k)
        .bin("s").str_contains("aa")
        .bin("s").str_starts_with("aa")
        .bin("s").str_ends_with("aa")
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == [True, True, True]


async def test_str_find_overlap_skip_spec_canonical(cluster):
    """Spec §4.1 canonical example: ``find("aaaa", "aa", 2) → 2``.

    Pinned verbatim from the spec ASCII-path verification — overlap-skip
    means the 2nd occurrence of "aa" in "aaaa" starts at index 2, not 1.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_find_overlap_canonical")
    await sess.upsert(k).bin("s").set_to("aaaa").execute()

    rs = await (sess.query(k)
        .bin("s").str_find("aa", occurrence=2)
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == 2


async def test_str_find_overlap_skip_icu_path_emoji(cluster):
    """Spec §4.1 ICU-path canonical: ``find("👋👋👋👋", "👋👋", 2) → 2``.

    Same overlap-skip contract on the ICU ``usearch`` code path. Indexes are
    codepoint indexes (per the spec's indexing note), so "👋👋" of length 2
    advances by 2 codepoints — the 2nd occurrence sits at codepoint
    index 2, not 1.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_find_overlap_icu_emoji")
    await sess.upsert(k).bin("s").set_to("👋👋👋👋").execute()

    rs = await (sess.query(k)
        .bin("s").str_find("👋👋", occurrence=2)
        .execute())
    rec = (await rs.first_or_raise()).record_or_raise()
    assert rec.bins["s"] == 2
