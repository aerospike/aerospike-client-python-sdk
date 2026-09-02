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
    StringNumericType,
    StringOperation,
    StringRegexFlags,
    StringWriteFlags,
)
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
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
            "reads", "modify", "snip", "append_ops", "exp_query",
            "transform_noop_missing", "create_from_missing",
            "concat_flag", "list_ctx", "map_ctx",
            "norm_replace", "norm_affix", "result_cap",
            "exp_read_sweep", "exp_modify_sweep", "exp_trim_sweep",
            "exp_regex_replace", "exp_convert_sweep",
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


async def test_str_snip_range_and_truncate_to_end(cluster):
    """``str_snip`` removes ``[start, end)``; without ``end`` it truncates
    from ``start`` through the end of the string.

    A wrong truncate result would be silent: a mispacked 2-element
    ``[start, flags]`` payload is accepted by the server as ``[start, end]``
    and no-ops, so the bin coming back shortened is the whole assertion.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_snip")

    # Range form [start, end).
    await sess.upsert(k).bin("s").set_to("abcdef").execute()
    result = await (await sess.upsert(k)
        .bin("s").str_snip(2, 4)
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "abef"

    # 1-arg form: truncate from start through the end.
    await sess.upsert(k).bin("s").set_to("hello world").execute()
    result = await (await sess.upsert(k)
        .bin("s").str_snip(5)
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "hello"

    # Negative start counts from the end of the string.
    await sess.upsert(k).bin("s").set_to("hello world").execute()
    result = await (await sess.upsert(k)
        .bin("s").str_snip(-6)
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "hello"

    # Same two forms through the low-level StringOperation factory.
    await sess.upsert(k).bin("s").set_to("Hello").execute()
    result = await (await sess.upsert(k)
        .add_operation(StringOperation.snip("s", 1, 4))
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "Ho"

    await sess.upsert(k).bin("s").set_to("hello world").execute()
    result = await (await sess.upsert(k)
        .add_operation(StringOperation.snip("s", 5))
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "hello"


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


async def test_str_create_only_and_update_only_flags(cluster):
    """``CREATE_ONLY`` gates on bin absence, ``UPDATE_ONLY`` on bin presence.

    ``CREATE_ONLY`` on a live bin raises ``BIN_EXISTS_ERROR`` unless paired
    with ``NO_FAIL``, which turns it into a silent no-op; ``UPDATE_ONLY`` on
    a missing bin is a no-op rather than a create.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_write_flags")
    await sess.delete(k).execute()
    await sess.upsert(k).bin("other").set_to("x").execute()

    # CREATE_ONLY on a missing bin creates it.
    await (sess.upsert(k)
        .bin("s").str_insert(0, "new", flags=StringWriteFlags.CREATE_ONLY)
        .execute())
    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "new"

    # CREATE_ONLY on the now-live bin raises BIN_EXISTS_ERROR.
    with pytest.raises(AerospikeError) as exc_info:
        await (sess.upsert(k)
            .bin("s").str_insert(0, "x", flags=StringWriteFlags.CREATE_ONLY)
            .execute())
    assert exc_info.value.result_code == ResultCode.BIN_EXISTS_ERROR

    # ...unless NO_FAIL is set: silent no-op, bin unchanged.
    await (sess.upsert(k)
        .bin("s").str_insert(
            0, "x", flags=StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL)
        .execute())
    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "new"

    # UPDATE_ONLY on a missing bin does not create it.
    await (sess.upsert(k)
        .bin("absent").str_insert(0, "x", flags=StringWriteFlags.UPDATE_ONLY)
        .execute())
    rs = await sess.query(k).execute()
    assert "absent" not in (await rs.first_or_raise()).record_or_raise().bins

    # The two flags together are rejected by the server.
    with pytest.raises(AerospikeError) as exc_info:
        await (sess.upsert(k)
            .bin("s").str_insert(
                0, "x", flags=StringWriteFlags.CREATE_ONLY | StringWriteFlags.UPDATE_ONLY)
            .execute())
    assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR


async def test_str_create_only_rejected_with_ctx(cluster):
    """``CREATE_ONLY`` never combines with a CTX path — server ``PARAMETER_ERROR``."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_write_flags_ctx")
    await sess.upsert(k).bin("lst").set_to(["a", "b"]).execute()

    with pytest.raises(AerospikeError) as exc_info:
        await (sess.upsert(k)
            .add_operation(StringOperation.insert(
                "lst", 0, "x",
                flags=int(StringWriteFlags.CREATE_ONLY),
                ctx=[CTX.list_index(1)]))
            .execute())
    assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR


async def test_str_no_fail_decides_outcome_on_unreachable_ctx_path(cluster):
    """An out-of-range CTX path is an in-op execution failure, so ``NO_FAIL``
    flips it from ``OP_NOT_APPLICABLE`` to a silent no-op."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_nofail_ctx")
    await sess.upsert(k).bin("lst").set_to(["alpha", "beta"]).execute()

    await (sess.upsert(k)
        .add_operation(StringOperation.append(
            "lst", "!",
            flags=int(StringWriteFlags.NO_FAIL),
            ctx=[CTX.list_index(99)]))
        .execute())
    rs = await sess.query(k).bin("lst").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["lst"] == ["alpha", "beta"]

    with pytest.raises(AerospikeError) as exc_info:
        await (sess.upsert(k)
            .add_operation(StringOperation.append("lst", "!", ctx=[CTX.list_index(99)]))
            .execute())
    assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


# ---------------------------------------------------------------------------
# Spot tests — CTX paths (chainable on_list_index / on_map_key not yet added;
# users drop to low-level StringOperation with ctx=[...] for nested ops)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Unicode canonical equivalence — replace and affix matching
# ---------------------------------------------------------------------------

_NFC = "caf\u00e9"     # composed
_NFD = "cafe\u0301"    # decomposed


async def test_str_replace_matches_across_normalization_forms(cluster):
    """``str_replace`` treats canonically equivalent needles as equal.

    Replace carries the same canonical-equivalence guarantee as find and
    contains: the server routes replace through its canonical search
    (``get_canon_search`` in ``particle_string.c``) whenever the forms
    differ, so a needle in one normalization form matches a stored value
    in the other.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_norm_replace")

    # Composed haystack, decomposed needle.
    await sess.upsert(k).bin("s").set_to(_NFC + " au lait").execute()
    result = await (await sess.upsert(k)
        .bin("s").str_replace(_NFD, "tea")
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "tea au lait"

    # Decomposed haystack, composed needle.
    await sess.upsert(k).bin("s").set_to(_NFD + " au lait").execute()
    result = await (await sess.upsert(k)
        .bin("s").str_replace(_NFC, "tea")
        .bin("s").get()
        .execute()).first_or_raise()
    assert result.record_or_raise().bins["s"] == "tea au lait"


async def test_str_starts_with_and_ends_with_match_across_normalization_forms(cluster):
    """``str_starts_with`` / ``str_ends_with`` match canonically, not byte-exact.

    The server's canonical search backs prefix and suffix matching too
    (four ``get_canon_search`` call sites, not two), so an affix in either
    normalization form matches a value stored in the other.
    """
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_norm_affix")

    async def probe(stored, op, affix):
        await sess.upsert(k).bin("s").set_to(stored).execute()
        chain = sess.query(k).bin("s")
        rs = await getattr(chain, op)(affix).execute()
        return (await rs.first_or_raise()).record_or_raise().bins["s"]

    assert await probe(_NFC + " au lait", "str_starts_with", _NFD) is True
    assert await probe(_NFD + " au lait", "str_starts_with", _NFC) is True
    assert await probe("au lait " + _NFC, "str_ends_with", _NFD) is True
    assert await probe("au lait " + _NFD, "str_ends_with", _NFC) is True


# ---------------------------------------------------------------------------
# Result-size cap
#
# Modify ops bound their estimated result at prepare time
# (particle_string.c string_modify_set_estimated_size). Exceeding the bound
# is PARAMETER_ERROR and nothing is written, so it is reported independently
# of RECORD_TOO_BIG — which the same ops raise for a result that clears the
# cap but outgrows the namespace record limit.
# ---------------------------------------------------------------------------

# Ceiling the server puts on a modify op's estimated result size.
_RESULT_SIZE_CAP = 8 * 1024 * 1024


async def test_modify_past_result_cap_raises_parameter_error(cluster):
    """Each estimate-growing modify op past the cap fails with ``PARAMETER_ERROR``."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_result_cap")
    await sess.upsert(k).bin("s").set_to("hello").execute()

    async def assert_param_error(build):
        with pytest.raises(AerospikeError) as exc_info:
            await build(sess.upsert(k).bin("s")).execute()
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR

    # Estimated as old_size * count.
    await assert_param_error(lambda b: b.str_repeat(_RESULT_SIZE_CAP))
    # Estimated as target_length * 4 — worst-case UTF-8 expansion.
    await assert_param_error(lambda b: b.str_pad_start(_RESULT_SIZE_CAP // 4 + 1, "*"))
    await assert_param_error(lambda b: b.str_pad_end(_RESULT_SIZE_CAP // 4 + 1, "*"))
    # Estimated as old_size + argument size, so only the argument can carry
    # the result past the cap.
    await assert_param_error(lambda b: b.str_concat("x" * _RESULT_SIZE_CAP))

    # Nothing was written by any of the rejected ops.
    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "hello"


# ---------------------------------------------------------------------------
# Expression-path sweeps
#
# The same string sub-ops reached as expressions rather than operations: each
# one projects a computed column and leaves the stored bin alone, so a whole
# family fits in one query. Sweeps rather than a test per member — the risk
# being covered is a mis-encoded argument, and one column per member catches
# that at a fraction of the round trips.
# ---------------------------------------------------------------------------

async def test_str_exp_read_sweep(cluster):
    """Every read expression, one projected column each."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_read_sweep")
    await sess.upsert(k) \
        .bin("s").set_to("hello") \
        .bin("csv").set_to("a,b,c") \
        .bin("b64").set_to("aGVsbG8=") \
        .execute()

    s = Exp.string_bin("s")
    rs = await sess.query(k) \
        .bin("strlen").select_from(Exp.string_strlen(s)) \
        .bin("substr").select_from(Exp.string_substr(Exp.val(1), s)) \
        .bin("substr_range").select_from(Exp.string_substr_range(Exp.val(1), Exp.val(4), s)) \
        .bin("char_at").select_from(Exp.string_char_at(Exp.val(1), s)) \
        .bin("find").select_from(Exp.string_find(Exp.val("ll"), s)) \
        .bin("find_nth").select_from(Exp.string_find_nth(Exp.val("l"), Exp.val(2), s)) \
        .bin("contains").select_from(Exp.string_contains(Exp.val("ell"), s)) \
        .bin("starts_with").select_from(Exp.string_starts_with(Exp.val("he"), s)) \
        .bin("ends_with").select_from(Exp.string_ends_with(Exp.val("lo"), s)) \
        .bin("byte_length").select_from(Exp.string_byte_length(s)) \
        .bin("is_numeric").select_from(Exp.string_is_numeric(s)) \
        .bin("is_numeric_int").select_from(
            Exp.string_is_numeric_typed(StringNumericType.INT, s)) \
        .bin("is_upper").select_from(Exp.string_is_upper(s)) \
        .bin("is_lower").select_from(Exp.string_is_lower(s)) \
        .bin("split").select_from(Exp.string_split(s)) \
        .bin("split_sep").select_from(
            Exp.string_split_by_separator(Exp.val(","), Exp.string_bin("csv"))) \
        .bin("b64_decode").select_from(Exp.string_b64_decode(Exp.string_bin("b64"))) \
        .bin("regex").select_from(Exp.string_regex_compare(Exp.val("^h.*o$"), s)) \
        .bin("regex_flags").select_from(Exp.string_regex_compare_with_flags(
            Exp.val("^H.*O$"), int(StringRegexFlags.CASE_INSENSITIVE), s)) \
        .execute()
    b = (await rs.first_or_raise()).record_or_raise().bins

    assert b["strlen"] == 5
    assert b["substr"] == "ello"
    assert b["substr_range"] == "ell"
    assert b["char_at"] == "e"
    assert b["find"] == 2
    assert b["find_nth"] == 3          # occurrence is 1-based; the 2nd "l"
    assert b["contains"] is True
    assert b["starts_with"] is True
    assert b["ends_with"] is True
    assert b["byte_length"] == 5
    assert b["is_numeric"] is False
    assert b["is_numeric_int"] is False
    assert b["is_upper"] is False
    assert b["is_lower"] is True
    assert b["split"] == ["h", "e", "l", "l", "o"]   # splits per codepoint
    assert b["split_sep"] == ["a", "b", "c"]
    assert b["b64_decode"] == b"hello"   # declared BLOB: base64 decodes to bytes
    assert b["regex"] is True
    assert b["regex_flags"] is True    # matches only because of CASE_INSENSITIVE
    # Projections compute; they never touch storage.
    assert b.get("s", "hello") == "hello"


async def test_str_exp_modify_sweep(cluster):
    """Every modify expression, one projected column each, none persisting."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_modify_sweep")
    await sess.upsert(k) \
        .bin("s").set_to("Hello") \
        .bin("nfc").set_to("e\u0301") \
        .execute()

    f = int(StringWriteFlags.DEFAULT)
    s = Exp.string_bin("s")
    rs = await sess.query(k) \
        .bin("orig").select_from(s) \
        .bin("upper").select_from(Exp.string_upper(f, s)) \
        .bin("lower").select_from(Exp.string_lower(f, s)) \
        .bin("case_fold").select_from(Exp.string_case_fold(f, s)) \
        .bin("normalize").select_from(Exp.string_normalize_nfc(f, Exp.string_bin("nfc"))) \
        .bin("insert").select_from(Exp.string_insert(f, Exp.val(1), Exp.val("X"), s)) \
        .bin("overwrite").select_from(Exp.string_overwrite(f, Exp.val(1), Exp.val("i"), s)) \
        .bin("snip").select_from(Exp.string_snip(f, Exp.val(1), Exp.val(4), s)) \
        .bin("snip_from").select_from(Exp.string_snip_from(Exp.val(3), s)) \
        .bin("snip_from_neg").select_from(Exp.string_snip_from(Exp.val(-2), s)) \
        .bin("append").select_from(Exp.string_append(f, Exp.val("!"), s)) \
        .bin("prepend").select_from(Exp.string_prepend(f, Exp.val(">"), s)) \
        .bin("replace").select_from(Exp.string_replace(f, Exp.val("lo"), Exp.val("LL"), s)) \
        .bin("replace_all").select_from(
            Exp.string_replace_all(f, Exp.val("l"), Exp.val("L"), s)) \
        .bin("pad_start").select_from(
            Exp.string_pad_start(f, Exp.val(7), Exp.val("0"), s)) \
        .bin("pad_end").select_from(Exp.string_pad_end(f, Exp.val(10), Exp.val("."), s)) \
        .bin("repeat").select_from(Exp.string_repeat(f, Exp.val(2), s)) \
        .bin("concat").select_from(Exp.string_concat(f, Exp.list_val(["!", "?"]), s)) \
        .execute()
    b = (await rs.first_or_raise()).record_or_raise().bins

    assert b["orig"] == "Hello"
    assert b["upper"] == "HELLO"
    assert b["lower"] == "hello"
    assert b["case_fold"] == "hello"
    assert b["normalize"] == "\u00e9"      # combining sequence folded to one codepoint
    assert b["insert"] == "HXello"
    assert b["overwrite"] == "Hillo"
    assert b["snip"] == "Ho"                # removes the half-open range [1, 4)
    # Truncate-to-end must actually truncate: a mispacked 2-element
    # [start, flags] payload is accepted by the server as [start, end] and
    # evaluates to the unchanged string.
    assert b["snip_from"] == "Hel"
    assert b["snip_from_neg"] == "Hel"      # negative start counts from the end
    assert b["append"] == "Hello!"
    assert b["prepend"] == ">Hello"
    assert b["replace"] == "HelLL"
    assert b["replace_all"] == "HeLLo"
    assert b["pad_start"] == "00Hello"
    assert b["pad_end"] == "Hello....."
    assert b["repeat"] == "HelloHello"
    assert b["concat"] == "Hello!?"

    # None of the above wrote anything: the bin still holds what was seeded.
    rs = await sess.query(k).bin("s").get().execute()
    assert (await rs.first_or_raise()).record_or_raise().bins["s"] == "Hello"


async def test_str_exp_trim_sweep(cluster):
    """The three trim expressions differ only in which end they touch."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_trim_sweep")
    await sess.upsert(k).bin("s").set_to("  pad  ").execute()

    f = int(StringWriteFlags.DEFAULT)
    s = Exp.string_bin("s")
    rs = await sess.query(k) \
        .bin("trim").select_from(Exp.string_trim(f, s)) \
        .bin("trim_start").select_from(Exp.string_trim_start(f, s)) \
        .bin("trim_end").select_from(Exp.string_trim_end(f, s)) \
        .execute()
    b = (await rs.first_or_raise()).record_or_raise().bins

    assert b["trim"] == "pad"
    assert b["trim_start"] == "pad  "
    assert b["trim_end"] == "  pad"


async def test_str_exp_regex_replace(cluster):
    """``GLOBAL`` decides whether every match or only the first is replaced."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_regex_replace")
    await sess.upsert(k).bin("s").set_to("abc123def456").execute()

    s = Exp.string_bin("s")
    rs = await sess.query(k) \
        .bin("first").select_from(Exp.string_regex_replace(
            Exp.val("[0-9]+"), Exp.val("NUM"), int(StringRegexFlags.DEFAULT), s)) \
        .bin("every").select_from(Exp.string_regex_replace(
            Exp.val("[0-9]+"), Exp.val("NUM"), int(StringRegexFlags.GLOBAL), s)) \
        .execute()
    b = (await rs.first_or_raise()).record_or_raise().bins

    assert b["first"] == "abcNUMdef456"
    assert b["every"] == "abcNUMdefNUM"


async def test_str_exp_conversion_sweep(cluster):
    """The type-conversion expressions, including the family-agnostic to_string."""
    sess = cluster.create_session()
    k = _TEST_DS.id("strop_exp_convert_sweep")
    await sess.upsert(k) \
        .bin("n").set_to(42) \
        .bin("i").set_to("42") \
        .bin("d").set_to("3.5") \
        .bin("s").set_to("hello") \
        .execute()

    rs = await sess.query(k) \
        .bin("to_string").select_from(Exp.to_string(Exp.int_bin("n"))) \
        .bin("to_integer").select_from(Exp.string_to_integer(Exp.string_bin("i"))) \
        .bin("to_double").select_from(Exp.string_to_double(Exp.string_bin("d"))) \
        .bin("to_blob").select_from(Exp.string_to_blob(Exp.string_bin("s"))) \
        .execute()
    b = (await rs.first_or_raise()).record_or_raise().bins

    assert b["to_string"] == "42"
    assert b["to_integer"] == 42
    assert b["to_double"] == 3.5
    assert b["to_blob"] == b"hello"
