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

"""PreparedAel against a live server: every bound literal compiles and means its value.

The server compiles the text, so these pin that each literal form is accepted
and that a bound value never reaches outside its own literal.
"""

import pytest

from aerospike_sdk import PreparedAel
from aerospike_sdk.dataset import DataSet

from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_server_compiled_ael

pytestmark = requires_server_compiled_ael

_DS = DataSet.of(general_namespace(), "prepared_ael")
_VALUE_KEY = _DS.id("value")
_PEOPLE = {"bob": 30, "amy": 41, "cy": 19}

_HOSTILE_STRINGS = [
    "o'brien",
    'say "hi"',
    "it's \"x\"",
    "trailing\\",
    "\\'",
    "' or true or $.v == '",
    "\\' or true or $.v == \\'",
    '" or $.v != "',
    "*/ or true /*",
    "line\nbreak\r\ttab",
    "nul\0ctl\x01del\x7f",
    "?v ?0 %s $.v",
    "héllo ☃ 😀",
]


@pytest.fixture(scope="module")
async def session(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        await session.truncate(_DS)
        yield session
        await session.truncate(_DS)


@pytest.fixture
async def people(session):
    keys = [_DS.id(name) for name in _PEOPLE]
    for name, age in _PEOPLE.items():
        await session.replace(_DS.id(name)).put({"name": name, "age": age}).execute()
    yield keys
    await session.delete(*keys).execute()


async def _names(stream) -> set:
    return {row.record_or_raise().bins["name"] async for row in stream}


async def _value_matches(session, stored, bound) -> int:
    """Store *stored* in bin ``v`` and count records whose ``v`` equals *bound*."""
    await session.replace(_VALUE_KEY).put({"v": stored}).execute()
    stream = await session.query(_VALUE_KEY).where(PreparedAel("$.v == ?v"), {"v": bound}).execute()
    count = len([row async for row in stream])
    await session.delete(_VALUE_KEY).execute()
    return count


@pytest.mark.parametrize(
    "value",
    [
        "Tim",
        "",
        *_HOSTILE_STRINGS,
        "".join(chr(code) for code in range(1, 0x80)),
        True,
        False,
        0,
        -7,
        2**63 - 1,
        -(2**63),
        1.5,
        -0.0,
        1e-7,
        1e20,
        1e60,
        0.1 + 0.2,
        b"",
        b"\xff\x00",
        bytes(range(256)),
        [],
        [1, "a'b", 1.5, True, b"\x01", [2, {"k": "v\"'"}]],
        {},
        {"b'c": [1, 2], "a": 1},
        {2: "x", 1: "y"},
        {b"\x01": 1, "s": {"n": 2.0}},
    ],
    ids=repr,
)
async def test_bound_value_matches_the_stored_value(session, value):
    assert await _value_matches(session, value, value) == 1


@pytest.mark.parametrize("value", _HOSTILE_STRINGS, ids=repr)
async def test_bound_string_stays_inside_its_literal(session, value):
    """Against a record holding another value, a string that escaped its quotes would match."""
    assert await _value_matches(session, "safe", value) == 0


@pytest.mark.parametrize(("stored", "bound"), [(2.0, 2), (3, 3.0), (True, 1)])
async def test_binding_keeps_the_python_type(session, stored, bound):
    """The server compares by type, so an ``int`` must not match a float bin, nor the reverse."""
    assert await _value_matches(session, stored, bound) == 0


async def test_query_where(session, people):
    older_than = PreparedAel("$.age > ?min_age and $.name != ?skip")
    stream = await session.query(_DS).where(older_than, {"min_age": 25, "skip": "amy"}).execute()
    assert await _names(stream) == {"bob"}


async def test_template_is_reusable_and_a_name_repeats(session, people):
    by_name_or_age = PreparedAel("$.age == ?age or $.name == ?name or $.age == ?age")
    first = await session.query(_DS).where(by_name_or_age, {"name": "bob", "age": 19}).execute()
    assert await _names(first) == {"bob", "cy"}
    second = await session.query(_DS).where(by_name_or_age, {"age": 30, "name": "amy"}).execute()
    assert await _names(second) == {"amy", "bob"}


async def test_in_list(session, people):
    in_names = PreparedAel("$.name in ?names")
    stream = await session.query(_DS).where(in_names, {"names": ["bob", "o'brien"]}).execute()
    assert await _names(stream) == {"bob"}


async def test_placeholder_text_inside_a_literal_is_not_bound(session, people):
    template = PreparedAel("$.name != '?name' and $.name == ?name /* ?other ?0 */")
    assert await _names(await session.query(_DS).where(template, {"name": "cy"}).execute()) == {"cy"}


async def test_write_segment_where(session, people):
    is_named = PreparedAel("$.name == ?name")
    stream = await (
        session.update(people[0]).bin("seen").set_to(True).where(is_named, {"name": "bob"})
        .update(people[1]).bin("seen").set_to(True).where(is_named, {"name": "nobody"})
        .execute()
    )
    stream.close()
    rows = [row async for row in await session.query(_DS).where("$.seen == true").execute()]
    assert {row.record_or_raise().bins["name"] for row in rows} == {"bob"}


async def test_default_where(session, people):
    stream = await (
        session.update(people[0]).bin("flag").set_to(1)
        .update(people[1]).bin("flag").set_to(1)
        .default_where(PreparedAel("$.age > ?min_age"), {"min_age": 35})
        .execute()
    )
    stream.close()
    assert await _names(await session.query(_DS).where("$.flag == 1").execute()) == {"amy"}


async def test_background_where(session, people):
    task = await (
        session.background_task()
        .update(_DS)
        .where(PreparedAel("$.name == ?name"), {"name": "cy"})
        .bin("tagged").set_to("yes")
        .execute()
    )
    assert await task.wait_till_complete()
    assert await _names(await session.query(_DS).where("$.tagged == 'yes'").execute()) == {"cy"}


async def test_select_from(session, people):
    stream = await (
        session.query(people[0])
        .bin("next").select_from(PreparedAel("$.age + ?years"), {"years": 5})
        .execute()
    )
    assert (await stream.first_or_raise()).record_or_raise().bins["next"] == 35


async def test_write_from_methods(session, people):
    key = people[0]
    add = PreparedAel("$.age + ?n")
    await session.upsert(key).bin("a").insert_from(add, {"n": 1}).execute()
    await session.upsert(key).bin("a").update_from(add, {"n": 2}).execute()
    await session.upsert(key).bin("b").upsert_from(add, {"n": 3}).execute()
    stream = await (
        session.upsert(key)
        .insert_from("c", add, {"n": 4})
        .update_from("c", add, {"n": 5})
        .upsert_from("d", add, {"n": 6})
        .select_from("e", add, {"n": 7})
        .execute()
    )
    assert (await stream.first_or_raise()).record_or_raise().bins["e"] == 37
    stored = (await (await session.query(key).execute()).first_or_raise()).record_or_raise().bins
    assert (stored["a"], stored["b"], stored["c"], stored["d"]) == (32, 33, 35, 36)


async def test_from_method_rejects_params_on_a_plain_string(session, people):
    with pytest.raises(TypeError, match="require a PreparedAel"):
        session.query(people[0]).bin("next").select_from("$.age + %d", 5)
    with pytest.raises(TypeError, match="require a PreparedAel"):
        session.upsert(people[0]).bin("a").upsert_from("$.age + %d", 5)


async def test_values_must_be_one_mapping(session, people):
    add = PreparedAel("$.age + ?n")
    with pytest.raises(TypeError, match="as one mapping"):
        session.query(_DS).where(PreparedAel("$.age > ?n"), 5)
    with pytest.raises(TypeError, match="as one mapping"):
        session.query(people[0]).bin("next").select_from(add, 5)
    with pytest.raises(TypeError, match="as one mapping"):
        session.upsert(people[0]).bin("a").upsert_from(add, 5)


async def test_geojson_text_binds_as_a_string_argument(session):
    from_region = PreparedAel("geoCompare($.loc, geoJson(?region))")
    circle = '{"type":"AeroCircle","coordinates":[[-122.349,47.62],3000.0]}'
    far = '{"type":"AeroCircle","coordinates":[[10.0,10.0],3000.0]}'
    await session.replace(_VALUE_KEY).put({"v": 1}).execute()
    await session.upsert(_VALUE_KEY).bin("loc").set_to_geo_json(
        '{"type":"Point","coordinates":[-122.349,47.62]}').execute()
    assert len([r async for r in await session.query(_VALUE_KEY).where(from_region, {"region": circle}).execute()]) == 1
    assert len([r async for r in await session.query(_VALUE_KEY).where(from_region, {"region": far}).execute()]) == 0
    await session.delete(_VALUE_KEY).execute()
