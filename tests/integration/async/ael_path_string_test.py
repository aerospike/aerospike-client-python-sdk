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

"""Path, loop-variable and string-method AEL through ``where``, ``select_from`` and ``upsert_from``.

The server compiles all of these, so the tests pin what it accepts and returns rather
than anything PSDK computes.
"""

import pytest

from aerospike_sdk import ResultCode
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.namespace import general_namespace
from tests.pac_compat import requires_server_compiled_ael

_KEY = DataSet.of(general_namespace(), "ael_path_string").id("r1")
_LIST = [100, 200, 300, 400, 500]
_SEED = {
    "l": _LIST,
    "m": {"alpha": 10, "beta": 20, "gamma": 30},
    "s": "Hello World",
    "padded": "  hi  ",
    "numstr": "42",
}


@pytest.fixture(scope="module")
async def session_with_paths(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        await session.replace(_KEY).put(_SEED).execute()
        yield session
        await session.delete(_KEY).execute()


async def _select(session, expression):
    stream = await session.query(_KEY).bin("out").select_from(expression).execute()
    return (await stream.first_or_raise()).record_or_raise().bins["out"]


async def _matches(session, expression) -> int:
    stream = await session.query(_KEY).where(expression).execute()
    return len([record async for record in stream])


@requires_server_compiled_ael
@pytest.mark.parametrize("expression, expected", [
    ("$.m:MAP.alpha:INT", 10),
    ("$.l:LIST.[1]:INT", 200),
    ("$.l:LIST.[#-1]:INT", 500),
    ("$.l:LIST.[0:2]", [100, 200]),
    ("$.m:MAP.{@alpha,gamma}", [10, 30]),
    ("$.l:LIST.*[?(@:INT > 200)]", [300, 400, 500]),
    ("$.l:LIST.*[?(@:INT > 200)].count()", 3),
    ("$.m:MAP.*[?(@key.upper() == 'BETA')].count()", 1),
    ("$.l:LIST.*[?((@:INT).toString() == '200')].count()", 1),
    ("$.s:STRING.upper()", "HELLO WORLD"),
    ("$.padded:STRING.trim()", "hi"),
    ("$.numstr:STRING.toInt()", 42),
])
async def test_select_from_evaluates_path_and_string_ael(session_with_paths, expression, expected):
    assert await _select(session_with_paths, expression) == expected


@requires_server_compiled_ael
@pytest.mark.parametrize("expression, expected", [
    ("$.l:LIST.[#-1]:INT == 500", 1),
    ("$.l:LIST.[#-1]:INT == 400", 0),
    ("$.l:LIST.*[?(@:INT > 200)].count() == 3", 1),
    ("$.s:STRING.upper() == 'HELLO WORLD'", 1),
    ("$.s:STRING =~ /^hello/i", 1),
    ("$.s:STRING =~ /^hello/", 0),
])
async def test_where_filters_on_path_and_string_ael(session_with_paths, expression, expected):
    assert await _matches(session_with_paths, expression) == expected


@requires_server_compiled_ael
@pytest.mark.parametrize("expression, expected", [
    ("$.l:LIST.append(600)", _LIST + [600]),
    ("$.l:LIST.[0]:INT.setTo(999)", [999, 200, 300, 400, 500]),
    ("$.l:LIST.appendItems([600, 700]):ADD_UNIQUE", _LIST + [600, 700]),
    ("$.l:LIST.*[?(@:INT > 300)].modify(@:INT + 1)", [100, 200, 300, 401, 501]),
])
async def test_upsert_from_writes_the_modified_copy_and_leaves_the_source(
    session_with_paths, expression, expected,
):
    await session_with_paths.upsert(_KEY).bin("out").upsert_from(expression).execute()
    stream = await session_with_paths.query(_KEY).bins(["l", "out"]).execute()
    bins = (await stream.first_or_raise()).record_or_raise().bins
    assert bins["out"] == expected
    assert bins["l"] == _LIST


@requires_server_compiled_ael
@pytest.mark.parametrize("expression, result_code", [
    # An unpinned ``@`` parses, so the cast fails only when evaluated.
    ("$.l:LIST.*[?(@.toString() == '200')].count()", ResultCode.OP_NOT_APPLICABLE),
    # A pinned loop variable needs parentheses before a method call.
    ("$.l:LIST.*[?(@:INT.toString() == '200')].count()", ResultCode.PARAMETER_ERROR),
])
async def test_loop_var_cast_rejections(session_with_paths, expression, result_code):
    with pytest.raises(AerospikeError) as excinfo:
        await _select(session_with_paths, expression)
    assert excinfo.value.result_code == result_code
