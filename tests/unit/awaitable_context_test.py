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

"""Unit tests for the awaitable / async-context-manager hybrid.

The hybrid exists so a factory call reads ``async with f()`` instead of
``async with await f()``. All three spellings must reach the same object, and
the scoped form must still run the result's own ``__aexit__``.
"""

import pytest

from aerospike_sdk.awaitable_context import AwaitableContext


class _Tracked:
    """Stand-in for a connected resource that closes on scope exit."""

    def __init__(self) -> None:
        self.entered = 0
        self.exited = 0
        self.exc_type = None

    async def __aenter__(self) -> "_Tracked":
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.exited += 1
        self.exc_type = exc_type
        return False


def _factory(resource: _Tracked) -> AwaitableContext[_Tracked]:
    async def _make() -> _Tracked:
        return resource

    return AwaitableContext(_make())


async def test_plain_await_returns_the_result_unentered() -> None:
    resource = _Tracked()
    assert await _factory(resource) is resource
    assert resource.entered == 0
    assert resource.exited == 0


async def test_async_with_enters_and_exits_the_result() -> None:
    resource = _Tracked()
    async with _factory(resource) as entered:
        assert entered is resource
        assert resource.entered == 1
        assert resource.exited == 0
    assert resource.exited == 1


async def test_double_keyword_spelling_still_works() -> None:
    # The pre-hybrid spelling awaits first, then enters the result directly.
    resource = _Tracked()
    async with await _factory(resource) as entered:
        assert entered is resource
    assert resource.entered == 1
    assert resource.exited == 1


async def test_exception_propagates_and_still_exits() -> None:
    resource = _Tracked()
    with pytest.raises(RuntimeError, match="boom"):
        async with _factory(resource):
            raise RuntimeError("boom")
    assert resource.exited == 1
    assert resource.exc_type is RuntimeError


async def test_failed_setup_does_not_call_aexit() -> None:
    async def _fail() -> _Tracked:
        raise ConnectionError("unreachable")

    ctx: AwaitableContext[_Tracked] = AwaitableContext(_fail())
    with pytest.raises(ConnectionError, match="unreachable"):
        async with ctx:
            pytest.fail("body must not run when setup fails")
