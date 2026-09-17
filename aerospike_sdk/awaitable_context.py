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

"""Hybrid awaitable / async-context-manager returned by async factory calls.

Lets one call site serve both spellings::

    cluster = await definition.connect()          # plain await

    async with definition.connect() as cluster:   # scoped, no double keyword
        ...

The awaited result is what the wrapped coroutine produced; the ``async with``
form additionally enters that result's own context manager, so scope exit
still runs the object's ``__aexit__``.
"""

from __future__ import annotations

import types
from typing import Any, Awaitable, Coroutine, Generator, Generic, Optional, Protocol, Self, TypeVar


class _AsyncClosable(Protocol):
    """An object that is itself an async context manager."""

    async def __aenter__(self) -> Self: ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any: ...


_T = TypeVar("_T", bound=_AsyncClosable)


class AwaitableContext(Awaitable[_T], Generic[_T]):
    """Awaitable wrapper that also works directly in ``async with``.

    Returned by factory calls whose result is itself an async context manager
    (a connected cluster, an open record stream). Awaiting yields the result
    unchanged; entering runs the same await and then delegates to the result's
    own ``__aenter__`` / ``__aexit__``.

    Example::

        async with session.query(users).execute() as stream:
            async for row in stream:
                print(row.bins)

    See Also:
        :meth:`aerospike_sdk.aio.cluster_definition.ClusterDefinition.connect`
    """

    __slots__ = ("_coro", "_result")

    def __init__(self, coro: Coroutine[Any, Any, _T]) -> None:
        self._coro = coro
        self._result: Optional[_T] = None

    def __await__(self) -> Generator[Any, None, _T]:
        # Hand back the coroutine's own generator rather than awaiting it in a
        # wrapper frame, so the plain-await path costs one object and no extra
        # suspension point.
        return self._coro.__await__()

    async def __aenter__(self) -> _T:
        result = await self._coro
        self._result = result
        return await result.__aenter__()

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> Any:
        result = self._result
        if result is None:
            return None
        self._result = None
        return await result.__aexit__(exc_type, exc_val, exc_tb)
