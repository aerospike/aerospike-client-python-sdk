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

"""PAC capability markers and skip helpers shared by unit and integration tests.

Integration tests declare requirements with :data:`requires_server_compiled_ael`
or :data:`requires_query_selection`; ``tests/integration/conftest.py`` resolves
a connected SDK client from fixtures and calls the matching skip helper
(``Client.supports_*``, computed from PAC ``Version.supports_*`` at connect).
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol

import pytest
from aerospike_async.exceptions import InvalidRequest

from aerospike_sdk.exceptions import AerospikeError, ResultCode


class SupportsPacCapabilities(Protocol):
    """Connected SDK client that reports PAC/cluster capability flags."""

    @property
    def supports_server_compiled_ael(self) -> bool:
        ...

    @property
    def supports_query_selection(self) -> bool:
        ...


def _capability_bool(client: object, attr: str) -> bool:
    """Read a connected SDK client's ``supports_*`` flag; fail if not a bool."""
    value = getattr(client, attr, None)
    if not isinstance(value, bool):
        pytest.fail(
            f"{attr!r} must be a bool property on the resolved SDK client, "
            f"got {type(value).__name__}",
            pytrace=False,
        )
    return value


def has_sdk_capability_properties(candidate: object) -> bool:
    """True when *candidate* exposes both ``supports_*`` bool properties."""
    return (
        isinstance(getattr(candidate, "supports_server_compiled_ael", None), bool)
        and isinstance(getattr(candidate, "supports_query_selection", None), bool)
    )


def skip_if_lacks_server_compiled_ael(client: SupportsPacCapabilities) -> None:
    """Skip when server-compiled AEL is not available for this connection/cluster.

    Reads :attr:`SupportsPacCapabilities.supports_server_compiled_ael` (the same
    public property on :class:`~aerospike_sdk.aio.client.Client` / sync client).
    """
    if _capability_bool(client, "supports_server_compiled_ael") is True:
        return
    pytest.skip(
        "Requires server-compiled AEL: Version.supports_server_compiled_ael on all nodes "
        "(Client.supports_server_compiled_ael)."
    )


def skip_if_lacks_query_selection(client: SupportsPacCapabilities) -> None:
    """Skip when field ``44`` query selection is not available for this cluster.

    Reads :attr:`SupportsPacCapabilities.supports_query_selection` (the same
    public property on :class:`~aerospike_sdk.aio.client.Client` / sync client).
    """
    if _capability_bool(client, "supports_query_selection") is True:
        return
    pytest.skip(
        "Requires query selection: Version.supports_query_selection on all nodes "
        "(Client.supports_query_selection)."
    )


async def assert_dataset_invalid_ael_rejected(execute_coro: Awaitable[Any]) -> None:
    """Assert invalid string AEL on a dataset query is rejected by the server.

    With query selection (explain→execute), ``PARAMETER_ERROR`` is raised from
    ``execute()``. With server-compiled AEL on field **43**, ``execute()`` may
    return a stream and the cluster rejects the filter while reading rows.
    """
    stream = None
    try:
        try:
            stream = await execute_coro
        except (AerospikeError, InvalidRequest) as exc:
            assert exc.result_code == ResultCode.PARAMETER_ERROR
            return

        with pytest.raises((AerospikeError, InvalidRequest)) as exc_info:
            async for _ in stream:
                pass
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
    finally:
        if stream is not None:
            stream.close()


async def assert_point_invalid_ael_rejected(execute_coro: Awaitable[Any]) -> None:
    """Assert invalid string AEL on a point query is rejected (field **43** path)."""
    try:
        rs = await execute_coro
    except (AerospikeError, InvalidRequest) as exc:
        assert exc.result_code == ResultCode.PARAMETER_ERROR
        return

    with pytest.raises((AerospikeError, InvalidRequest)) as exc_info:
        await rs.first_or_raise()
    assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR


requires_server_compiled_ael = pytest.mark.requires_server_compiled_ael
requires_query_selection = pytest.mark.requires_query_selection
