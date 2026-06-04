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

import pytest
from aerospike_async import FilterExpression

from aerospike_sdk import parse_ael
from aerospike_sdk.ael.server_filter import filter_expression_from_ael_string


def test_server_filter_uses_parse_when_not_supported() -> None:
    fe = filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_filter_expression=False,
    )
    assert fe == parse_ael("$.x > 1")


def test_server_filter_uses_server_compiled_when_supported() -> None:
    if not callable(getattr(FilterExpression, "from_server_compiled_ael", None)):
        pytest.skip("PAC lacks FilterExpression.from_server_compiled_ael")
    fe = filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_filter_expression=True,
    )
    assert fe != parse_ael("$.x > 1")


def test_server_filter_falls_back_when_pac_lacks_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _NoFactory:
        pass

    import aerospike_sdk.ael.server_filter as sf

    monkeypatch.setattr(sf, "FilterExpression", _NoFactory)
    fe = sf.filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_filter_expression=True,
    )
    assert fe == parse_ael("$.x > 1")


def test_server_filter_respects_force_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AEROSPIKE_SDK_FORCE_CLIENT_AEL_PARSE", "1")
    try:
        fe = filter_expression_from_ael_string(
            "$.x > 1",
            supports_server_compiled_filter_expression=True,
        )
        assert fe == parse_ael("$.x > 1")
    finally:
        monkeypatch.delenv("AEROSPIKE_SDK_FORCE_CLIENT_AEL_PARSE", raising=False)
