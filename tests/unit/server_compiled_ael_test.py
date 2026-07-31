# Copyright 2025-2026 Aerospike, Inc.
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

"""Unit tests for server-compiled AEL routing helpers."""

from unittest.mock import MagicMock, patch

from aerospike_sdk.ael.server_filter import filter_expression_from_ael_string
from aerospike_sdk.server_compiled_ael import (
    compute_server_compiled_ael_support_blocking,
)


class TestFilterExpressionFromAelString:
    def test_uses_client_parse_when_gate_off(self):
        with patch("aerospike_sdk.ael.server_filter.parse_ael") as parse_ael:
            sentinel = object()
            parse_ael.return_value = sentinel
            result = filter_expression_from_ael_string(
                "$.age > 1",
                supports_server_compiled_ael=False,
            )
        assert result is sentinel
        parse_ael.assert_called_once_with("$.age > 1")

    def test_uses_server_compiled_when_gate_on(self):
        sentinel = object()
        factory = MagicMock(return_value=sentinel)
        with patch(
            "aerospike_sdk.ael.server_filter._SERVER_COMPILED_FACTORY",
            factory,
        ):
            with patch(
                "aerospike_sdk.ael.server_filter._PAC_EXPOSES_SERVER_COMPILED",
                True,
            ):
                with patch("aerospike_sdk.ael.server_filter.parse_ael") as parse_ael:
                    result = filter_expression_from_ael_string(
                        "$.age > 1",
                        supports_server_compiled_ael=True,
                    )
        assert result is sentinel
        factory.assert_called_once_with("$.age > 1")
        parse_ael.assert_not_called()


class TestComputeServerCompiledAelSupport:
    def test_false_when_factory_missing(self):
        pac = MagicMock()
        pac.nodes_blocking.return_value = [MagicMock(version=MagicMock())]
        with patch(
            "aerospike_sdk.server_compiled_ael._pac_exposes_server_compiled_factory",
            return_value=False,
        ):
            assert compute_server_compiled_ael_support_blocking(pac) is False

    def test_all_nodes_must_support(self):
        pac = MagicMock()
        v_ok = MagicMock()
        v_ok.supports_server_compiled_ael.return_value = True
        v_old = MagicMock()
        v_old.supports_server_compiled_ael.return_value = False
        pac.nodes_blocking.return_value = [
            MagicMock(version=v_ok),
            MagicMock(version=v_old),
        ]
        with patch(
            "aerospike_sdk.server_compiled_ael._pac_exposes_server_compiled_factory",
            return_value=True,
        ):
            assert compute_server_compiled_ael_support_blocking(pac) is False
