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

"""Unit tests for server-compiled AEL filter encoding."""

from unittest.mock import MagicMock, patch

import pytest

from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.server_filter import filter_expression_from_ael_string


def test_raises_when_gate_off():
    """Old clusters reject string AEL with the code the Java SDK uses."""
    with pytest.raises(AerospikeError, match="server-compiled AEL") as exc_info:
        filter_expression_from_ael_string(
            "$.age > 1",
            supports_server_compiled_ael=False,
        )
    assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


def test_uses_server_compiled_when_gate_on():
    sentinel = MagicMock()
    with patch(
        "aerospike_sdk.server_filter.FilterExpression.from_server_compiled_ael",
        return_value=sentinel,
    ) as factory:
        result = filter_expression_from_ael_string(
            "$.age > 1",
            supports_server_compiled_ael=True,
        )
    factory.assert_called_once_with("$.age > 1")
    assert result is sentinel
