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

"""Encode string AEL as server-compiled filter expressions (field 43)."""

from __future__ import annotations

from aerospike_async import FilterExpression


def filter_expression_from_ael_string(
    ael: str,
    *,
    supports_server_compiled_ael: bool,
) -> FilterExpression:
    """Return field **43** ``FilterExpression`` for *ael* (server compiles at eval time).

    Raises:
        ValueError: When the cluster lacks server-compiled AEL support.
    """
    if not supports_server_compiled_ael:
        raise ValueError(
            "String AEL requires server-compiled AEL support (Aerospike >= 8.1.3 "
            "on every node). Use FilterExpression / Exp builders, or upgrade the cluster.",
        )
    return FilterExpression.from_server_compiled_ael(ael)
