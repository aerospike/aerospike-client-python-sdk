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

"""Pick client-parsed vs server-compiled filter wire form for AEL strings."""

from __future__ import annotations

from aerospike_async import FilterExpression

from aerospike_sdk.ael.parser import parse_ael

def filter_expression_from_ael_string(
    ael: str,
    *,
    supports_server_compiled_ael: bool,
) -> FilterExpression:
    """Return a ``FilterExpression`` for *ael*, using server-compiled wire form when allowed.

    When ``supports_server_compiled_ael`` is true
    returns that (MessagePack ``[128, "<utf-8 ael>"]``). Otherwise, parses on the
    client via :func:`~aerospike_sdk.ael.parser.parse_ael`.
    """
    if supports_server_compiled_ael:
        factory = getattr(FilterExpression, "from_server_compiled_ael", None)
        if callable(factory):
            return factory(ael)
    return parse_ael(ael)
