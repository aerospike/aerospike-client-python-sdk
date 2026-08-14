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

from typing import Any, Union

from aerospike_async import FilterExpression

from aerospike_sdk.exceptions import AerospikeError, ResultCode


def bind_ael_params(
    expression: Union[str, FilterExpression],
    params: tuple[Any, ...],
) -> Union[str, FilterExpression]:
    """Interpolate printf-style *params* into an AEL template.

    An empty *params* passes the template through untouched, so an AEL string is
    only ever treated as a format string when the caller supplies values. Use
    ``%%`` to escape literal ``%`` when params are supplied.

    Booleans are lowered to ``true`` / ``false`` and ``None`` to ``null``;
    Python's ``%s`` would otherwise emit ``True`` or ``None``, which the AEL
    parser rejects.

    Raises:
        TypeError: If *params* accompany a non-string expression.
        ValueError: If the template is not a valid printf format string.
    """
    if not params:
        return expression
    if not isinstance(expression, str):
        raise TypeError(
            "AEL params require a string template, got "
            f"{type(expression).__name__}",
        )
    bound = tuple(
        "true"
        if p is True
        else "false"
        if p is False
        else "null"
        if p is None
        else p
        for p in params
    )
    try:
        return expression % bound
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"cannot bind params into AEL template {expression!r}: {exc}. "
            "AEL's '%' (modulo) must be written '%%' when params are supplied.",
        ) from exc


def filter_expression_from_ael_string(
    ael: str,
    *,
    supports_server_compiled_ael: bool,
) -> FilterExpression:
    """Return field **43** ``FilterExpression`` for *ael* (server compiles at eval time).

    Raises:
        AerospikeError: When the cluster lacks server-compiled AEL support,
            carrying ``ResultCode.OP_NOT_APPLICABLE`` so callers can branch on
            the result code rather than the exception type.
    """
    if not supports_server_compiled_ael:
        raise AerospikeError(
            "String AEL requires server-compiled AEL support (Aerospike >= 8.1.3 "
            "on every node). Use FilterExpression / Exp builders, or upgrade the cluster.",
            result_code=ResultCode.OP_NOT_APPLICABLE,
        )
    return FilterExpression.from_server_compiled_ael(ael)
