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

from collections.abc import Mapping
from typing import Any, Union

from aerospike_native import FilterExpression

from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.prepared_ael import PreparedAel


def _bind_prepared(expression: PreparedAel, params: tuple[Any, ...]) -> str:
    if not params:
        return expression.bind()
    values = params[0]
    if len(params) != 1 or not (type(values) is dict or isinstance(values, Mapping)):
        raise TypeError(
            "pass the values for a PreparedAel as one mapping of placeholder "
            'name to value, like where(template, {"min_age": 21})',
        )
    return expression._bind(values)


def bind_ael_params(
    expression: Union[str, PreparedAel, FilterExpression],
    params: tuple[Any, ...],
) -> Union[str, FilterExpression]:
    """Bind *params* into an AEL template.

    A :class:`~aerospike_sdk.PreparedAel` takes one mapping of placeholder
    name to value and binds each value as a typed AEL literal. A plain string
    is a printf template: an empty *params* passes it through untouched, so an
    AEL string is only ever treated as a format string when the caller
    supplies values. Use ``%%`` to escape literal ``%`` when params are
    supplied.

    In the printf form booleans are lowered to ``true`` / ``false``; Python's
    ``%s`` would otherwise emit ``True``, which the AEL parser rejects.

    Raises:
        TypeError: If *params* accompany a ``FilterExpression``, the values
            for a ``PreparedAel`` are not one mapping, a value has no AEL
            literal, or a printf param is ``None``.
        ValueError: If the template is not a valid printf format string, or a
            ``PreparedAel`` cannot bind the values.
    """
    if isinstance(expression, PreparedAel):
        return _bind_prepared(expression, params)
    if not params:
        return expression
    if not isinstance(expression, str):
        raise TypeError(
            "AEL params require a string template, got "
            f"{type(expression).__name__}",
        )
    for p in params:
        if p is None:
            # AEL has no null literal; the server rejects the word wherever it appears.
            raise TypeError("None has no AEL literal and cannot be bound")
    bound = tuple(
        ["true" if p is True else "false" if p is False else p for p in params],
    )
    try:
        return expression % bound
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"cannot bind params into AEL template {expression!r}: {exc}. "
            "AEL's '%' (modulo) must be written '%%' when params are supplied.",
        ) from exc


def bind_prepared_ael_params(
    expression: Union[str, PreparedAel, FilterExpression],
    params: tuple[Any, ...],
) -> Union[str, FilterExpression]:
    """Bind *params* for a method that takes them only with a ``PreparedAel``.

    Raises:
        TypeError: If *params* accompany anything but a ``PreparedAel``, or
            are not one mapping.
    """
    if isinstance(expression, PreparedAel):
        return _bind_prepared(expression, params)
    if params:
        raise TypeError(
            "params require a PreparedAel template, got "
            f"{type(expression).__name__}",
        )
    return expression


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
            "String AEL requires server-compiled AEL support (Aerospike >= 8.2.0 "
            "on every node). Use FilterExpression / Exp builders, or upgrade the cluster.",
            result_code=ResultCode.OP_NOT_APPLICABLE,
        )
    return FilterExpression.from_server_compiled_ael(ael)
