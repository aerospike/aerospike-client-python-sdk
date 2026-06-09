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
# License for the specific language governing permissions and limitations
# under the License.

"""Shared helpers for server-side background dataset operations."""

from __future__ import annotations

from typing import Any, Optional, Sequence

from aerospike_async import (
    Expiration,
    FilterExpression,
    RecordExistsAction,
    Statement,
    WritePolicy,
)
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape
from aerospike_sdk.policy.policy_mapper import resolve_durable_delete, to_write_policy

_TTL_NEVER_EXPIRE = -1
_TTL_DONT_UPDATE = -2
_TTL_SERVER_DEFAULT = 0


def ttl_to_expiration(ttl: int) -> Expiration:
    """Convert an integer TTL value to an ``Expiration`` object."""
    if ttl == _TTL_NEVER_EXPIRE:
        return Expiration.NEVER_EXPIRE
    if ttl == _TTL_DONT_UPDATE:
        return Expiration.DONT_UPDATE
    if ttl == _TTL_SERVER_DEFAULT:
        return Expiration.NAMESPACE_DEFAULT
    return Expiration.seconds(ttl)


def reject_unsupported_background_write_ops(operations: Sequence[Any]) -> None:
    """Raise if *operations* contain CDT/HLL types unsupported for background query_operate."""
    for op in operations:
        name = type(op).__name__
        if name in (
            "MapOperation", "ListOperation", "BitOperation", "HllOperation",
        ):
            raise AerospikeError(
                "Collection and HLL operations are not supported for "
                "background task execution.",
                result_code=ResultCode.OP_NOT_APPLICABLE,
            )


def make_background_write_policy(
    behavior: Optional[Behavior],
    filter_expression: Optional[FilterExpression],
    ttl_seconds: Optional[int],
    record_exists_action: Optional[RecordExistsAction] = None,
    *,
    namespace_mode: Mode = Mode.AP,
    durable_delete_command_default: Optional[bool] = None,
    durable_delete_override: Optional[bool] = None,
) -> WritePolicy:
    """Build a ``WritePolicy`` for background ``query_operate`` / ``query_execute_udf``."""
    if behavior is not None:
        settings = behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.QUERY, namespace_mode)
        wp = to_write_policy(settings)
        wp.durable_delete = resolve_durable_delete(
            settings.durable_delete,
            durable_delete_command_default,
            durable_delete_override,
        )
    else:
        wp = WritePolicy()
        wp.durable_delete = resolve_durable_delete(
            None,
            durable_delete_command_default,
            durable_delete_override,
        )
    if filter_expression is not None:
        wp.filter_expression = filter_expression
    if ttl_seconds is not None:
        wp.expiration = ttl_to_expiration(ttl_seconds)
    if record_exists_action is not None:
        wp.record_exists_action = record_exists_action
    return wp


def dataset_statement(namespace: str, set_name: str) -> Statement:
    """Statement for a full-set scan or expression-filtered scan/query."""
    return Statement(namespace, set_name, None)
