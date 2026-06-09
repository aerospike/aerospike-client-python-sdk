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

"""Settings and scope types for the Behavior model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional

from aerospike_async import CommitLevel, ReadModeAP, ReadModeSC, Replica


class OpKind(Enum):
    """The kind of database operation."""
    READ = "read"
    WRITE_RETRYABLE = "write_retryable"
    WRITE_NON_RETRYABLE = "write_non_retryable"


class OpShape(Enum):
    """The shape (cardinality) of the operation."""
    POINT = "point"
    BATCH = "batch"
    QUERY = "query"


class Mode(Enum):
    """Namespace consistency mode."""
    AP = "ap"
    SC = "sc"


class Scope(Enum):
    """Named scopes for configuring operation settings within a Behavior.

    Scope values correspond to keyword arguments accepted by
    ``Behavior.derive_with_changes()``.
    """
    ALL = "all"
    READS = "reads"
    READS_POINT = "reads_point"
    READS_BATCH = "reads_batch"
    READS_QUERY = "reads_query"
    READS_AP = "reads_ap"
    READS_SC = "reads_sc"
    WRITES = "writes"
    WRITES_RETRYABLE = "writes_retryable"
    WRITES_NON_RETRYABLE = "writes_non_retryable"
    WRITES_POINT = "writes_point"
    WRITES_BATCH = "writes_batch"
    WRITES_QUERY = "writes_query"
    WRITES_AP = "writes_ap"
    WRITES_SC = "writes_sc"


@dataclass(frozen=True)
class Settings:
    """Immutable set of operation settings.

    All fields are Optional; ``None`` means "not configured / inherit from
    a less-specific scope or the parent Behavior".
    """

    total_timeout: Optional[timedelta] = None
    socket_timeout: Optional[timedelta] = None
    max_retries: Optional[int] = None
    retry_delay: Optional[timedelta] = None

    send_key: Optional[bool] = None
    durable_delete: Optional[bool] = None
    commit_level: Optional[CommitLevel] = None

    replica: Optional[Replica] = None
    read_mode_ap: Optional[ReadModeAP] = None
    read_mode_sc: Optional[ReadModeSC] = None

    use_compression: Optional[bool] = None
    compression_threshold: Optional[int] = None

    max_concurrent_nodes: Optional[int] = None
    record_queue_size: Optional[int] = None
    allow_inline: Optional[bool] = None
    allow_inline_ssd: Optional[bool] = None

    read_touch_ttl_percent: Optional[int] = None

    @classmethod
    def merge(cls, base: Settings, override: Settings) -> Settings:
        """Merge two Settings; override's non-None fields win."""
        return cls(
            total_timeout=_pick(override.total_timeout, base.total_timeout),
            socket_timeout=_pick(override.socket_timeout, base.socket_timeout),
            max_retries=_pick(override.max_retries, base.max_retries),
            retry_delay=_pick(override.retry_delay, base.retry_delay),
            send_key=_pick(override.send_key, base.send_key),
            durable_delete=_pick(override.durable_delete, base.durable_delete),
            commit_level=_pick(override.commit_level, base.commit_level),
            replica=_pick(override.replica, base.replica),
            read_mode_ap=_pick(override.read_mode_ap, base.read_mode_ap),
            read_mode_sc=_pick(override.read_mode_sc, base.read_mode_sc),
            use_compression=_pick(override.use_compression, base.use_compression),
            compression_threshold=_pick(override.compression_threshold, base.compression_threshold),
            max_concurrent_nodes=_pick(override.max_concurrent_nodes, base.max_concurrent_nodes),
            record_queue_size=_pick(override.record_queue_size, base.record_queue_size),
            allow_inline=_pick(override.allow_inline, base.allow_inline),
            allow_inline_ssd=_pick(override.allow_inline_ssd, base.allow_inline_ssd),
            read_touch_ttl_percent=_pick(override.read_touch_ttl_percent, base.read_touch_ttl_percent),
        )


def _pick(override, base):
    """Return override if not None, else base."""
    return override if override is not None else base


_READ_SHAPE_SCOPES = {
    OpShape.POINT: Scope.READS_POINT,
    OpShape.BATCH: Scope.READS_BATCH,
    OpShape.QUERY: Scope.READS_QUERY,
}

_WRITE_SHAPE_SCOPES = {
    OpShape.POINT: Scope.WRITES_POINT,
    OpShape.BATCH: Scope.WRITES_BATCH,
    OpShape.QUERY: Scope.WRITES_QUERY,
}


def resolution_order(kind: OpKind, shape: OpShape, mode: Mode = Mode.AP) -> tuple[Scope, ...]:
    """Return applicable scopes from least-specific to most-specific.

    Used by ``Behavior.get_settings()`` to layer patches so that
    more-specific scopes override less-specific ones.
    """
    order: list[Scope] = [Scope.ALL]

    if kind == OpKind.READ:
        order.append(Scope.READS)
        order.append(Scope.READS_AP if mode == Mode.AP else Scope.READS_SC)
        order.append(_READ_SHAPE_SCOPES[shape])
    else:
        order.append(Scope.WRITES)
        order.append(Scope.WRITES_AP if mode == Mode.AP else Scope.WRITES_SC)
        if kind == OpKind.WRITE_RETRYABLE:
            order.append(Scope.WRITES_RETRYABLE)
        else:
            order.append(Scope.WRITES_NON_RETRYABLE)
        order.append(_WRITE_SHAPE_SCOPES[shape])

    return tuple(order)
