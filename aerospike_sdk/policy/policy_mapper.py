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

"""Map resolved Settings to PAC policy objects."""

from __future__ import annotations

from typing import Optional

from aerospike_async import (
    BatchPolicy,
    BatchReadPolicy,
    ReadPolicy,
    QueryPolicy,
    WritePolicy,
)
from aerospike_async.exceptions import ValueError as _PacValueError

from aerospike_sdk.policy.behavior_settings import Settings


def _to_value_error(exc: _PacValueError) -> ValueError:
    """Translate a PAC policy-validation error into a built-in ``ValueError``.

    ``read_touch_ttl`` is the only policy field the underlying async client
    (PAC) validates on assignment, raising ``aerospike_async.exceptions``. We
    surface it as a built-in :exc:`ValueError` so callers only ever catch
    Python/SDK exceptions, consistent with the rest of the SDK's input
    validation, rather than reaching into ``aerospike_async``.
    """
    return ValueError(str(exc))


def resolve_durable_delete(
    setting: Optional[bool],
    command_default: Optional[bool],
    override: Optional[bool],
) -> bool:
    """Resolve durable-delete intent using override, command default, then behavior."""
    if override is not None:
        return override
    if command_default is not None:
        return command_default
    return bool(setting)


def _ms(td) -> int:
    """Convert a timedelta to integer milliseconds."""
    return int(td.total_seconds() * 1000)


def to_read_policy(settings: Settings) -> ReadPolicy:
    """Build a ReadPolicy from resolved Settings.

    Uses :meth:`ReadPolicy.from_fields` so that the full policy crosses the
    Rust boundary exactly once instead of once per field.

    Raises:
        ValueError: If ``read_touch_ttl_percent`` is out of range (must be -1,
            0, or 1-100).
    """
    try:
        return ReadPolicy.from_fields(
            total_timeout=(
                _ms(settings.total_timeout)
                if settings.total_timeout is not None else None
            ),
            socket_timeout=(
                _ms(settings.socket_timeout)
                if settings.socket_timeout is not None else None
            ),
            max_retries=settings.max_retries,
            sleep_between_retries=(
                _ms(settings.retry_delay)
                if settings.retry_delay is not None else None
            ),
            replica=settings.replica,
            read_mode_ap=settings.read_mode_ap,
            read_mode_sc=settings.read_mode_sc,
            read_touch_ttl=settings.read_touch_ttl_percent,
            use_compression=settings.use_compression,
            compression_threshold=settings.compression_threshold,
            error_detail_verbosity=settings.error_detail_verbosity,
        )
    except _PacValueError as e:
        raise _to_value_error(e) from e


def to_write_policy(settings: Settings) -> WritePolicy:
    """Build a WritePolicy from resolved Settings.

    Uses :meth:`WritePolicy.from_fields` so that the full policy crosses the
    Rust boundary exactly once instead of once per field.
    """
    return WritePolicy.from_fields(
        total_timeout=(
            _ms(settings.total_timeout)
            if settings.total_timeout is not None else None
        ),
        socket_timeout=(
            _ms(settings.socket_timeout)
            if settings.socket_timeout is not None else None
        ),
        max_retries=settings.max_retries,
        sleep_between_retries=(
            _ms(settings.retry_delay)
            if settings.retry_delay is not None else None
        ),
        send_key=settings.send_key,
        durable_delete=settings.durable_delete,
        commit_level=settings.commit_level,
        use_compression=settings.use_compression,
        compression_threshold=settings.compression_threshold,
        error_detail_verbosity=settings.error_detail_verbosity,
    )


def to_query_policy(settings: Settings) -> QueryPolicy:
    """Build a QueryPolicy from resolved Settings.

    ``QueryPolicy`` has no bulk constructor, so this still fills fields
    individually; keeping the shape simple for now.
    """
    p = QueryPolicy()
    if settings.total_timeout is not None:
        p.total_timeout = _ms(settings.total_timeout)
    if settings.socket_timeout is not None:
        p.socket_timeout = _ms(settings.socket_timeout)
    if settings.max_retries is not None:
        p.max_retries = settings.max_retries
    if settings.retry_delay is not None:
        p.sleep_between_retries = _ms(settings.retry_delay)
    if settings.replica is not None:
        p.replica = settings.replica
    if settings.read_mode_ap is not None:
        p.read_mode_ap = settings.read_mode_ap
    if settings.read_mode_sc is not None:
        p.read_mode_sc = settings.read_mode_sc
    if settings.use_compression is not None:
        p.use_compression = settings.use_compression
    if settings.compression_threshold is not None:
        p.compression_threshold = settings.compression_threshold
    if settings.max_concurrent_nodes is not None:
        p.max_concurrent_nodes = settings.max_concurrent_nodes
    if settings.record_queue_size is not None:
        p.record_queue_size = settings.record_queue_size
    if settings.error_detail_verbosity is not None:
        p.error_detail_verbosity = settings.error_detail_verbosity
    return p


def to_batch_read_policy(settings: Settings) -> BatchReadPolicy:
    """Build a BatchReadPolicy from resolved Settings.

    Raises:
        ValueError: If ``read_touch_ttl_percent`` is out of range (must be -1,
            0, or 1-100).
    """
    p = BatchReadPolicy()
    if settings.read_touch_ttl_percent is not None:
        try:
            p.read_touch_ttl = settings.read_touch_ttl_percent
        except _PacValueError as e:
            raise _to_value_error(e) from e
    return p


def to_batch_policy(settings: Settings) -> BatchPolicy:
    """Build a BatchPolicy from resolved Settings.

    Uses :meth:`BatchPolicy.from_fields` so that the full policy crosses the
    Rust boundary exactly once instead of once per field.
    """
    return BatchPolicy.from_fields(
        total_timeout=(
            _ms(settings.total_timeout)
            if settings.total_timeout is not None else None
        ),
        socket_timeout=(
            _ms(settings.socket_timeout)
            if settings.socket_timeout is not None else None
        ),
        max_retries=settings.max_retries,
        sleep_between_retries=(
            _ms(settings.retry_delay)
            if settings.retry_delay is not None else None
        ),
        allow_inline=settings.allow_inline,
        allow_inline_ssd=settings.allow_inline_ssd,
        use_compression=settings.use_compression,
        compression_threshold=settings.compression_threshold,
        error_detail_verbosity=settings.error_detail_verbosity,
    )


def apply_to_read_policy(settings: Settings, policy: ReadPolicy) -> ReadPolicy:
    """Apply non-None Settings fields onto an existing ReadPolicy.

    Used when an explicit policy was provided by the user and behavior
    settings should fill in any fields the user did not set.
    """
    if settings.total_timeout is not None and policy.total_timeout == 0:
        policy.total_timeout = _ms(settings.total_timeout)
    if settings.socket_timeout is not None and policy.socket_timeout == 0:
        policy.socket_timeout = _ms(settings.socket_timeout)
    if settings.max_retries is not None and policy.max_retries == 0:
        policy.max_retries = settings.max_retries
    if settings.replica is not None:
        policy.replica = settings.replica
    if settings.read_mode_ap is not None:
        policy.read_mode_ap = settings.read_mode_ap
    if settings.read_mode_sc is not None:
        policy.read_mode_sc = settings.read_mode_sc
    # 0 is the ReadPolicy default (server-default touch TTL); treat it as unset
    # so behavior settings fill it without clobbering an explicit non-default.
    if settings.read_touch_ttl_percent is not None and policy.read_touch_ttl == 0:
        try:
            policy.read_touch_ttl = settings.read_touch_ttl_percent
        except _PacValueError as e:
            raise _to_value_error(e) from e
    if settings.use_compression is not None:
        policy.use_compression = settings.use_compression
    if settings.compression_threshold is not None:
        policy.compression_threshold = settings.compression_threshold
    # 0 (NONE) is the policy default; treat it as unset so behavior settings
    # fill it without clobbering an explicit non-default verbosity.
    if settings.error_detail_verbosity is not None and policy.error_detail_verbosity == 0:
        policy.error_detail_verbosity = settings.error_detail_verbosity
    return policy


def apply_to_write_policy(settings: Settings, policy: WritePolicy) -> WritePolicy:
    """Apply non-None Settings fields onto an existing WritePolicy.

    Explicit user-set fields on the policy take precedence; behavior
    settings act as defaults for unset fields.
    """
    if settings.total_timeout is not None and policy.total_timeout == 0:
        policy.total_timeout = _ms(settings.total_timeout)
    if settings.socket_timeout is not None and policy.socket_timeout == 0:
        policy.socket_timeout = _ms(settings.socket_timeout)
    if settings.max_retries is not None and policy.max_retries == 0:
        policy.max_retries = settings.max_retries
    if settings.send_key is not None:
        policy.send_key = settings.send_key
    if settings.durable_delete is not None:
        policy.durable_delete = settings.durable_delete
    if settings.commit_level is not None:
        policy.commit_level = settings.commit_level
    if settings.use_compression is not None:
        policy.use_compression = settings.use_compression
    if settings.compression_threshold is not None:
        policy.compression_threshold = settings.compression_threshold
    # 0 (NONE) is the policy default; treat it as unset so behavior settings
    # fill it without clobbering an explicit non-default verbosity.
    if settings.error_detail_verbosity is not None and policy.error_detail_verbosity == 0:
        policy.error_detail_verbosity = settings.error_detail_verbosity
    return policy
