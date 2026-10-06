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

"""Error handling strategy for SDK operations.

Controls how per-record errors are surfaced during execution:

- **Default** (no argument): single-key operations raise immediately;
  batch / multi-key operations embed errors in the ``RecordStream``.
- **ErrorStrategy.IN_STREAM**: always embed errors as ``RecordResult``
  entries with non-OK result codes, even for single-key operations.
- **ErrorHandler** callback: errors are dispatched to the callback and
  excluded from the returned stream.
"""

from __future__ import annotations

from enum import Enum
from typing import Callable, Union

from aerospike_async import Key

from aerospike_sdk.exceptions import AerospikeError


class ErrorStrategy(Enum):
    """Strategy for handling per-record errors during execution.

    Pass to ``execute(on_error=...)`` to override the default behavior.

    Example::

        stream = await (
            session.query(k1, k2).execute(on_error=ErrorStrategy.IN_STREAM)
        )

    """

    IN_STREAM = "in_stream"
    """Embed errors in the ``RecordStream`` as ``RecordResult`` entries."""


ErrorHandler = Callable[[Key, int, AerospikeError], None]
"""Callback ``(key, index, exception) -> None`` for per-record error handling.

The callback receives the failed record's key, its position in the batch
(0-based; 0 for single-key, -1 for queries), and the typed exception.
Errors dispatched to the handler are excluded from the returned stream.
"""

OnError = Union[ErrorStrategy, ErrorHandler]
"""Type alias for the ``on_error`` parameter of ``execute()``."""


# ---------------------------------------------------------------------------
# Internal disposition (not part of public API)
# ---------------------------------------------------------------------------

class _ErrorDisposition(Enum):
    """Resolved error routing decision, threaded through execution internals."""

    THROW = "throw"
    IN_STREAM = "in_stream"
    HANDLER = "handler"


def _resolve_disposition(
    on_error: OnError | None,
    is_single_key: bool,
) -> _ErrorDisposition:
    """Resolve user-facing ``on_error`` to an internal disposition.

    When ``on_error`` is ``None``, the default depends on cardinality:
    single-key operations raise (THROW), batch operations embed (IN_STREAM).
    """
    if on_error is None:
        return _ErrorDisposition.THROW if is_single_key else _ErrorDisposition.IN_STREAM
    if isinstance(on_error, ErrorStrategy):
        if on_error is ErrorStrategy.IN_STREAM:
            return _ErrorDisposition.IN_STREAM
    return _ErrorDisposition.HANDLER
