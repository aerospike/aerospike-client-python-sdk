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

"""Stable logger names for operator tuning.

The SDK emits diagnostics through Python's standard :mod:`logging` package
under a small set of stable, cross-cutting logger names. Hosts raise
verbosity for one operational area (queries, commands, background tasks)
without enabling the whole SDK, the same way they tune other database
drivers.

The SDK never configures handlers, formatters, or levels on import — those
are host-application choices.
"""

from __future__ import annotations

import aerospike_async

# Absent on PAC releases that predate the log-level cache refresh entry;
# refresh_log_levels() degrades to a no-op there.
_pac_refresh_log_levels = getattr(aerospike_async, "refresh_log_levels", None)


def refresh_log_levels() -> None:
    """Re-sync Rust-emitted log levels with the Python ``logging`` hierarchy.

    The bridge that forwards Rust log records (the ``aerospike_core`` and
    ``aerospike_async`` loggers) caches each logger's effective level the
    first time that logger emits. A runtime ``setLevel()`` on those loggers
    is invisible to Rust-emitted records until the cache is dropped by
    calling this function. The SDK calls it automatically on every client
    connect, so it is only needed when changing levels while connected.

    Levels on ``aerospike_sdk.*`` loggers are read live by Python's
    :mod:`logging` and never need a refresh.

    Example:
        Raise cluster-tend verbosity on a live client during an incident::

            import logging
            from aerospike_sdk import refresh_log_levels

            logging.getLogger("aerospike_core.cluster").setLevel(logging.DEBUG)
            refresh_log_levels()

    See Also:
        :class:`SdkLoggers`: Stable logger names for the Python-side areas.
    """
    if _pac_refresh_log_levels is not None:
        _pac_refresh_log_levels()


class SdkLoggers:
    """Stable logger names emitted by the SDK, for host-side configuration.

    Each constant is a dotted name under the ``aerospike_sdk`` root, so
    configuring ``aerospike_sdk`` adjusts every SDK logger at once, while a
    specific constant narrows to one operational area.

    Two additional roots are emitted below the SDK and are tuned the same
    way: ``aerospike_async`` (PAC client lifecycle) and ``aerospike_core``
    (Rust core: cluster tend, connection pools, wire protocol).

    Example:
        Enable per-operation command summaries during an incident, keeping
        the rest of the stack quiet::

            import logging

            logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
            logging.getLogger(SdkLoggers.COMMAND).setLevel(logging.DEBUG)
            logging.getLogger("aerospike_core").setLevel(logging.WARNING)

    See Also:
        :doc:`/guide/logging`: Component table, production defaults, and
        the user-data policy.
    """

    #: Point-op and batch command summaries (op type, counts, latency).
    COMMAND = "aerospike_sdk.command"
    #: Dataset / secondary-index query execution summaries.
    QUERY = "aerospike_sdk.query"
    #: Info-protocol helpers (``namespace_details``, index metadata probes).
    INFO = "aerospike_sdk.info"
    #: Background task submission (background writes, background UDF).
    BACKGROUND = "aerospike_sdk.background"
    #: Secondary-index cache refresh (:class:`~aerospike_sdk.index_monitor.IndexesMonitor`).
    INDEX_MONITOR = "aerospike_sdk.index_monitor"
    #: Client connect / close on both the async and sync surfaces.
    LIFECYCLE = "aerospike_sdk.lifecycle"
    #: :class:`~aerospike_sdk.aio.pool.AsyncPool` start / stop and loop threads.
    POOL = "aerospike_sdk.pool"
    #: Record stream chunk fetches and close diagnostics.
    RECORD_STREAM = "aerospike_sdk.record_stream"
