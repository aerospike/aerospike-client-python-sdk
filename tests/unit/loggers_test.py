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

"""Tests for the stable logger-name taxonomy and command-summary helpers.

The names in :class:`SdkLoggers` are operator-facing configuration keys —
changing one silently breaks host logging configs, so they are pinned here.
"""

import logging

from time import perf_counter

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.operations_shared import (
    _CMD_DEBUG,
    _cmd_cluster,
    _cmd_done,
    _cmd_enabled,
    _cmd_failed,
)


class _FakeClient:
    """Stand-in for a PAC client exposing the ``cluster_name`` attribute."""

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name


class TestSdkLoggerNames:
    """The taxonomy values are a public contract with host logging configs."""

    def test_names_are_pinned(self):
        assert SdkLoggers.COMMAND == "aerospike_sdk.command"
        assert SdkLoggers.QUERY == "aerospike_sdk.query"
        assert SdkLoggers.INFO == "aerospike_sdk.info"
        assert SdkLoggers.BACKGROUND == "aerospike_sdk.background"
        assert SdkLoggers.INDEX_MONITOR == "aerospike_sdk.index_monitor"
        assert SdkLoggers.LIFECYCLE == "aerospike_sdk.lifecycle"
        assert SdkLoggers.POOL == "aerospike_sdk.pool"
        assert SdkLoggers.RECORD_STREAM == "aerospike_sdk.record_stream"

    def test_all_names_live_under_the_sdk_root(self):
        names = [
            v for k, v in vars(SdkLoggers).items() if not k.startswith("_") and isinstance(v, str)
        ]
        assert names, "taxonomy unexpectedly empty"
        for name in names:
            assert name.startswith("aerospike_sdk."), name


class TestCommandSummaryHelpers:

    def test_guard_is_false_when_command_debug_is_off(self):
        logger = logging.getLogger(SdkLoggers.COMMAND)
        previous = logger.level
        logger.setLevel(logging.INFO)
        try:
            assert not _cmd_enabled(_CMD_DEBUG)
        finally:
            logger.setLevel(previous)

    def test_guard_is_true_when_command_debug_is_on(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            assert _cmd_enabled(_CMD_DEBUG)

    def test_guard_tracks_runtime_level_changes(self):
        logger = logging.getLogger(SdkLoggers.COMMAND)
        previous = logger.level
        try:
            logger.setLevel(logging.WARNING)
            assert not _cmd_enabled(_CMD_DEBUG)
            logger.setLevel(logging.DEBUG)
            assert _cmd_enabled(_CMD_DEBUG)
        finally:
            logger.setLevel(previous)

    def test_cmd_done_emits_operational_fields_only(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            t0 = perf_counter()
            _cmd_done("upsert", "test", "users", 1, t0)
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.name == SdkLoggers.COMMAND
        message = record.getMessage()
        assert message.startswith("upsert test.users keys=1 latency_ms=")

    def test_cmd_done_labels_plain_reads(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            _cmd_done(None, "test", "users", 3, perf_counter())
        assert caplog.records[0].getMessage().startswith("read test.users keys=3")

    def test_cmd_failed_logs_exception_type_not_message(self, caplog):
        exc = ValueError("secret-bin-value=42")
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            _cmd_failed("delete", 2, exc)
        message = caplog.records[0].getMessage()
        assert "ValueError" in message
        assert "rc=2" in message
        assert "secret-bin-value" not in message

    def test_cmd_failed_is_silent_when_debug_is_off(self, caplog):
        logger = logging.getLogger(SdkLoggers.COMMAND)
        previous = logger.level
        logger.setLevel(logging.WARNING)
        try:
            with caplog.at_level(logging.WARNING, logger=SdkLoggers.COMMAND):
                _cmd_failed("delete", 2, ValueError("x"))
            assert not caplog.records
        finally:
            logger.setLevel(previous)


class TestClusterTag:
    """The configured cluster name rides along as a structured field."""

    def test_cmd_cluster_reads_attribute(self):
        assert _cmd_cluster(_FakeClient("prod-1")) == "prod-1"

    def test_cmd_cluster_is_none_without_the_attribute(self):
        assert _cmd_cluster(object()) is None
        assert _cmd_cluster(None) is None

    def test_cmd_done_attaches_cluster_field(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            _cmd_done("upsert", "test", "users", 1, perf_counter(), _FakeClient("prod-1"))
        record = caplog.records[0]
        assert getattr(record, "aerospike.cluster") == "prod-1"

    def test_cmd_done_cluster_field_is_none_when_unconfigured(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            _cmd_done("upsert", "test", "users", 1, perf_counter())
        assert getattr(caplog.records[0], "aerospike.cluster") is None

    def test_cmd_failed_attaches_cluster_field(self, caplog):
        with caplog.at_level(logging.DEBUG, logger=SdkLoggers.COMMAND):
            _cmd_failed("delete", 2, ValueError("x"), _FakeClient("prod-2"))
        assert getattr(caplog.records[0], "aerospike.cluster") == "prod-2"
