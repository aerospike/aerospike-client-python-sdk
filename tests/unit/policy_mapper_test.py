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

"""Tests for policy_mapper: Settings -> PAC policy type conversion."""

from datetime import timedelta

import pytest
from aerospike_async import (
    BatchPolicy,
    CommitLevel,
    ReadModeAP,
    ReadModeSC,
    ReadPolicy,
    QueryPolicy,
    Replica,
    WritePolicy,
)

from aerospike_sdk.policy.policy_mapper import (
    apply_to_read_policy,
    apply_to_write_policy,
    to_batch_policy,
    to_read_policy,
    to_query_policy,
    to_write_policy,
)
from aerospike_sdk.policy.behavior_settings import Settings


class TestToReadPolicy:
    """Verify to_read_policy() maps all relevant Settings fields to
    ReadPolicy and leaves the policy at defaults when fields are None."""
    def test_all_fields(self):
        s = Settings(
            total_timeout=timedelta(seconds=10),
            socket_timeout=timedelta(seconds=2),
            max_retries=3,
            retry_delay=timedelta(milliseconds=100),
            replica=Replica.PREFER_RACK,
            read_mode_ap=ReadModeAP.ALL,
            read_mode_sc=ReadModeSC.LINEARIZE,
            use_compression=True,
        )
        p = to_read_policy(s)
        assert p.total_timeout == 10_000
        assert p.socket_timeout == 2_000
        assert p.max_retries == 3
        assert p.sleep_between_retries == 100
        assert p.replica == Replica.PREFER_RACK
        assert p.read_mode_ap == ReadModeAP.ALL
        assert p.read_mode_sc == ReadModeSC.LINEARIZE
        assert p.use_compression is True

    def test_none_fields_not_set(self):
        p = to_read_policy(Settings())
        assert isinstance(p, ReadPolicy)

    def test_compression_threshold_propagates(self):
        # ``compression_threshold`` should round-trip from Settings into the
        # underlying ReadPolicy alongside use_compression.
        s = Settings(use_compression=True, compression_threshold=1024)
        p = to_read_policy(s)
        assert p.use_compression is True
        assert p.compression_threshold == 1024


class TestToWritePolicy:
    """Verify to_write_policy() maps Settings fields (including write-
    specific ones like send_key and commit_level) to WritePolicy."""
    def test_all_fields(self):
        s = Settings(
            total_timeout=timedelta(seconds=5),
            socket_timeout=timedelta(seconds=1),
            max_retries=1,
            retry_delay=timedelta(milliseconds=50),
            send_key=True,
            durable_delete=True,
            commit_level=CommitLevel.COMMIT_ALL,
        )
        p = to_write_policy(s)
        assert p.total_timeout == 5_000
        assert p.socket_timeout == 1_000
        assert p.max_retries == 1
        assert p.sleep_between_retries == 50
        assert p.send_key is True
        assert p.durable_delete is True
        assert p.commit_level == CommitLevel.COMMIT_ALL

    def test_none_fields_not_set(self):
        p = to_write_policy(Settings())
        assert isinstance(p, WritePolicy)

    def test_compression_threshold_propagates(self):
        s = Settings(use_compression=True, compression_threshold=2048)
        p = to_write_policy(s)
        assert p.use_compression is True
        assert p.compression_threshold == 2048


class TestToQueryPolicy:
    """Verify to_query_policy() maps Settings fields (including query-
    specific ones like max_concurrent_nodes and record_queue_size)
    to QueryPolicy."""
    def test_all_fields(self):
        s = Settings(
            total_timeout=timedelta(seconds=60),
            socket_timeout=timedelta(seconds=10),
            max_retries=5,
            retry_delay=timedelta(seconds=1),
            replica=Replica.SEQUENCE,
            max_concurrent_nodes=8,
            record_queue_size=10000,
        )
        p = to_query_policy(s)
        assert p.total_timeout == 60_000
        assert p.socket_timeout == 10_000
        assert p.max_retries == 5
        assert p.sleep_between_retries == 1_000
        assert p.replica == Replica.SEQUENCE
        assert p.max_concurrent_nodes == 8
        assert p.record_queue_size == 10000

    def test_none_fields_not_set(self):
        p = to_query_policy(Settings())
        assert isinstance(p, QueryPolicy)

    def test_compression_threshold_propagates(self):
        s = Settings(use_compression=True, compression_threshold=512)
        p = to_query_policy(s)
        assert p.use_compression is True
        assert p.compression_threshold == 512


class TestToBatchPolicy:
    """Verify to_batch_policy() maps Settings fields (including batch-
    specific ones like allow_inline and allow_inline_ssd) to BatchPolicy."""
    def test_all_fields(self):
        s = Settings(
            total_timeout=timedelta(seconds=15),
            socket_timeout=timedelta(seconds=3),
            max_retries=4,
            retry_delay=timedelta(milliseconds=200),
            allow_inline=True,
            allow_inline_ssd=False,
        )
        p = to_batch_policy(s)
        assert p.total_timeout == 15_000
        assert p.socket_timeout == 3_000
        assert p.max_retries == 4
        assert p.sleep_between_retries == 200
        assert p.allow_inline is True
        assert p.allow_inline_ssd is False

    def test_none_fields_not_set(self):
        p = to_batch_policy(Settings())
        assert isinstance(p, BatchPolicy)

    def test_compression_threshold_propagates(self):
        s = Settings(use_compression=True, compression_threshold=4096)
        p = to_batch_policy(s)
        assert p.use_compression is True
        assert p.compression_threshold == 4096


class TestApplyToReadPolicy:
    """Verify apply_to_read_policy() fills unset numeric fields (zero-check)
    while always applying enum fields like replica. Explicitly-set policy
    fields are preserved."""
    def test_fills_unset_fields(self):
        s = Settings(replica=Replica.PREFER_RACK)
        p = ReadPolicy()
        result = apply_to_read_policy(s, p)
        assert result.replica == Replica.PREFER_RACK

    def test_does_not_overwrite_existing(self):
        s = Settings(total_timeout=timedelta(seconds=10))
        p = ReadPolicy()
        p.total_timeout = 3000
        result = apply_to_read_policy(s, p)
        assert result.total_timeout == 3000


class TestApplyToWritePolicy:
    """Verify apply_to_write_policy() fills unset numeric fields (zero-check)
    while always applying send_key and commit_level. Explicitly-set policy
    fields are preserved."""
    def test_fills_unset_fields(self):
        s = Settings(
            send_key=True,
            durable_delete=True,
            commit_level=CommitLevel.COMMIT_ALL,
        )
        p = WritePolicy()
        result = apply_to_write_policy(s, p)
        assert result.send_key is True
        assert result.durable_delete is True
        assert result.commit_level == CommitLevel.COMMIT_ALL

    def test_does_not_overwrite_timeout(self):
        s = Settings(total_timeout=timedelta(seconds=5))
        p = WritePolicy()
        p.total_timeout = 2000
        result = apply_to_write_policy(s, p)
        assert result.total_timeout == 2000
