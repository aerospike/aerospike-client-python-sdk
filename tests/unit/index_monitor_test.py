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

"""Unit tests for index_monitor: info parsing and IndexesMonitor lifecycle.

The monitor now drives a daemon thread that polls PAC's blocking info APIs
(`info_on_all_nodes_blocking`, `info_blocking`) — no asyncio. Tests use
`MagicMock` to stand in for the PAC client and `time.sleep` to let the
background thread make progress between assertions.
"""

import time
from unittest.mock import MagicMock

import pytest

from aerospike_sdk.ael.filter_gen import IndexTypeEnum
from aerospike_sdk.index_monitor import (
    IndexesMonitor,
    _parse_entries_per_bval,
    _parse_sindex_list,
)


class TestParseSindexList:
    """Tests for _parse_sindex_list response parsing."""

    def test_single_index(self):
        raw = {"node1": {"sindex-list": (
            "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:"
            "indextype=default:context=null:state=RW"
        )}}
        entries = _parse_sindex_list(raw)
        assert len(entries) == 1
        e = entries[0]
        assert e["ns"] == "test"
        assert e["set"] == "users"
        assert e["bin"] == "age"
        assert e["indexname"] == "age_idx"
        assert e["type"] == "numeric"

    def test_multiple_indexes_semicolon_separated(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:"
                    "indextype=default:state=RW;"
                    "ns=test:indexname=name_idx:set=users:bin=name:type=string:"
                    "indextype=default:state=RW"
                )
            }
        }
        entries = _parse_sindex_list(raw)
        assert len(entries) == 2
        names = {e["indexname"] for e in entries}
        assert names == {"age_idx", "name_idx"}

    def test_deduplicates_across_nodes(self):
        entry = (
            "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW"
        )
        raw = {
            "node1": {"sindex-list": entry},
            "node2": {"sindex-list": entry},
        }
        entries = _parse_sindex_list(raw)
        assert len(entries) == 1

    def test_empty_response(self):
        raw = {"node1": {"sindex-list": ""}}
        assert _parse_sindex_list(raw) == []

    def test_entry_missing_required_fields_skipped(self):
        raw = {"node1": {"sindex-list": (
            "ns=test:indexname=incomplete;"
            "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW"
        )}}
        entries = _parse_sindex_list(raw)
        assert len(entries) == 1
        assert entries[0]["indexname"] == "age_idx"


class TestParseEntriesPerBval:
    """Tests for _parse_entries_per_bval from sindex-stat."""

    def test_extracts_value(self):
        raw = {"sindex-stat:...": "entries=100;entries_per_bval=1.5;keys=50"}
        assert _parse_entries_per_bval(raw) == 1.5

    def test_integer_value(self):
        raw = {"sindex-stat:...": "entries_per_bval=10"}
        assert _parse_entries_per_bval(raw) == 10.0

    def test_missing_field(self):
        raw = {"sindex-stat:...": "entries=100;keys=50"}
        assert _parse_entries_per_bval(raw) is None

    def test_empty_response(self):
        raw = {"sindex-stat:...": ""}
        assert _parse_entries_per_bval(raw) is None


class TestIndexesMonitorLifecycle:
    """Tests for IndexesMonitor start/stop and cache access."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.info_on_all_nodes_blocking.return_value = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW;"
                    "ns=test:indexname=name_idx:set=users:bin=name:type=string:state=RW;"
                    "ns=prod:indexname=total_idx:set=orders:bin=total:type=numeric:state=RW"
                )
            }
        }
        client.info_blocking.return_value = {
            "sindex-stat": "entries_per_bval=2.5"
        }
        return client

    def test_start_populates_cache(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            assert ctx.namespace == "test"
            assert len(ctx.indexes) == 2
            bins = {idx.bin for idx in ctx.indexes}
            assert bins == {"age", "name"}
        finally:
            monitor.stop()

    def test_index_set_name_plumbed_from_sindex_list(self, mock_client):
        """``set`` from sindex-list is wired into ``Index.set_name`` so query-set
        filtering can exclude indexes defined on a different set."""
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            sets = {idx.bin: idx.set_name for idx in ctx.indexes}
            assert sets == {"age": "users", "name": "users"}

            prod_ctx = monitor.get_index_context("prod")
            assert prod_ctx is not None
            assert prod_ctx.indexes[0].set_name == "orders"
        finally:
            monitor.stop()

    def test_blank_set_normalizes_to_none(self):
        """An sindex-list entry with an empty/missing ``set`` produces a
        cross-set Index (set_name=None)."""
        client = MagicMock()
        client.info_on_all_nodes_blocking.return_value = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=cross_idx:set=:bin=val:type=numeric:state=RW"
                )
            }
        }
        client.info_blocking.return_value = {"sindex-stat": "entries_per_bval=1.0"}
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            assert len(ctx.indexes) == 1
            assert ctx.indexes[0].set_name is None
        finally:
            monitor.stop()

    def test_different_namespaces(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            test_ctx = monitor.get_index_context("test")
            prod_ctx = monitor.get_index_context("prod")
            assert test_ctx is not None
            assert prod_ctx is not None
            assert len(test_ctx.indexes) == 2
            assert len(prod_ctx.indexes) == 1
            assert prod_ctx.indexes[0].bin == "total"
        finally:
            monitor.stop()

    def test_nonexistent_namespace_returns_none(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            assert monitor.get_index_context("nonexistent") is None
        finally:
            monitor.stop()

    def test_stop_joins_thread(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        assert monitor._thread is not None
        assert monitor._thread.is_alive()
        monitor.stop()
        assert monitor._thread is None

    def test_start_is_idempotent(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        thread1 = monitor._thread
        monitor.start(mock_client)
        assert monitor._thread is thread1
        monitor.stop()

    def test_index_type_mapping(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            type_map = {idx.bin: idx.index_type for idx in ctx.indexes}
            assert type_map["age"] == IndexTypeEnum.NUMERIC
            assert type_map["name"] == IndexTypeEnum.STRING
        finally:
            monitor.stop()

    def test_server_integer_type_maps_to_numeric(self):
        """Server 8.1.2+ may emit ``type=integer``; older servers emit
        ``type=numeric``. Both must collapse to ``IndexTypeEnum.NUMERIC``
        so filter selection is wire-version-agnostic."""
        client = MagicMock()
        client.info_on_all_nodes_blocking.return_value = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=integer:state=RW;"
                    "ns=test:indexname=score_idx:set=users:bin=score:type=numeric:state=RW"
                )
            }
        }
        client.info_blocking.return_value = {"sindex-stat": "entries_per_bval=1.0"}
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            type_map = {idx.bin: idx.index_type for idx in ctx.indexes}
            assert type_map["age"] == IndexTypeEnum.NUMERIC
            assert type_map["score"] == IndexTypeEnum.NUMERIC
        finally:
            monitor.stop()

    def test_bin_values_ratio_populated(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=60.0)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            ctx = monitor.get_index_context("test")
            assert ctx is not None
            for idx in ctx.indexes:
                assert idx.bin_values_ratio == 2.5
        finally:
            monitor.stop()

    def test_cache_refreshes_on_interval(self, mock_client):
        monitor = IndexesMonitor(refresh_interval=0.1)
        monitor.start(mock_client)
        monitor.wait_until_ready()
        try:
            initial_count = mock_client.info_on_all_nodes_blocking.call_count
            time.sleep(0.35)
            assert mock_client.info_on_all_nodes_blocking.call_count > initial_count
        finally:
            monitor.stop()

    def test_survives_fetch_error(self):
        client = MagicMock()
        call_count = 0

        def flaky_info(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("network blip")
            return {
                "node1": {
                    "sindex-list": (
                        "ns=test:indexname=age_idx:set=users:bin=age:"
                        "type=numeric:state=RW"
                    )
                }
            }

        client.info_on_all_nodes_blocking.side_effect = flaky_info
        client.info_blocking.return_value = {"sindex-stat": "entries_per_bval=1.0"}

        monitor = IndexesMonitor(refresh_interval=0.1)
        monitor.start(client)
        monitor.wait_until_ready()
        try:
            time.sleep(0.35)
            ctx = monitor.get_index_context("test")
            assert ctx is not None
        finally:
            monitor.stop()
