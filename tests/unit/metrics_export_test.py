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

"""Unit tests for metrics export: fan-out, suspension, selection, the file exporter."""

import asyncio
import logging
import re
from types import SimpleNamespace

import pytest

from aerospike_sdk.cluster_shared import ClusterBase
from aerospike_sdk.metrics.export import (
    SUSPEND_AFTER_CONSECUTIVE_FAILURES,
    SUSPENDED_RETRY_EVERY_INTERVALS,
    AsyncMetricsExportTimer,
    MetricsWriter,
    MetricsExporterType,
    SyncMetricsExportTimer,
    built_in_exporter,
    check_exporter,
    export_mode,
)
from aerospike_sdk.metrics.export import _NodeCloseTracker
from aerospike_sdk.policy.system_settings import MetricsSettings

# A timestamp, then one space, starts every line.
_STAMP = r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} "


class _RecordingExporter:
    def __init__(self, name, raises=False):
        self.name, self.raises, self.calls = name, raises, []

    def export(self, snapshot):
        self.calls.append("export")
        if self.raises:
            raise RuntimeError("destination is down")


class _StubSnapshot:
    def __init__(self, doc):
        self._doc = doc

    def to_canonical_dict(self):
        return self._doc

    def _mark_departed(self, tracker):
        pass


class _StubCluster:
    """What the export timer reads: this interval's targets and a snapshot."""

    def __init__(self, exporters, doc=None):
        self._exporters = exporters
        self._doc = doc if doc is not None else _DOC
        self.polls = 0

    def _export_targets(self):
        return list(self._exporters)

    def metrics_snapshot(self):
        self.polls += 1
        return _StubSnapshot(self._doc)


_DOC = {
    "timestamp": "2026-10-09T17:00:00.123000+00:00",
    "cluster_name": "c1",
    "client_type": "python-sdk",
    "client_version": "9.9.9",
    "app_id": "billing",
    "labels": {"owner": "platform"},
    "latency_unit": "milliseconds",
    "latency_columns": 2,
    "latency_shift": 1,
    "cluster": {"command_count": 57, "command_retries": 3},
    "nodes": [
        {
            "name": "BB9",
            "address": "10.0.0.1",
            "port": 3000,
            "connections": {"opened": 4, "closed": 1, "in_use": 1, "in_pool": 2},
            "namespaces": [
                {
                    "name": "test",
                    "errors": 2, "timeouts": 1, "key_busy": 0,
                    "bytes_in": 10, "bytes_out": 20,
                    "latency": {"read": [1, 2], "write": [3, 0], "conn": [5, 0]},
                }
            ],
        }
    ],
    "nodes_departed": [],
}


def _export_once(cluster):
    """Drive one export cycle through the real sync timer."""
    timer = SyncMetricsExportTimer(cluster, 3600.0)
    timer._export_once()
    return timer


class TestExportFanOut:
    """Every selected exporter receives the same snapshot, in order."""

    def test_all_exporters_receive_the_snapshot(self):
        a, b = _RecordingExporter("a"), _RecordingExporter("b")
        _export_once(_StubCluster([a, b]))
        assert a.calls == ["export"] and b.calls == ["export"]

    def test_a_raising_exporter_does_not_stop_the_rest(self, caplog):
        bad, good = _RecordingExporter("bad", raises=True), _RecordingExporter("good")
        with caplog.at_level(logging.WARNING):
            _export_once(_StubCluster([bad, good]))
        assert good.calls == ["export"]
        assert "raised" in caplog.text

    def test_no_exporters_takes_no_snapshot(self):
        """Snapshotting drains client-core state; skip it when nobody consumes."""
        cluster = _StubCluster([])
        _export_once(cluster)
        assert cluster.polls == 0

    def test_registration_after_start_takes_effect(self):
        """The list is read per cycle, not captured at timer start."""
        exporters = []
        cluster = _StubCluster(exporters)
        timer = SyncMetricsExportTimer(cluster, 3600.0)
        timer._export_once()
        late = _RecordingExporter("late")
        exporters.append(late)
        timer._export_once()
        assert late.calls == ["export"]


class TestExporterSuspension:
    """A persistently failing exporter is parked, retried, and can recover."""

    def _run_cycles(self, timer, count):
        for _ in range(count):
            timer._export_once()

    def test_suspended_after_consecutive_failures(self, caplog):
        bad = _RecordingExporter("bad", raises=True)
        timer = SyncMetricsExportTimer(_StubCluster([bad]), 3600.0)
        with caplog.at_level(logging.WARNING):
            self._run_cycles(timer, SUSPEND_AFTER_CONSECUTIVE_FAILURES + 2)
        # Called for each failure up to the threshold, then skipped.
        assert len(bad.calls) == SUSPEND_AFTER_CONSECUTIVE_FAILURES
        assert "suspending" in caplog.text

    def test_suspended_exporter_is_retried_periodically(self):
        bad = _RecordingExporter("bad", raises=True)
        timer = SyncMetricsExportTimer(_StubCluster([bad]), 3600.0)
        self._run_cycles(
            timer, SUSPEND_AFTER_CONSECUTIVE_FAILURES + SUSPENDED_RETRY_EVERY_INTERVALS
        )
        assert len(bad.calls) == SUSPEND_AFTER_CONSECUTIVE_FAILURES + 1

    def test_success_on_retry_resumes_the_normal_cadence(self, caplog):
        bad = _RecordingExporter("bad", raises=True)
        timer = SyncMetricsExportTimer(_StubCluster([bad]), 3600.0)
        self._run_cycles(timer, SUSPEND_AFTER_CONSECUTIVE_FAILURES)
        bad.raises = False
        with caplog.at_level(logging.INFO, logger="aerospike_sdk.behavior"):
            self._run_cycles(timer, SUSPENDED_RETRY_EVERY_INTERVALS + 3)
        # One successful retry, then every cycle again.
        assert len(bad.calls) == SUSPEND_AFTER_CONSECUTIVE_FAILURES + 1 + 3
        assert "recovered" in caplog.text

    def test_one_suspended_exporter_does_not_park_the_others(self):
        bad, good = _RecordingExporter("bad", raises=True), _RecordingExporter("good")
        timer = SyncMetricsExportTimer(_StubCluster([bad, good]), 3600.0)
        cycles = SUSPEND_AFTER_CONSECUTIVE_FAILURES + 4
        self._run_cycles(timer, cycles)
        assert len(good.calls) == cycles

    def test_a_single_failure_does_not_suspend(self):
        flaky = _RecordingExporter("flaky", raises=True)
        timer = SyncMetricsExportTimer(_StubCluster([flaky]), 3600.0)
        timer._export_once()
        flaky.raises = False
        timer._export_once()
        timer._export_once()
        assert len(flaky.calls) == 3


class TestNodeCloseTracker:
    """Departure is judged against the live node list, never a snapshot diff."""

    def test_reports_a_host_missing_from_the_cluster(self):
        tracker = _NodeCloseTracker()
        assert tracker.departed(["a:1", "b:2"], ["a:1"]) == ["b:2"]

    def test_fires_once_per_host(self):
        tracker = _NodeCloseTracker()
        tracker.departed(["a:1", "b:2"], ["a:1"])
        assert tracker.departed(["a:1", "b:2"], ["a:1"]) == []

    def test_a_host_still_in_the_cluster_never_fires(self):
        """The snapshot keeps every host it ever saw, so only the live list shrinks."""
        tracker = _NodeCloseTracker()
        assert tracker.departed(["a:1"], ["a:1"]) == []


class TestMetricsWriter:
    """The line format, field for field."""

    def _lines(self, tmp_path, doc=None, limit=0):
        exporter = MetricsWriter(str(tmp_path), limit)
        exporter.export(_StubSnapshot(doc if doc is not None else _DOC))
        exporter.close()
        return sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()

    def test_first_export_opens_the_file_and_writes_the_header(self, tmp_path):
        """The file appears on the first export, not at registration."""
        exporter = MetricsWriter(str(tmp_path))
        assert list(tmp_path.iterdir()) == []
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        lines = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()
        assert re.match(_STAMP + r"header\(5\) ", lines[0])
        assert re.match(_STAMP + r"cluster\[", lines[1])

    def test_header_is_the_shared_schema(self, tmp_path):
        header = self._lines(tmp_path)[0].split(" ", 2)[2]
        assert header == (
            "header(5)"
            " cluster[cluster_name,client_type,client_version,app_id,labels[],cpu,mem,"
            "recover_queue_size,nodes_invalid,command_count,blocking_count,deferred_count,"
            "background_count,tran_count,command_retries,nodes[]]"
            " labels[name,value]"
            " nodes[name,address,port,conns_in_use,conns_in_pool,conns_opened,conns_closed,"
            "namespaces[]]"
            " namespaces[name,errors,timeouts,key_busy,bytes_in,bytes_out,latency[]]"
            " latency(MILLISECONDS,2,1)[type[l1,l2,l3...]]"
        )

    def test_cluster_line_is_positional_in_header_order(self, tmp_path):
        doc = dict(
            _DOC,
            cluster={
                "command_count": 57, "command_retries": 3,
                "cpu_percent": 12.5, "memory_bytes": 1048576,
                "recover_queue": {"size": 2}, "nodes": {"active": 1, "invalid": 1},
            },
            usage={
                "feature.api.blocking": 40, "feature.api.deferred": 7,
                "feature.api.background": 2, "feature.transaction": 5,
                "feature.shape.point": 900,
            },
        )
        line = self._lines(tmp_path, doc)[1].split(" ", 2)[2]
        assert line == (
            "cluster[c1,python-sdk,9.9.9,billing,[[owner,platform]],12.5,1048576,2,1,"
            "57,40,7,2,5,3,"
            "[[BB9,10.0.0.1,3000,1,2,4,1,"
            "[test,2,1,0,10,20,[conn[5,0],write[3,0],read[1,2],batch[0,0],query[0,0]]]]]]"
        )

    def test_usage_columns_read_zero_when_usage_is_off(self, tmp_path):
        line = self._lines(tmp_path)[1]
        assert ",9.9.9,billing,[[owner,platform]],0.0,0,0,0,57,0,0,0,0,3,[" in line

    def test_only_the_api_and_transaction_counters_reach_the_file(self, tmp_path):
        """The other feature counters stay in the structured snapshot."""
        text = "\n".join(self._lines(tmp_path, dict(_DOC, usage={"feature.shape.point": 900})))
        assert "900" not in text
        assert "feature." not in text

    def test_no_labels_write_an_empty_list(self, tmp_path):
        line = self._lines(tmp_path, dict(_DOC, labels={}))[1]
        assert ",billing,[],0.0," in line

    def test_departed_nodes_follow_the_live_ones_in_the_node_list(self, tmp_path):
        departed = {
            "name": "BB8", "address": "10.0.0.9", "port": 3000,
            "connections": {"opened": 9, "closed": 9, "in_use": 0, "in_pool": 0},
            "namespaces": [],
        }
        lines = self._lines(tmp_path, dict(_DOC, nodes_departed=[departed]))
        assert len(lines) == 2, "no separate line per departed node"
        assert lines[1].endswith(",[BB8,10.0.0.9,3000,0,0,9,9,[]]]")

    def test_field_names_are_snake_case(self, tmp_path):
        header = self._lines(tmp_path)[0]
        assert not re.search(r"[a-z][A-Z]", header), header

    def test_line_time_is_the_snapshot_time(self, tmp_path):
        line = self._lines(tmp_path)[1]
        assert re.match(_STAMP, line)
        assert line[20:23] == "123"

    def test_microsecond_buckets_are_named_in_the_header(self, tmp_path):
        doc = dict(_DOC, latency_unit="microseconds", latency_columns=18)
        assert "latency(MICROSECONDS,18,1)" in self._lines(tmp_path, doc)[0]

    def test_a_changed_shape_starts_a_new_file(self, tmp_path):
        """A header describes one histogram shape; a re-enabled policy gets its own file."""
        exporter = MetricsWriter(str(tmp_path))
        exporter.export(_StubSnapshot(_DOC))
        exporter.export(_StubSnapshot(dict(_DOC, latency_columns=9)))
        exporter.close()
        headers = sorted(f.read_text().splitlines()[0] for f in tmp_path.glob("metrics-*.log"))
        assert len(headers) == 2
        assert any("latency(MILLISECONDS,9,1)" in h for h in headers)

    def test_empty_report_dir_writes_nothing(self, tmp_path):
        exporter = MetricsWriter("")
        exporter.export(_StubSnapshot(_DOC))
        assert list(tmp_path.iterdir()) == []

    def test_export_after_close_opens_a_new_file(self, tmp_path):
        exporter = MetricsWriter(str(tmp_path))
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        assert len(list(tmp_path.glob("metrics-*.log"))) == 2

    def test_a_size_limit_under_the_minimum_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="report_size_limit"):
            MetricsWriter(str(tmp_path), report_size_limit=200)

    def test_rotates_past_the_size_limit(self, tmp_path):
        exporter = MetricsWriter(str(tmp_path), report_size_limit=1_000_000)
        exporter._limit = 200       # below the minimum, so every write rotates
        for _ in range(3):
            exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        files = list(tmp_path.glob("metrics-*.log"))
        names = [f.name for f in files]
        # Rotation follows the write that crosses the limit, so three writes
        # leave three full files and a fresh one holding just its header.
        # They land in the same second; each still gets its own name.
        assert len(names) == len(set(names)) == 4
        assert sorted(len(f.read_text().splitlines()) for f in files) == [1, 2, 2, 2]

    def test_an_unwritable_directory_raises_to_the_timer(self, tmp_path):
        """IO failures propagate; the export timer's suspension handles them."""
        blocker = tmp_path / "not-a-dir"
        blocker.write_text("")
        exporter = MetricsWriter(str(blocker / "reports"))
        with pytest.raises(OSError):
            exporter.export(_StubSnapshot(_DOC))


class TestExportMode:
    """`metrics.exporter`, and what unset resolves to."""

    def test_unset_is_file(self):
        assert export_mode(MetricsSettings()) is MetricsExporterType.FILE
        assert export_mode(None) is MetricsExporterType.FILE

    def test_strings_in_any_case_name_the_member(self):
        assert MetricsExporterType("FILE") is MetricsExporterType.FILE
        assert MetricsSettings(exporter="Custom").exporter is MetricsExporterType.CUSTOM
        assert export_mode(MetricsSettings(exporter="none")) is MetricsExporterType.NONE
        # The member reads as its configuration-file spelling.
        assert str(MetricsExporterType.FILE) == "file"

    def test_unknown_names_are_rejected(self):
        with pytest.raises(ValueError, match="exporter"):
            MetricsSettings(exporter="learn_metrics_file")


class TestExportTargets:
    """Which exporters a cluster's interval actually calls."""

    def _cluster(self, exporter=None, registered=()):
        built_in = _RecordingExporter("built-in")
        cluster = SimpleNamespace(
            _sdk_client=SimpleNamespace(
                _sdk_settings=SimpleNamespace(metrics=MetricsSettings(exporter=exporter)),
            ),
            _exporters=list(registered),
            _installed_exporter=built_in,
        )
        return cluster, built_in

    def test_file_calls_only_the_built_in(self):
        app = _RecordingExporter("app")
        cluster, built_in = self._cluster("file", [app])
        assert ClusterBase._export_targets(cluster) == [built_in]

    def test_custom_calls_only_the_registered_list(self):
        app = _RecordingExporter("app")
        cluster, _ = self._cluster("custom", [app])
        assert ClusterBase._export_targets(cluster) == [app]

    def test_none_calls_nobody(self):
        cluster, _ = self._cluster("none", [_RecordingExporter("app")])
        assert ClusterBase._export_targets(cluster) == []

    def test_unset_is_file_whatever_is_registered(self):
        """Registering alone does not switch the mode; `custom` must be selected."""
        cluster, built_in = self._cluster(registered=[_RecordingExporter("app")])
        assert ClusterBase._export_targets(cluster) == [built_in]


class TestBuiltInExporter:
    """When the configuration installs the log writer."""

    def test_none_and_custom_install_nothing(self, tmp_path):
        for mode in ("none", "custom"):
            settings = MetricsSettings(exporter=mode, report_dir=str(tmp_path))
            assert built_in_exporter(settings) is None

    def test_default_without_a_report_dir_installs_nothing(self):
        """The default with nowhere to write is a no-op, not an error."""
        assert built_in_exporter(MetricsSettings()) is None

    def test_file_or_unset_with_a_report_dir_writes_files(self, tmp_path):
        for mode in (None, "file"):
            settings = MetricsSettings(exporter=mode, report_dir=str(tmp_path))
            assert isinstance(built_in_exporter(settings), MetricsWriter)

    def test_file_by_name_without_a_report_dir_warns(self, caplog):
        """Asking for the file and giving it nowhere to go is worth a line in the log."""
        with caplog.at_level(logging.WARNING):
            assert built_in_exporter(MetricsSettings(exporter="file")) is None
        assert "report_dir" in caplog.text
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert built_in_exporter(MetricsSettings()) is None
        assert "report_dir" not in caplog.text

    def test_awaitable_wraps_for_the_async_client(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        wrapped = built_in_exporter(settings, awaitable=True)
        assert isinstance(wrapped.inner, MetricsWriter)

    async def test_the_wrapped_built_in_exports_off_loop(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        wrapped = built_in_exporter(settings, awaitable=True)
        await wrapped.export(_StubSnapshot(_DOC))
        wrapped.close()
        assert len(list(tmp_path.glob("metrics-*.log"))) == 1


class TestCheckExporter:
    """Protocol mismatches fail at registration, not silently at export time."""

    def test_missing_export_method_is_rejected(self):
        with pytest.raises(TypeError, match="missing export"):
            check_exporter(object(), awaitable=False)

    def test_sync_exporter_rejected_by_the_async_cluster(self):
        with pytest.raises(TypeError, match="AsyncMetricsExporter"):
            check_exporter(_RecordingExporter("sync"), awaitable=True)

    def test_async_exporter_rejected_by_the_sync_cluster(self):
        class AsyncShaped:
            async def export(self, snapshot): ...

        with pytest.raises(TypeError, match="MetricsExporter"):
            check_exporter(AsyncShaped(), awaitable=False)

    def test_matching_protocols_pass(self):
        class AsyncShaped:
            async def export(self, snapshot): ...

        check_exporter(_RecordingExporter("sync"), awaitable=False)
        check_exporter(AsyncShaped(), awaitable=True)


class TestAsyncExportTimer:
    """The async twin of the fan-out loop."""

    async def test_fans_out_and_isolates_failures(self, caplog):
        received, failures = [], []

        class Good:
            async def export(self, snapshot):
                received.append(snapshot)

        class Bad:
            async def export(self, snapshot):
                failures.append(1)
                raise RuntimeError("destination is down")

        timer = AsyncMetricsExportTimer(_StubCluster([Bad(), Good()]), 3600.0)
        with caplog.at_level(logging.WARNING):
            await timer._export_once()
        assert len(received) == 1 and len(failures) == 1
        assert "raised" in caplog.text


class _AsyncRecordingExporter:
    def __init__(self):
        self.calls = 0

    async def export(self, snapshot):
        self.calls += 1


class _FailingSnapshotCluster(_StubCluster):
    def metrics_snapshot(self):
        raise RuntimeError("client core is gone")


class TestFinalExport:
    """Stopping the timer for good pushes one last snapshot; restarting it does not."""

    def test_sync_final_stop_pushes_one_snapshot(self):
        exporter = _RecordingExporter("a")
        timer = SyncMetricsExportTimer(_StubCluster([exporter]), 3600.0)
        timer.start()
        timer.stop(final=True)
        assert exporter.calls == ["export"]

    def test_sync_plain_stop_pushes_nothing(self):
        exporter = _RecordingExporter("a")
        timer = SyncMetricsExportTimer(_StubCluster([exporter]), 3600.0)
        timer.start()
        timer.stop()
        assert exporter.calls == []

    def test_a_failing_final_snapshot_is_logged_not_raised(self, caplog):
        timer = SyncMetricsExportTimer(
            _FailingSnapshotCluster([_RecordingExporter("a")]), 3600.0,
        )
        timer.start()
        with caplog.at_level(logging.WARNING):
            timer.stop(final=True)
        assert "Final metrics snapshot failed" in caplog.text

    async def test_async_request_stop_snapshots_now_and_delivers_later(self):
        exporter = _AsyncRecordingExporter()
        cluster = _StubCluster([exporter])
        timer = AsyncMetricsExportTimer(cluster, 3600.0)
        timer.start()
        delivery = timer.request_stop(final=True)
        # Taken before the caller switches collection off, not when delivered.
        assert cluster.polls == 1 and exporter.calls == 0
        await delivery
        assert exporter.calls == 1

    async def test_async_plain_request_stop_pushes_nothing(self):
        exporter = _AsyncRecordingExporter()
        cluster = _StubCluster([exporter])
        timer = AsyncMetricsExportTimer(cluster, 3600.0)
        timer.start()
        assert timer.request_stop() is None
        assert cluster.polls == 0

    async def test_async_final_stop_delivers_before_returning(self):
        exporter = _AsyncRecordingExporter()
        timer = AsyncMetricsExportTimer(_StubCluster([exporter]), 3600.0)
        timer.start()
        await timer.stop(final=True)
        assert exporter.calls == 1

    async def test_a_new_timer_waits_for_the_pending_final_push(self):
        exporter = _AsyncRecordingExporter()
        pending = asyncio.get_running_loop().create_future()
        timer = AsyncMetricsExportTimer(_StubCluster([exporter]), 0.0, after=pending)
        timer.start()
        try:
            for _ in range(5):
                await asyncio.sleep(0)
            assert exporter.calls == 0
            pending.set_result(None)
            for _ in range(5):
                await asyncio.sleep(0)
            assert exporter.calls > 0
        finally:
            await timer.stop()
