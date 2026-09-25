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

"""Unit tests for metrics export: fan-out, suspension, the file exporter."""

import logging

import pytest

from aerospike_sdk.metrics.export import (
    SUSPEND_AFTER_CONSECUTIVE_FAILURES,
    SUSPENDED_RETRY_EVERY_INTERVALS,
    AsyncMetricsExportTimer,
    LearnMetricsFileExporter,
    SyncMetricsExportTimer,
    built_in_exporter,
    check_exporter,
)
from aerospike_sdk.metrics.export import _NodeCloseTracker
from aerospike_sdk.policy.system_settings import MetricsSettings


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
    """The two attributes the export timer reads: the exporter list, metrics()."""

    def __init__(self, exporters, doc=None):
        self._exporters = exporters
        self._doc = doc if doc is not None else _DOC
        self.polls = 0

    def metrics(self):
        self.polls += 1
        return _StubSnapshot(self._doc)


_DOC = {
    "cluster_name": "c1",
    "client_type": "python",
    "client_version": "9.9.9",
    "app_id": "billing",
    "labels": {"owner": "platform"},
    "latency_columns": 7,
    "latency_shift": 1,
    "cluster": {"command_count": 57, "command_retries": 3},
    "nodes": [
        {
            "name": "BB9",
            "address": "10.0.0.1",
            "port": 3000,
            "connections": {"opened": 4, "closed": 1, "open": 3, "open_failure": 0},
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
    """Every registered exporter receives the same snapshot, in order."""

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


class TestLearnMetricsFileExporter:
    """The legacy line format, including what it cannot fill in."""

    def _write_one(self, tmp_path, limit=0, doc=None):
        exporter = LearnMetricsFileExporter(str(tmp_path), limit)
        exporter.export(_StubSnapshot(doc if doc is not None else _DOC))
        exporter.close()
        return sorted(tmp_path.glob("metrics-*.log"))

    def test_first_export_opens_the_file_and_writes_the_header(self, tmp_path):
        """The file appears on the first export, not at registration."""
        exporter = LearnMetricsFileExporter(str(tmp_path))
        assert list(tmp_path.iterdir()) == []
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        lines = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()
        assert lines[0].startswith("header(3)")
        assert lines[1].startswith("cluster[")

    def test_header_carries_every_format_field(self, tmp_path):
        """Nothing is unavailable any more, so the header names no such segment."""
        header = self._write_one(tmp_path)[0].read_text().splitlines()[0]
        assert "unavailable[" not in header
        assert "retryCount,node[]]" in header

    def test_header_declares_the_cluster_columns_in_the_format_order(self, tmp_path):
        """cpu, mem, recover depth and invalid nodes precede the six usage columns and retryCount."""
        header = self._write_one(tmp_path)[0].read_text().splitlines()[0]
        assert (
            "label[],cpu,mem,recoverQueueSize,invalidNodeCount,singleCount,batchCount,queryCount,"
            "blockingCount,deferredCount,backgroundCount,retryCount" in header
        )
        # The format has no field for the per-call count.
        assert "commandCount" not in header

    def test_cluster_line_writes_the_process_samples(self, tmp_path):
        """cpu is written as a whole percent and mem as bytes, in the header's positions."""
        doc = dict(_DOC, cluster={"command_count": 57, "command_retries": 3,
                                  "cpu_percent": 12.7, "memory_bytes": 1048576,
                                  "recover_queue": {"size": 2}, "nodes": {"active": 1, "invalid": 1}})
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(doc))
        exporter.close()
        line = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[1]
        assert "label[owner=platform],12,1048576,2,1," in line

    def test_conn_segment_uses_the_legacy_layout(self, tmp_path):
        """`conn[inUse,inPool,opened,closed]`, the order existing parsers expect."""
        doc = dict(_DOC, cluster={"command_count": 57, "command_retries": 3,
                                  "recover_queue": {"size": 2}, "nodes": {"active": 1, "invalid": 1}})
        doc["nodes"] = [dict(_DOC["nodes"][0], connections={
            "opened": 4, "closed": 1, "open": 3, "in_use": 1, "in_pool": 2, "open_failure": 0,
        })]
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(doc))
        exporter.close()
        header, line = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[:2]
        assert "conn[inUse,inPool,opened,closed]" in header
        assert "conn[1,2,4,1]" in line
        # cpu and mem (absent here, so 0) then recoverQueueSize and
        # invalidNodeCount sit after the labels.
        assert "label[owner=platform],0,0,2,1," in line

    def test_cluster_line_carries_identity_and_labels(self, tmp_path):
        line = self._write_one(tmp_path)[0].read_text().splitlines()[1]
        assert "python" in line and "9.9.9" in line
        assert "label[owner=platform]" in line
        assert "c1" in line and "billing" in line
        # The labels follow appId, then the usage columns, per the header.
        assert "billing,label[owner=platform]," in line

    def test_header_field_names_stay_camel_case(self, tmp_path):
        """The header declares the format's legacy field names, for existing log shippers."""
        header = self._write_one(tmp_path)[0].read_text().splitlines()[0]
        assert "keyBusy" in header and "bytesIn" in header
        assert "key_busy" not in header

    def test_latency_segment_reports_the_shape(self, tmp_path):
        line = self._write_one(tmp_path)[0].read_text().splitlines()[1]
        assert "latency(7,1)" in line
        assert "conn[5,0]" in line and "read[1,2]" in line and "write[3,0]" in line

    def test_departed_nodes_get_a_final_line(self, tmp_path):
        doc = dict(_DOC, nodes_departed=[
            {
                "name": "BB8",
                "address": "10.0.0.9",
                "port": 3000,
                "connections": {"opened": 9, "closed": 9, "open": 0},
                "namespaces": [],
            }
        ])
        lines = self._write_one(tmp_path, doc=doc)[0].read_text().splitlines()
        final = [ln for ln in lines if ln.startswith("node[")]
        assert len(final) == 1
        assert "BB8" in final[0] and "10.0.0.9" in final[0]

    def test_empty_report_dir_writes_nothing(self, tmp_path):
        exporter = LearnMetricsFileExporter("")
        exporter.export(_StubSnapshot(_DOC))
        assert list(tmp_path.iterdir()) == []

    def test_export_after_close_opens_a_new_file(self, tmp_path):
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        assert len(list(tmp_path.glob("metrics-*.log"))) == 2

    def test_rotates_past_the_size_limit(self, tmp_path):
        exporter = LearnMetricsFileExporter(str(tmp_path), report_size_limit=200)
        for _ in range(4):
            exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        files = list(tmp_path.glob("metrics-*.log"))
        assert len(files) > 1, "rotation should start a new file"
        # Each file is its header plus at most one line: the limit here is
        # smaller than a single cluster line, so every write rotates.
        assert all(len(f.read_text().splitlines()) <= 2 for f in files)

    def test_rotation_within_one_second_does_not_reuse_a_name(self, tmp_path):
        """The name stamp is per-second, so several rotations can collide in it."""
        exporter = LearnMetricsFileExporter(str(tmp_path), report_size_limit=200)
        for _ in range(3):
            exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        names = [f.name for f in tmp_path.glob("metrics-*.log")]
        assert len(names) == len(set(names)) > 1

    def test_an_unwritable_directory_raises_to_the_timer(self, tmp_path):
        """IO failures propagate; the export timer's suspension handles them."""
        blocker = tmp_path / "not-a-dir"
        blocker.write_text("")
        exporter = LearnMetricsFileExporter(str(blocker / "reports"))
        with pytest.raises(OSError):
            exporter.export(_StubSnapshot(_DOC))


class TestLegacyFormatIsNotExtended:
    """The file format is externally defined; we do not add fields to it."""

    def test_no_usage_segment_even_when_counters_are_on(self, tmp_path):
        """Usage counters reach consumers via the snapshot, not this file.

        The legacy format's field list has no place for them, and its readers
        are tools this SDK does not control, so inventing a segment would make
        the file non-interoperable with the other clients that read and write
        it. The cross-SDK spec routes anything outside the legacy field list
        through the canonical snapshot.
        """
        doc = dict(_DOC, usage={"feature.shape.point": 900})
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(doc))
        exporter.close()
        text = sorted(tmp_path.glob("metrics-*.log"))[0].read_text()
        assert "usage[" not in text
        assert "feature." not in text

    def test_header_declares_only_the_legacy_segments(self, tmp_path):
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(_DOC))
        exporter.close()
        header = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[0]
        for segment in ("cluster[", "node[", "namespace[", "latency("):
            assert segment in header
        assert "usage[" not in header


class TestLatencyShapeIsAlwaysWritten:
    """The (columns,shift) pair goes in whatever the unit is."""

    def test_shape_is_written_for_microsecond_buckets(self, tmp_path):
        """Omitting it would lose the shape as well as the unit.

        The legacy format has no unit field, so a microsecond histogram is
        indistinguishable from a millisecond one to a legacy reader. Writing
        the pair anyway keeps the shape recoverable; the ambiguity is
        documented rather than papered over by dropping data.
        """
        doc = dict(_DOC, latency_unit="microseconds", latency_columns=18, latency_shift=1)
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.export(_StubSnapshot(doc))
        exporter.close()
        line = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[1]
        assert "latency(18,1)" in line


class TestBuiltInExporterSelection:
    """What `exporter:` and `report_dir` select."""

    def test_none_installs_nothing(self):
        assert built_in_exporter(MetricsSettings(exporter="none")) is None

    def test_default_without_a_report_dir_installs_nothing(self):
        """The documented default with nowhere to write is a no-op, not an error."""
        assert built_in_exporter(MetricsSettings()) is None

    def test_default_with_a_report_dir_writes_files(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        assert isinstance(built_in_exporter(settings), LearnMetricsFileExporter)

    def test_unknown_name_warns_and_installs_nothing(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = built_in_exporter(MetricsSettings(exporter="prometheus"))
        assert result is None
        assert "prometheus" in caplog.text

    def test_awaitable_wraps_for_the_async_client(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        wrapped = built_in_exporter(settings, awaitable=True)
        assert isinstance(wrapped.inner, LearnMetricsFileExporter)

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

        class Cluster:
            def __init__(self):
                self._exporters = [Bad(), Good()]

            async def metrics(self):
                return _StubSnapshot(_DOC)

        timer = AsyncMetricsExportTimer(Cluster(), 3600.0)
        with caplog.at_level(logging.WARNING):
            await timer._export_once()
        assert len(received) == 1 and len(failures) == 1
        assert "raised" in caplog.text
