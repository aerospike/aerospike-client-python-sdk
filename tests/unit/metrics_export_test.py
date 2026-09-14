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

"""Unit tests for metrics export: composites, the file exporter, node close."""

import logging

from aerospike_sdk.metrics.export import (
    LearnMetricsFileExporter,
    MultipleMetricsExporter,
    NoOpMetricsExporter,
    built_in_exporter,
)
from aerospike_sdk.metrics.export import _NodeCloseTracker
from aerospike_sdk.policy.system_settings import MetricsSettings


class _RecordingExporter:
    def __init__(self, name, raises=False):
        self.name, self.raises, self.calls = name, raises, []

    def on_enable(self, cluster, settings):
        self.calls.append("on_enable")

    def on_snapshot(self, snapshot):
        self.calls.append("on_snapshot")
        if self.raises:
            raise RuntimeError("destination is down")

    def on_node_close(self, host, snapshot):
        self.calls.append(f"on_node_close:{host}")

    def on_disable(self, cluster):
        self.calls.append("on_disable")


class _StubSnapshot:
    def __init__(self, doc):
        self._doc = doc

    def to_canonical_dict(self):
        return self._doc


_DOC = {
    "cluster_name": "c1",
    "client_type": "python",
    "client_version": "9.9.9",
    "app_id": "billing",
    "labels": {"owner": "platform"},
    "latency_columns": 7,
    "latency_shift": 1,
    "nodes": [
        {
            "name": "BB9",
            "address": "10.0.0.1",
            "port": 3000,
            "connections": {"opened": 4, "closed": 1, "open": 3},
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
}


class TestMultipleMetricsExporter:
    """One broken destination must not silence the others."""

    def test_fans_out_in_order(self):
        a, b = _RecordingExporter("a"), _RecordingExporter("b")
        MultipleMetricsExporter(a, b).on_snapshot(_StubSnapshot(_DOC))
        assert a.calls == ["on_snapshot"] and b.calls == ["on_snapshot"]

    def test_a_raising_delegate_does_not_stop_the_rest(self, caplog):
        bad, good = _RecordingExporter("bad", raises=True), _RecordingExporter("good")
        with caplog.at_level(logging.WARNING):
            MultipleMetricsExporter(bad, good).on_snapshot(_StubSnapshot(_DOC))
        assert good.calls == ["on_snapshot"]
        assert "destination is down" in caplog.text or "raised in" in caplog.text

    def test_exporters_are_exposed_in_call_order(self):
        a, b = _RecordingExporter("a"), _RecordingExporter("b")
        assert list(MultipleMetricsExporter(a, b).exporters) == [a, b]


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

    def _write_one(self, tmp_path, limit=0):
        exporter = LearnMetricsFileExporter(str(tmp_path), limit)
        exporter.on_enable(None, None)
        exporter.on_snapshot(_StubSnapshot(_DOC))
        exporter.on_disable(None)
        return sorted(tmp_path.glob("metrics-*.log"))

    def test_header_names_the_fields_it_cannot_emit(self, tmp_path):
        header = self._write_one(tmp_path)[0].read_text().splitlines()[0]
        for field in ("inUse", "inPool", "recoverQueueSize", "commandCount", "retryCount"):
            assert field in header

    def test_cluster_line_carries_identity_and_labels(self, tmp_path):
        line = self._write_one(tmp_path)[0].read_text().splitlines()[1]
        assert "python" in line and "9.9.9" in line
        assert "label[owner=platform]" in line
        assert "c1" in line and "billing" in line

    def test_segment_fields_stay_camel_case(self, tmp_path):
        """The one place the SDK keeps camelCase, for existing log shippers."""
        header = self._write_one(tmp_path)[0].read_text().splitlines()[0]
        assert "keyBusy" in header and "bytesIn" in header
        assert "key_busy" not in header

    def test_latency_segment_reports_the_shape(self, tmp_path):
        line = self._write_one(tmp_path)[0].read_text().splitlines()[1]
        assert "latency(7,1)" in line
        assert "conn[5,0]" in line and "read[1,2]" in line and "write[3,0]" in line

    def test_empty_report_dir_writes_nothing(self, tmp_path):
        exporter = LearnMetricsFileExporter("")
        exporter.on_enable(None, None)
        exporter.on_snapshot(_StubSnapshot(_DOC))
        assert list(tmp_path.iterdir()) == []

    def test_rotates_past_the_size_limit(self, tmp_path):
        exporter = LearnMetricsFileExporter(str(tmp_path), report_size_limit=200)
        exporter.on_enable(None, None)
        for _ in range(4):
            exporter.on_snapshot(_StubSnapshot(_DOC))
        exporter.on_disable(None)
        files = list(tmp_path.glob("metrics-*.log"))
        assert len(files) > 1, "rotation should start a new file"
        # Each file is its header plus at most one line: the limit here is
        # smaller than a single cluster line, so every write rotates.
        assert all(len(f.read_text().splitlines()) <= 2 for f in files)

    def test_rotation_within_one_second_does_not_reuse_a_name(self, tmp_path):
        """The name stamp is per-second, so several rotations can collide in it."""
        exporter = LearnMetricsFileExporter(str(tmp_path), report_size_limit=200)
        exporter.on_enable(None, None)
        for _ in range(3):
            exporter.on_snapshot(_StubSnapshot(_DOC))
        exporter.on_disable(None)
        names = [f.name for f in tmp_path.glob("metrics-*.log")]
        assert len(names) == len(set(names)) > 1


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
        exporter.on_enable(None, None)
        exporter.on_snapshot(_StubSnapshot(doc))
        exporter.on_disable(None)
        text = sorted(tmp_path.glob("metrics-*.log"))[0].read_text()
        assert "usage[" not in text
        assert "feature." not in text

    def test_header_declares_only_the_legacy_segments(self, tmp_path):
        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.on_enable(None, None)
        exporter.on_disable(None)
        header = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[0]
        for segment in ("cluster[", "node[", "namespace[", "latency(", "unavailable["):
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
        exporter.on_enable(None, None)
        exporter.on_snapshot(_StubSnapshot(doc))
        exporter.on_disable(None)
        line = sorted(tmp_path.glob("metrics-*.log"))[0].read_text().splitlines()[1]
        assert "latency(18,1)" in line


class TestBuiltInExporterSelection:
    """What `exporter:` and `report_dir` select."""

    def test_none_installs_a_no_op(self):
        settings = MetricsSettings(exporter="none")
        assert isinstance(built_in_exporter(settings), NoOpMetricsExporter)

    def test_default_without_a_report_dir_leaves_the_exporter_alone(self):
        """The documented default with nowhere to write is a no-op, not an error."""
        assert built_in_exporter(MetricsSettings()) is None

    def test_default_with_a_report_dir_writes_files(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        assert isinstance(built_in_exporter(settings), LearnMetricsFileExporter)

    def test_unknown_name_warns_and_changes_nothing(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = built_in_exporter(MetricsSettings(exporter="prometheus"))
        assert result is None
        assert "prometheus" in caplog.text

    def test_awaitable_wraps_for_the_async_client(self, tmp_path):
        settings = MetricsSettings(report_dir=str(tmp_path))
        wrapped = built_in_exporter(settings, awaitable=True)
        assert isinstance(wrapped.inner, LearnMetricsFileExporter)
