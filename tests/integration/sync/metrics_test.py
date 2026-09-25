# Copyright 2025-2026 Aerospike, Inc.
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

"""Sync integration tests for cluster metrics (enable, snapshot, derived views)."""

import os

import pytest

from aerospike_sdk.metrics import CommandType, LatencyType, LatencyUnit, MetricsPolicy, Sampler
from aerospike_sdk.dataset import DataSet
from aerospike_sdk import UDFLang
from aerospike_sdk.sync import ClusterDefinition

from tests.integration.general_auth import apply_general_auth, general_auth_enabled
from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
def metrics_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped cluster so metrics state isn't shared with other suites."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


# Same shape as the core's construction default; see the async twin for why.
_SHAPE_SAFE = MetricsPolicy(
    operational_enabled=True,
    latency_unit=LatencyUnit.MICROSECONDS,
    latency_columns=24,
)


def _do_some_ops(cluster, count):
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), "sdk_metrics")
    for i in range(count):
        session.upsert(ds.id(i)).put({"n": i}).execute()
        first = session.query(ds.id(i)).execute().first_or_raise()
        assert first.is_ok


class TestSyncMetrics:

    def test_enable_disable_round_trip(self, metrics_cluster):
        assert metrics_cluster.metrics_enabled() is False
        metrics_cluster.enable_metrics()
        assert metrics_cluster.metrics_enabled() is True
        metrics_cluster.disable_metrics()
        assert metrics_cluster.metrics_enabled() is False

    def test_snapshot_and_derived_views(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        _do_some_ops(metrics_cluster, count=5)

        snapshot = metrics_cluster.metrics()
        assert snapshot.total_nodes >= 1
        assert snapshot.open_connections >= 1

        agg = snapshot.cluster_aggregated
        assert agg.latency_unit == LatencyUnit.MICROSECONDS

        reads = snapshot.latency(LatencyType.READ)
        writes = snapshot.latency(LatencyType.WRITE)
        assert reads.count >= 5
        assert writes.count >= 5
        assert sum(reads.buckets) == reads.count

        assert general_namespace() in agg.detailed_namespaces()
        detail = agg.detailed_metric(general_namespace(), CommandType.GET)
        assert detail is not None
        assert detail.latency.count >= 5

        metrics_cluster.disable_metrics()

    def test_sampler_never_gates_command_metrics(self, metrics_cluster):
        policy = MetricsPolicy(
            operational_enabled=True,
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=24,
            sampler=Sampler.never(),
        )
        before = metrics_cluster.metrics().latency(LatencyType.READ).count
        metrics_cluster.enable_metrics(policy)
        _do_some_ops(metrics_cluster, count=3)

        assert metrics_cluster.metrics().latency(LatencyType.READ).count == before
        metrics_cluster.disable_metrics()

    def test_to_dict_stable_names(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        _do_some_ops(metrics_cluster, count=1)

        d = metrics_cluster.metrics().to_dict()
        assert d["total_nodes"] >= 1
        assert d["cluster_aggregated_metrics"]["latency_unit"] == "us"
        metrics_cluster.disable_metrics()

    def test_usage_counters_record_and_gate(self, metrics_cluster):
        """Usage counters are recorded on the sync surface, and only when asked."""
        metrics_cluster.enable_metrics(MetricsPolicy())          # usage off
        _do_some_ops(metrics_cluster, count=3)
        assert metrics_cluster.metrics().usage == {}

        metrics_cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
        _do_some_ops(metrics_cluster, count=3)
        usage = metrics_cluster.metrics().usage
        assert usage.get("feature.api.blocking", 0) >= 3, usage
        assert usage.get("feature.shape.point", 0) >= 3, usage
        metrics_cluster.disable_metrics()

    def test_command_count_is_kept_without_the_usage_group(
        self, aerospike_host, make_cluster_definition,
    ):
        """The call count needs no opt-in, and counts a batch once."""
        # Own cluster: the count is cumulative for the client lifetime, so a
        # shared fixture could not be asserted against exact numbers.
        with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
            cluster.enable_metrics(MetricsPolicy())              # usage off
            session = cluster.create_session()
            ds = DataSet.of(general_namespace(), "sdk_metrics_cc")
            for i in range(4):
                session.upsert(ds.id(i)).put({"n": i}).execute()

            snapshot = cluster.metrics()
            assert snapshot.usage == {}, "usage was off"
            assert snapshot.command_count == 4

            # One call, four keys: the count follows calls, not records.
            session.query(ds.id(0), ds.id(1), ds.id(2), ds.id(3)).execute().collect()
            assert cluster.metrics().command_count == 5
            assert cluster.metrics().to_canonical_dict()["cluster"]["command_count"] == 5

    def test_command_count_stops_with_collection(
        self, aerospike_host, make_cluster_definition,
    ):
        """Disabling metrics freezes the count; traffic after it is not added."""
        with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
            cluster.enable_metrics(MetricsPolicy())
            session = cluster.create_session()
            ds = DataSet.of(general_namespace(), "sdk_metrics_cc_off")
            session.upsert(ds.id("a")).put({"n": 1}).execute()
            cluster.disable_metrics()

            frozen = cluster.metrics().command_count
            assert frozen == 1
            session.upsert(ds.id("b")).put({"n": 2}).execute()
            assert cluster.metrics().command_count == frozen

    def test_usage_counters_record_background(
        self, aerospike_host, make_cluster_definition,
    ):
        """A background job registration counts as background, not blocking."""
        # Own cluster: usage totals are cumulative for the client lifetime,
        # and the module-scoped fixture is asserted empty when usage is off.
        with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
            cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
            session = cluster.create_session()
            ds = DataSet.of(general_namespace(), "sdk_metrics_bg")
            session.upsert(ds.id("k")).put({"n": 1}).execute()
            task = (
                session.background_task()
                .update(ds)
                .bin("n").add(1)
                .execute()
            )
            task.wait_till_complete_blocking()
            snapshot = cluster.metrics()
            counts = snapshot.usage
            assert counts.get("feature.api.background", 0) >= 1, counts
            assert counts.get("feature.shape.query", 0) >= 1, counts
            assert counts.get("feature.background.operate", 0) >= 1, counts
            # The upsert and the registration, once each: the registration
            # takes its own dispatch path and must not also be counted as a
            # normal execute.
            assert snapshot.command_count == 2

    def test_record_udf_call_is_counted(self, aerospike_host, make_cluster_definition):
        """A record UDF call counts like any other blocking point call, plus its feature."""
        lua = os.path.join(os.path.dirname(__file__), "..", "udf", "record_example.lua")
        with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
            cluster.register_udf_from_file(
                os.path.normpath(lua), "record_example.lua", UDFLang.LUA
            ).wait_till_complete_blocking(sleep_time=0.2, timeout=10.0)
            cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
            session = cluster.create_session()
            key = DataSet.of(general_namespace(), "sdk_metrics_udf").id("k")
            session.upsert(key).put({"n": 1}).execute()
            session.execute_udf(key).function("record_example", "readBin").passing("n").execute()
            snapshot = cluster.metrics()
            counts = snapshot.usage
            assert counts.get("feature.udf.record", 0) == 1, counts
            assert counts.get("feature.api.blocking", 0) == 2, counts
            assert counts.get("feature.shape.point", 0) == 2, counts
            # The upsert and the UDF call; the registration is not a command.
            assert snapshot.command_count == 2

    def test_app_id_defaults_without_being_declared(
        self, aerospike_host, make_cluster_definition,
    ):
        """An application that named none is still attributable."""
        with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
            cluster.enable_metrics(MetricsPolicy())
            app_id = cluster.metrics().to_canonical_dict()["app_id"]
        expected = os.environ.get("AEROSPIKE_AUTH_USER", "") if general_auth_enabled() else ""
        assert app_id == (expected or "not-set")

    def test_declared_app_id_is_reported(self, aerospike_host):
        """The name the application chose wins over the default."""
        host, _, port = aerospike_host.rpartition(":")
        definition = apply_general_auth(
            ClusterDefinition(host, int(port)),
        ).app_id("sdk-metrics-itest")
        with definition.connect() as cluster:
            cluster.enable_metrics(MetricsPolicy())
            assert cluster.metrics().to_canonical_dict()["app_id"] == "sdk-metrics-itest"

    def test_exporter_receives_pushed_snapshots(self, metrics_cluster):
        received = []

        class Recording:
            def export(self, snapshot):
                received.append(snapshot)

        recording = Recording()
        metrics_cluster.add_exporter(recording)
        try:
            metrics_cluster.enable_metrics(_SHAPE_SAFE)
            metrics_cluster._export_timer._export_once()
            assert len(received) == 1
            doc = received[0].to_canonical_dict()
            assert doc["client_type"] == "python"
            assert doc["nodes_departed"] == []
        finally:
            metrics_cluster.disable_metrics()
            metrics_cluster.remove_exporter(recording)

    def test_mismatched_exporter_protocol_is_rejected(self, metrics_cluster):
        """An async exporter on the sync cluster fails at registration."""

        class AsyncShaped:
            async def export(self, snapshot): ...

        with pytest.raises(TypeError, match="MetricsExporter"):
            metrics_cluster.add_exporter(AsyncShaped())



class TestCommandCount:
    """The SDK-counted cluster command total: one per data-path API call."""

    def test_counts_once_per_user_call(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "cmd_count")
        base = metrics_cluster.metrics().command_count

        for i in range(3):
            session.upsert(ds.id(i)).put({"n": i}).execute()
        single = session.query(ds.id(0)).execute()
        assert single.first_or_raise().is_ok
        batch = session.query(ds.id(0), ds.id(1), ds.id(2)).execute()
        assert len(batch.collect()) == 3

        after = metrics_cluster.metrics().command_count
        # Three writes, one point read, one batch read: the batch is one
        # call however many keys it carries.
        assert after - base == 5

        doc = metrics_cluster.metrics().to_canonical_dict()
        assert doc["cluster"]["command_count"] == after
        # Derived from the per-node counters the client core keeps.
        assert doc["cluster"]["command_retries"] >= 0
        assert "closed_idle" in doc["nodes"][0]["connections"]
        metrics_cluster.disable_metrics()

    def test_disabled_metrics_freeze_the_count(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "cmd_count_off")
        session.upsert(ds.id(1)).put({"n": 1}).execute()
        metrics_cluster.disable_metrics()

        frozen = metrics_cluster.metrics().command_count
        session.upsert(ds.id(2)).put({"n": 2}).execute()
        assert metrics_cluster.metrics().command_count == frozen
