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

import pytest

from aerospike_sdk import CommandType, LatencyType, LatencyUnit, MetricsPolicy, Sampler
from aerospike_sdk.dataset import DataSet

from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
def metrics_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped cluster so metrics state isn't shared with other suites."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


# Same shape as the core's construction default; see the async twin for why.
_SHAPE_SAFE = MetricsPolicy(latency_unit=LatencyUnit.MICROSECONDS, latency_columns=24)


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
        assert d["total-nodes"] >= 1
        assert d["cluster-aggregated-metrics"]["latency-unit"] == "us"
        metrics_cluster.disable_metrics()
