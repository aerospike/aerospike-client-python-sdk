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

"""Unit tests for the feature-usage counter helpers."""

import threading
from types import SimpleNamespace

from aerospike_async import (
    HllOperation,
    ListOperation,
    ListPolicy,
    MapOperation,
    MapPolicy,
    Operation,
    StringOperation,
    Txn,
)

from aerospike_sdk.metrics import usage
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.operations.query import SyncQueryBuilder


class _Recorder:
    """Stands in for the SDK client, capturing what the call sites record."""

    supports_server_compiled_ael = False
    supports_query_selection = False

    def __init__(self, usage_on=True):
        self._usage_on = usage_on
        self.calls = []
        self._usage_counters = _CapturingCounters(self.calls)


class _CapturingCounters:
    """A UsageCounters stand-in that records each flushed feature set."""

    def __init__(self, calls):
        self._calls = calls

    def add(self, features):
        self._calls.append(list(features))


class TestCollectionDetection:

    def test_collection_operations_are_recognized(self):
        assert usage.has_cdt([ListOperation.append("b", 1, ListPolicy())])
        assert usage.has_cdt([MapOperation.put("b", "k", 1, MapPolicy())])
        assert usage.has_cdt([HllOperation.init("b", 12, -1, None)])

    def test_scalar_operations_are_not_collections(self):
        assert not usage.has_cdt([Operation.put("b", 1)])
        assert not usage.has_cdt([Operation.get()])
        # A string append mutates a scalar bin, not a collection.
        assert not usage.has_cdt([StringOperation.append("b", "x")])

    def test_a_collection_op_anywhere_in_the_list_counts(self):
        ops = [Operation.put("b", 1), ListOperation.append("l", 1, ListPolicy())]
        assert usage.has_cdt(ops)

    def test_no_operations_is_not_a_collection(self):
        assert not usage.has_cdt([])


class TestRecording:

    def test_features_reach_the_core_in_one_call(self):
        client = _Recorder()
        usage.record(client, [usage.SHAPE_POINT, usage.API_BLOCKING])
        assert client.calls == [[usage.SHAPE_POINT, usage.API_BLOCKING]]

    def test_a_missing_client_is_tolerated(self):
        # Builders can be constructed without an owning client; staying
        # silent is preferable to raising from a metrics path.
        usage.record(None, [usage.SHAPE_POINT])

    def test_point_call_reports_mode_and_shape(self):
        client = _Recorder()
        usage.record_point(client, usage.API_DEFERRED)
        assert client.calls == [[usage.API_DEFERRED, usage.SHAPE_POINT]]

    def test_point_call_adds_transaction_and_collection(self):
        client = _Recorder()
        usage.record_point(
            client, usage.API_BLOCKING,
            txn=Txn(),
            operations=[ListOperation.append("l", 1, ListPolicy())],
        )
        assert client.calls == [[
            usage.API_BLOCKING, usage.SHAPE_POINT, usage.TRANSACTION, usage.CDT,
        ]]


class TestBuilderAccumulation:
    """The builder folds each segment's features in, then flushes once."""

    def _builder(self, usage_on=True):
        client = _Recorder(usage_on=usage_on)
        qb = SyncQueryBuilder(
            client=SimpleNamespace(),
            namespace="test",
            set_name="people",
            behavior=Behavior.DEFAULT,
            sdk_client=client,
        )
        return qb, client

    def test_transaction_participation_is_counted(self):
        qb, client = self._builder()
        qb._txn = Txn()
        qb._flush_usage(usage.API_BLOCKING, usage.SHAPE_POINT)
        assert client.calls == [[
            usage.API_BLOCKING, usage.SHAPE_POINT, usage.TRANSACTION,
        ]]

    def test_durable_delete_is_collected_from_the_segment(self):
        qb, client = self._builder()
        qb._durable_delete = True
        qb._collect_segment_usage()
        qb._flush_usage(usage.API_BLOCKING, usage.SHAPE_POINT)
        assert client.calls == [[
            usage.WRITE_DURABLE_DELETE, usage.API_BLOCKING, usage.SHAPE_POINT,
        ]]

    def test_segments_accumulate_across_a_chain(self):
        # Finalizing a segment clears the state it came from, so a chained
        # write + filtered read must still report both segments' features.
        qb, client = self._builder()
        qb._durable_delete = True
        qb._collect_segment_usage()
        qb._durable_delete = None
        qb._where_ael = "$.age > 21"
        qb._collect_segment_usage()
        qb._flush_usage(usage.API_DEFERRED, usage.SHAPE_BATCH)
        assert client.calls[0].count(usage.WRITE_DURABLE_DELETE) == 1
        assert usage.FILTER_AEL in client.calls[0]

    def test_one_flush_sends_one_call(self):
        qb, client = self._builder()
        qb._collect_segment_usage()
        qb._flush_usage(usage.API_BLOCKING, usage.SHAPE_POINT)
        assert len(client.calls) == 1

    def test_flushing_clears_the_pending_set(self):
        # A builder reused for a second call must not double-report.
        qb, client = self._builder()
        qb._where_ael = "$.age > 21"
        qb._collect_segment_usage()
        qb._flush_usage(usage.API_BLOCKING, usage.SHAPE_POINT)
        qb._flush_usage(usage.API_BLOCKING, usage.SHAPE_POINT)
        assert usage.FILTER_AEL in client.calls[0]
        assert usage.FILTER_AEL not in client.calls[1]

    def test_shape_follows_the_finalized_specs(self):
        qb, _ = self._builder()
        assert qb._usage_shape() == usage.SHAPE_QUERY
        qb._specs = [SimpleNamespace(keys=[1])]
        assert qb._usage_shape() == usage.SHAPE_POINT
        qb._specs = [SimpleNamespace(keys=[1, 2])]
        assert qb._usage_shape() == usage.SHAPE_BATCH
        qb._specs = [SimpleNamespace(keys=[1]), SimpleNamespace(keys=[2])]
        assert qb._usage_shape() == usage.SHAPE_BATCH

    def test_the_gate_is_off_unless_the_client_opted_in(self):
        qb, client = self._builder(usage_on=False)
        assert qb._usage_on is False
        assert client.calls == []


class TestIdentifiers:

    def test_execution_mode_rides_in_the_counter_name(self):
        # The transport is a flat mapping, so the mode cannot be an attribute.
        assert usage.API_BLOCKING == "feature.api.blocking"
        assert usage.API_DEFERRED == "feature.api.deferred"
        assert usage.API_BACKGROUND == "feature.api.background"

    def test_filter_technologies_have_separate_counters(self):
        assert usage.FILTER_AEL != usage.FILTER_EXP
        assert usage.OPERATE_AEL != usage.FILTER_AEL
        assert usage.OPERATE_EXP != usage.FILTER_EXP

    def test_every_identifier_is_namespaced_under_feature(self):
        names = [
            v for k, v in vars(usage).items()
            if k.isupper() and isinstance(v, str)
        ]
        assert names
        assert all(n.startswith("feature.") for n in names)


class TestUsageCounters:
    """The per-thread counter store, which is what replaced the core round-trip."""

    def test_counts_accumulate(self):
        counters = usage.UsageCounters()
        counters.add([usage.SHAPE_POINT, usage.API_BLOCKING])
        counters.add([usage.SHAPE_POINT])
        assert counters.totals() == {usage.SHAPE_POINT: 2, usage.API_BLOCKING: 1}

    def test_totals_are_a_copy(self):
        """A caller holding a snapshot must not see it move underneath them."""
        counters = usage.UsageCounters()
        counters.add([usage.SHAPE_POINT])
        first = counters.totals()
        counters.add([usage.SHAPE_POINT])
        assert first[usage.SHAPE_POINT] == 1

    def test_empty_before_anything_is_recorded(self):
        assert usage.UsageCounters().totals() == {}

    def test_threads_are_merged(self):
        """Each thread counts into its own mapping; the merge is what adds them."""
        counters = usage.UsageCounters()
        threads = [
            threading.Thread(target=lambda: [counters.add([usage.SHAPE_BATCH])
                                             for _ in range(100)])
            for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert counters.totals() == {usage.SHAPE_BATCH: 800}

    def test_record_is_silent_without_a_client(self):
        """Builders made without an owning client stay quiet rather than raise."""
        usage.record(None, [usage.SHAPE_POINT])

    def test_record_is_silent_when_the_client_has_no_counters(self):
        usage.record(SimpleNamespace(), [usage.SHAPE_POINT])

    def test_finished_threads_do_not_accumulate_buckets(self):
        """A thread-per-request server must not grow the bucket list forever."""
        import gc

        counters = usage.UsageCounters()
        for _ in range(30):
            thread = threading.Thread(
                target=lambda: counters.add([usage.SHAPE_POINT])
            )
            thread.start()
            thread.join()
        gc.collect()
        assert counters.totals() == {usage.SHAPE_POINT: 30}, "counts must survive"
        assert len(counters._buckets) < 30, "dead threads' buckets must be reclaimed"

