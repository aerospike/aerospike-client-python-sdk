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

"""Client metrics: policy, snapshot, exporters, and usage counters.

Everything a caller needs is re-exported here, so ``aerospike_sdk.metrics``
stays the import path and the split into :mod:`.policy`, :mod:`.snapshot` and
:mod:`.export` is internal. :mod:`.usage` is not re-exported: its counter names
and recording helpers are called by the SDK itself, not by applications, which
only read the counts back through
:attr:`~aerospike_sdk.MetricsSnapshot.usage`.
"""

from aerospike_sdk.metrics.policy import (
    CommandType,
    LatencyType,
    LatencyUnit,
    MetricsPolicy,
    Sampler,
    apply_metrics_settings,
    policy_from_settings,
)
from aerospike_sdk.metrics.snapshot import DerivedHistogram, MetricsSnapshot
from aerospike_sdk.metrics.export import (
    AsyncMetricsExporter,
    AsyncMultipleMetricsExporter,
    AsyncNoOpMetricsExporter,
    LearnMetricsFileExporter,
    MetricsExporter,
    MultipleMetricsExporter,
    NoOpMetricsExporter,
)

__all__ = [
    "AsyncMetricsExporter",
    "AsyncMultipleMetricsExporter",
    "AsyncNoOpMetricsExporter",
    "CommandType",
    "DerivedHistogram",
    "LatencyType",
    "LatencyUnit",
    "LearnMetricsFileExporter",
    "MetricsExporter",
    "MetricsPolicy",
    "MetricsSnapshot",
    "MultipleMetricsExporter",
    "NoOpMetricsExporter",
    "Sampler",
    "apply_metrics_settings",
    "policy_from_settings",
]
