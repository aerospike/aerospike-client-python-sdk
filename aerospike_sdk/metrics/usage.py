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

"""Stable identifiers for the feature-usage counters.

Usage counters classify *how* the application invokes the SDK — which shapes,
filter technologies, and API styles it uses — rather than how fast commands
run. They are counter-only: an increment per feature per call, never a timer,
and never sampled.

Collection is off unless :attr:`~aerospike_sdk.MetricsPolicy.usage_enabled`
was set when metrics were enabled. Call sites gate on a cached flag before
assembling anything, so the disabled path costs one attribute load and a
branch.

The counter transport is a flat name-to-count mapping, so the specification's
``execution_mode`` dimension on ``feature.api`` is carried in the name rather
than as a separate attribute: ``feature.api.blocking`` is the counter the
specification writes as ``feature.api`` with ``execution_mode=blocking``.

Counters live in this SDK rather than in the client core, so they count calls
made through this API only.
"""

import threading
import weakref
from typing import Dict, List

from aerospike_async import BitOperation, HllOperation, ListOperation, MapOperation

# -- API surface --------------------------------------------------------------

#: Caller waits for completion before the API returns (the sync surface).
API_BLOCKING = "feature.api.blocking"
#: API returns an awaitable; completion is observed later (the async surface).
API_DEFERRED = "feature.api.deferred"
#: API registers server-side work and returns before the job finishes.
API_BACKGROUND = "feature.api.background"

#: Single-key read, write, operate or delete.
SHAPE_POINT = "feature.shape.point"
#: Multi-key batch.
SHAPE_BATCH = "feature.shape.batch"
#: Partition scan or secondary-index query stream.
SHAPE_QUERY = "feature.shape.query"

# -- Filters and expressions --------------------------------------------------

#: Record filter / ``where`` expressed in AEL.
FILTER_AEL = "feature.filter.ael"
#: Record filter / ``where`` built with the programmatic expression API.
FILTER_EXP = "feature.filter.exp"
#: Query or background job restricted by a secondary-index slice.
FILTER_SECONDARY_INDEX = "feature.filter.secondary_index"
#: AEL evaluated in operate (bin read/modify), not as a filter.
OPERATE_AEL = "feature.operate.ael"
#: Programmatic expression evaluated in operate, not as a filter.
OPERATE_EXP = "feature.operate.exp"

# -- Queries and scans --------------------------------------------------------

#: Partition-pagination cursor used on a query or scan.
QUERY_PARTITION_FILTER = "feature.query.partition_filter"

# -- UDFs and background server work ------------------------------------------

#: Record-level UDF execute.
UDF_RECORD = "feature.udf.record"
#: Background UDF job registration.
BACKGROUND_UDF = "feature.background.udf"
#: Background scan update / delete / touch on a set.
BACKGROUND_OPERATE = "feature.background.operate"

# -- Transactions, CDT, writes ------------------------------------------------

#: Multi-record transaction.
TRANSACTION = "feature.transaction"
#: CDT list/map/bit/hyperloglog operation on the wire.
CDT = "feature.cdt"
#: Write, delete or operate carrying the durable-delete flag.
WRITE_DURABLE_DELETE = "feature.write.durable_delete"

# -- Admin --------------------------------------------------------------------

#: Create or drop a secondary index.
ADMIN_INDEX = "feature.admin.index"
#: Truncate a set or namespace.
ADMIN_TRUNCATE = "feature.admin.truncate"


# Collection-data-type operations, for the CDT counter. String operations are
# excluded: they act on a scalar bin, not a collection.
_CDT_OPS = (ListOperation, MapOperation, BitOperation, HllOperation)


def has_cdt(operations) -> bool:
    """Whether any of ``operations`` is a collection-data-type operation."""
    for op in operations:
        if isinstance(op, _CDT_OPS):
            return True
    return False


class _ThreadBucket:
    """One thread's counters, in a weak-referenceable wrapper.

    A bare dict cannot be weak-referenced, so the mapping is carried on an
    object that can be, letting a finished thread's bucket be reclaimed.
    """

    __slots__ = ("counts", "__weakref__")

    def __init__(self) -> None:
        self.counts: Dict[str, int] = {}


class UsageCounters:
    """Per-client feature counters, accumulated per thread.

    Counting happens on the calling thread into a dict that only that thread
    writes, and the totals are merged when a snapshot is taken. That keeps
    increments lock-free: a shared counter behind a mutex would serialize
    every threaded caller on the SDK's own bookkeeping, which is a poor trade
    for numbers nobody reads until export. It is also correct without the GIL,
    since no two threads touch the same mapping.

    The lock is taken only when a thread first records anything and when a
    snapshot copies the list of buckets -- never on the increment itself.
    """

    __slots__ = ("_local", "_buckets", "_lock", "_retired")

    def __init__(self) -> None:
        self._local = threading.local()
        # Weak handles, so a thread that exits does not pin its bucket. A
        # server running a thread per request would otherwise grow this list
        # for the life of the process. The counts themselves are folded into
        # `_retired` as each thread goes, keeping totals cumulative.
        self._buckets: List[weakref.ref] = []
        self._retired: Dict[str, int] = {}
        self._lock = threading.Lock()

    def _bucket(self) -> Dict[str, int]:
        """This thread's counter mapping, registered on first use."""
        holder = getattr(self._local, "holder", None)
        if holder is None:
            holder = _ThreadBucket()
            self._local.holder = holder
            # The finalizer keeps `counts` alive past the holder so the
            # departing thread's numbers can be banked. A dict cannot be
            # weak-referenced, which is why the holder exists at all.
            weakref.finalize(holder, self._retire, holder.counts)
            with self._lock:
                self._buckets.append(weakref.ref(holder))
        return holder.counts

    def _retire(self, counts: Dict[str, int]) -> None:
        """Fold a departed thread's counts into the cumulative total."""
        with self._lock:
            for feature, count in counts.items():
                self._retired[feature] = self._retired.get(feature, 0) + count
            self._buckets = [ref for ref in self._buckets if ref() is not None]

    def add(self, features) -> None:
        """Increment each named counter once."""
        bucket = self._bucket()
        for feature in features:
            bucket[feature] = bucket.get(feature, 0) + 1

    def totals(self) -> Dict[str, int]:
        """Every thread's counts, merged. Cumulative, never reset."""
        with self._lock:
            merged: Dict[str, int] = dict(self._retired)
            refs = list(self._buckets)
        for ref in refs:
            holder = ref()
            if holder is None:
                continue          # already banked by its finalizer
            bucket = holder.counts
            # A concurrent increment may land mid-merge; the total is then one
            # export behind for that counter, which is the accepted cost of not
            # locking the hot path.
            for feature, count in list(bucket.items()):
                merged[feature] = merged.get(feature, 0) + count
        return merged


def record(sdk_client, features) -> None:
    """Add a feature set to the client's counters.

    Args:
        sdk_client: The owning SDK client. ``None`` is tolerated so builders
            constructed without an owning client stay silent rather than
            raising.
        features: Counter identifiers to increment, one increment each.
    """
    if sdk_client is None:
        return
    counters = getattr(sdk_client, "_usage_counters", None)
    if counters is not None:
        counters.add(features)


def record_point(sdk_client, execution_mode: str, txn=None, operations=()) -> None:
    """Record the counters for a single-key call taken on a fast path.

    The fast paths bypass the builder's segment bookkeeping, so they report
    their feature set directly from the few pieces of state they carry.

    Args:
        sdk_client: The owning SDK client, or ``None`` to stay silent.
        execution_mode: One of :data:`API_BLOCKING` or :data:`API_DEFERRED`.
        txn: The enclosing transaction, if the call joined one.
        operations: Operations on the call, checked for collection types.
    """
    features = [execution_mode, SHAPE_POINT]
    if txn is not None:
        features.append(TRANSACTION)
    if has_cdt(operations):
        features.append(CDT)
    record(sdk_client, features)
