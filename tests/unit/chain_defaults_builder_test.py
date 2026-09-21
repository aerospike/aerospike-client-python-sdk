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

"""Chain-level defaults must be reachable from a write-verb chain.

``default_where`` and the ``default_*`` TTL verbs set a default for every
operation in the chain that does not supply its own. They are defined on the
query builder, so a chain opened with ``session.query(...)`` has them -- but a
chain opened with a write verb (``insert``/``update``/``upsert``) yields a
write segment, which did not forward them.

Two shapes, because the write segments differ:

* multi-key segments wrap a real ``QueryBuilder``, so forwarding writes
  straight through to it;
* single-key segments are a fast path that builds no query builder until
  ``_promote()``. Chain defaults promote first and then delegate -- the same
  shape the per-op TTL verbs already use. Stashing the default instead and
  replaying it at promotion looks cheaper but is wrong: a chain like
  ``upsert(k).default_expire_record_after(...).bin("v").set_to(1)`` never
  promotes, so the stashed default is silently dropped and the record is
  written with no TTL.
"""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from aerospike_sdk import Key
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteSegmentBuilder,
    _SingleKeyWriteSegment,
)
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.operations.query import (
    QueryBuilder as SyncQueryBuilder,
    WriteSegmentBuilder as SyncWriteSegmentBuilder,
    _SingleKeyWriteSegment as SyncSingleKeyWriteSegment,
)

TTL_SECONDS = 600


def _key(val: int = 1) -> Key:
    return Key("test", "test", val)


def _multi(qb_cls, wsb_cls):
    qb = qb_cls(client=MagicMock(), namespace="test", set_name="t")
    qb._op_type = "upsert"
    qb._single_key = _key()
    return wsb_cls(qb), qb


def _single(seg_cls):
    return seg_cls(
        client=MagicMock(),
        key=_key(),
        op_type="upsert",
        behavior=Behavior.DEFAULT,
        write_policy=None,
    )


@pytest.mark.parametrize(
    "qb_cls, wsb_cls",
    [(QueryBuilder, WriteSegmentBuilder), (SyncQueryBuilder, SyncWriteSegmentBuilder)],
    ids=["async", "sync"],
)
class TestMultiKeyWriteSegmentForwards:
    """A multi-key segment wraps a real builder, so it forwards directly."""

    def test_default_expire_record_after_seconds(self, qb_cls, wsb_cls):
        wsb, qb = _multi(qb_cls, wsb_cls)
        assert qb._default_ttl_seconds is None
        assert wsb.default_expire_record_after_seconds(TTL_SECONDS) is wsb
        assert qb._default_ttl_seconds == TTL_SECONDS

    def test_default_expire_record_after_timedelta(self, qb_cls, wsb_cls):
        wsb, qb = _multi(qb_cls, wsb_cls)
        assert wsb.default_expire_record_after(timedelta(minutes=10)) is wsb
        assert qb._default_ttl_seconds == TTL_SECONDS

    def test_default_never_expire(self, qb_cls, wsb_cls):
        wsb, qb = _multi(qb_cls, wsb_cls)
        assert wsb.default_never_expire() is wsb
        assert qb._default_ttl_seconds == -1

    def test_default_where_sets_the_chain_filter(self, qb_cls, wsb_cls):
        wsb, qb = _multi(qb_cls, wsb_cls)
        assert qb._default_where_ael is None
        assert wsb.default_where("$.age > %s", 21) is wsb
        assert qb._default_where_ael is not None


@pytest.mark.parametrize(
    "seg_cls", [_SingleKeyWriteSegment, SyncSingleKeyWriteSegment], ids=["async", "sync"]
)
class TestSingleKeyWriteSegmentPromotes:
    """A single-key segment promotes on a chain default, then delegates."""

    def test_setting_a_default_promotes_and_applies(self, seg_cls):
        """Promotion is the point: an unpromoted chain would drop the default."""
        seg = _single(seg_cls)
        assert seg._qb is None
        assert seg.default_expire_record_after_seconds(TTL_SECONDS) is seg
        assert seg._qb is not None, "a chain default must promote to take effect"
        assert seg._qb._default_ttl_seconds == TTL_SECONDS

    def test_default_where_promotes_and_applies(self, seg_cls):
        seg = _single(seg_cls)
        seg.default_where("$.age > %s", 21)
        assert seg._qb._default_where_ael is not None

    def test_plain_fast_path_still_does_not_promote(self, seg_cls):
        """Only chain defaults promote -- the untouched fast path is unaffected."""
        seg = _single(seg_cls)
        seg.with_durable_delete()
        assert seg._qb is None

    def test_default_after_promotion_still_applies(self, seg_cls):
        """The other entry order: already promoted, then set the default."""
        seg = _single(seg_cls)
        seg._promote()
        assert seg.default_expire_record_after_seconds(TTL_SECONDS) is seg
        assert seg._qb._default_ttl_seconds == TTL_SECONDS
