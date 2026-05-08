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

"""Integration tests for Behavior: verify that derived behaviors
correctly flow through Session -> operation builders -> PAC policies
when performing real cluster operations."""

from datetime import timedelta

import pytest

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy.behavior_settings import Settings


@pytest.fixture
def dataset():
    return DataSet.of("test", "behavior_test")


@pytest.fixture
async def cleanup(client, dataset):
    """Delete test keys after each test."""
    keys_to_clean = []
    yield keys_to_clean
    session = client.create_session()
    for key in keys_to_clean:
        try:
            await session.delete(key).execute()
        except Exception:
            pass


async def test_custom_behavior_put_get(client, dataset, cleanup):
    """A session with a derived behavior can write and read back data."""
    behavior = Behavior.DEFAULT.derive_with_changes(
        "integration_custom",
        reads=Settings(total_timeout=timedelta(seconds=10), max_retries=3),
        writes=Settings(total_timeout=timedelta(seconds=10)),
    )
    session = client.create_session(behavior)
    key = dataset.id("bhv_custom_1")
    cleanup.append(key)

    await session.upsert(key).set_bins({"name": "Alice", "score": 100}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        assert result.record.bins == {"name": "Alice", "score": 100}


async def test_predefined_read_fast(client, dataset, cleanup):
    """Behavior.READ_FAST works end-to-end for writes (inherited from
    DEFAULT) and reads (with its own short timeouts)."""
    session = client.create_session(Behavior.READ_FAST)
    key = dataset.id("bhv_readfast_1")
    cleanup.append(key)

    await session.upsert(key).set_bins({"x": 42}).execute()

    stream = await session.query(key).execute()
    result = await stream.first_or_raise()
    assert result is not None
    assert result.record.bins["x"] == 42


async def test_behavior_inheritance_chain(client, dataset, cleanup):
    """A grandchild behavior inherits correctly through the chain and
    can perform operations."""
    parent = Behavior.DEFAULT.derive_with_changes(
        "parent",
        all=Settings(max_retries=4),
    )
    child = parent.derive_with_changes(
        "child",
        reads=Settings(total_timeout=timedelta(seconds=15)),
    )

    name = child.name  # sanity
    assert name == "child"

    s = client.create_session(child)
    key = dataset.id("bhv_inherit_1")
    cleanup.append(key)

    await s.upsert(key).set_bins({"level": "grandchild"}).execute()

    stream = await s.query(key).execute()
    async for result in stream:
        assert result.record.bins["level"] == "grandchild"


async def test_different_sessions_independent(client, dataset, cleanup):
    """Two sessions with different behaviors operate independently on
    the same cluster connection."""
    fast = Behavior.DEFAULT.derive_with_changes(
        "fast",
        reads=Settings(
            total_timeout=timedelta(seconds=5),
            max_retries=1,
        ),
    )
    safe = Behavior.DEFAULT.derive_with_changes(
        "safe",
        all=Settings(
            total_timeout=timedelta(seconds=30),
            max_retries=5,
        ),
    )

    fast_session = client.create_session(fast)
    safe_session = client.create_session(safe)

    key_fast = dataset.id("bhv_fast_1")
    key_safe = dataset.id("bhv_safe_1")
    cleanup.extend([key_fast, key_safe])

    await fast_session.upsert(key_fast).set_bins({"src": "fast"}).execute()
    await safe_session.upsert(key_safe).set_bins({"src": "safe"}).execute()

    stream_fast = await fast_session.query(key_fast).execute()
    stream_safe = await safe_session.query(key_safe).execute()
    rec_fast = await stream_fast.first_or_raise()
    rec_safe = await stream_safe.first_or_raise()

    assert rec_fast.record.bins["src"] == "fast"
    assert rec_safe.record.bins["src"] == "safe"

    assert fast_session.behavior.name == "fast"
    assert safe_session.behavior.name == "safe"


async def test_batch_with_custom_behavior(client, dataset, cleanup):
    """Batch operations work through a session with a custom behavior."""
    behavior = Behavior.DEFAULT.derive_with_changes(
        "batch_bhv",
        reads=Settings(max_retries=3),
    )
    session = client.create_session(behavior)

    keys = dataset.ids("bhv_batch_1", "bhv_batch_2", "bhv_batch_3")
    cleanup.extend(keys)

    for i, key in enumerate(keys):
        await session.upsert(key).set_bins({"idx": i}).execute()

    stream = await session.query(*keys).execute()
    count = 0
    async for result in stream:
        assert "idx" in result.record.bins
        count += 1
    assert count == 3
