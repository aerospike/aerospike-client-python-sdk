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

"""Integration tests for record expiration / TTL.

The first three tests parallel the reference JSDK ``ExpireTest`` suite
(``expire``, ``noExpire``, ``resetReadTtl``); the remaining three exercise
the ``expire_record_after(timedelta)`` and ``expire_record_at(datetime)``
methods unique to PSDK.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from aerospike_sdk import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Settings

BIN_NAME = "expirebin"
EXPIRE_SET = "expire"


@pytest.fixture
async def client(aerospike_host, client_policy):
    """Function-scoped: each test owns its key and starts from a clean slate."""
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        yield client


async def test_expire(client):
    """A 1-second TTL: record reads immediately, is gone after a short wait."""
    session = client.create_session()
    k = DataSet.of("test", EXPIRE_SET).id("expire")
    await session.delete(k).execute()

    await (
        session.upsert(k)
            .expire_record_after_seconds(1)
            .bin(BIN_NAME).set_to("expirevalue")
            .execute()
    )

    result = await session.query(k).execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.bins[BIN_NAME] == "expirevalue"

    await asyncio.sleep(3)

    result = await session.query(k).execute()
    first = await result.first()
    assert first is None or not first.is_ok or BIN_NAME not in first.record_or_raise().bins


async def test_no_expire(client):
    """never_expire() keeps a record past any wall-clock check."""
    session = client.create_session()
    k = DataSet.of("test", EXPIRE_SET).id("noExpire")
    await session.delete(k).execute()

    await (
        session.upsert(k)
            .never_expire()
            .bin(BIN_NAME).set_to("noexpirevalue")
            .execute()
    )

    result = await session.query(k).execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.bins[BIN_NAME] == "noexpirevalue"

    await asyncio.sleep(4)

    result = await session.query(k).execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.bins[BIN_NAME] == "noexpirevalue"


async def test_reset_read_ttl(client):
    """read_touch_ttl_percent=80 extends TTL on read when remaining < threshold."""
    k = DataSet.of("test", EXPIRE_SET).id("resetReadTtl")
    await client.create_session().delete(k).execute()

    await (
        client.create_session().upsert(k)
            .expire_record_after_seconds(2)
            .bin(BIN_NAME).set_to("expirevalue")
            .execute()
    )

    # Read the record before it expires and reset read ttl.
    await asyncio.sleep(1)

    behavior_reset = Behavior.DEFAULT.derive_with_changes(
        "reset_read_ttl",
        all=Settings(read_touch_ttl_percent=80),
    )
    session_reset = client.create_session(behavior=behavior_reset)
    result = await session_reset.query(k).execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.bins[BIN_NAME] == "expirevalue"

    # Read the record again, but don't reset read ttl.
    await asyncio.sleep(1)

    behavior_no_reset = Behavior.DEFAULT.derive_with_changes(
        "no_reset_read_ttl",
        all=Settings(read_touch_ttl_percent=0),
    )
    session_no_reset = client.create_session(behavior=behavior_no_reset)
    result = await session_no_reset.query(k).execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.bins[BIN_NAME] == "expirevalue"

    # Read the record after it expires, showing it's gone.
    await asyncio.sleep(2)
    session = client.create_session()
    result = await session.query(k).execute()
    first = await result.first()
    assert first is None or not first.is_ok or BIN_NAME not in first.record_or_raise().bins


async def test_expire_record_after_timedelta(client):
    """expire_record_after(timedelta) sets TTL via duration object."""
    session = client.create_session()
    k = DataSet.of("test", EXPIRE_SET).id("afterTimedelta")
    await session.delete(k).execute()

    await (
        session.upsert(k)
            .expire_record_after(timedelta(minutes=5))
            .bin(BIN_NAME).set_to("td_ttl")
            .execute()
    )

    result = await session.query(k).with_no_bins().execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.ttl is not None
    assert 290 <= rec.ttl <= 305


async def test_expire_record_at_datetime(client):
    """expire_record_at(datetime) sets TTL via absolute timestamp."""
    session = client.create_session()
    k = DataSet.of("test", EXPIRE_SET).id("atDatetime")
    await session.delete(k).execute()

    target = datetime.now(timezone.utc) + timedelta(minutes=5)
    await (
        session.upsert(k)
            .expire_record_at(target)
            .bin(BIN_NAME).set_to("abs_ttl")
            .execute()
    )

    result = await session.query(k).with_no_bins().execute()
    rec = (await result.first_or_raise()).record_or_raise()
    assert rec.ttl is not None
    assert 290 <= rec.ttl <= 305


async def test_expire_record_at_rejects_past_datetime(client):
    """A datetime in the past raises ValueError before any wire IO."""
    session = client.create_session()
    k = DataSet.of("test", EXPIRE_SET).id("atDatetimePast")

    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    with pytest.raises(ValueError, match="must be in the future"):
        session.upsert(k).expire_record_at(past)
