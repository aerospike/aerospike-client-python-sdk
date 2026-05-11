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

"""Tests for QueryPolicy field exposure."""

import pytest
from aerospike_async import BasePolicy, QueryDuration, QueryPolicy, Replica

from aerospike_sdk import DataSet, Client
from aerospike_sdk.policy.behavior import Behavior


@pytest.fixture
async def session(client):
    """Setup session with default behavior for testing."""
    return client.create_session(Behavior.DEFAULT)


async def test_records_per_second(session):
    """Test records_per_second method on QueryBuilder."""
    users = DataSet.of("test", "users")

    # Test that the method exists and can be called
    query_builder = session.query(users).records_per_second(1000)
    assert query_builder is not None

    # Verify the policy was set
    policy = query_builder._policy
    assert policy is not None
    assert policy.records_per_second == 1000


async def test_max_records(session):
    """Test max_records method on QueryBuilder."""
    users = DataSet.of("test", "users")

    # Test that the method exists and can be called
    query_builder = session.query(users).max_records(10000)
    assert query_builder is not None

    # Verify the policy was set
    policy = query_builder._policy
    assert policy is not None
    assert policy.max_records == 10000


async def test_expected_duration(session):
    """Test expected_duration method on QueryBuilder."""
    users = DataSet.of("test", "users")

    # Test that the method exists and can be called with QueryDuration enum
    query_builder = session.query(users).expected_duration(QueryDuration.SHORT)
    assert query_builder is not None

    # Verify the policy was set
    policy = query_builder._policy
    assert policy is not None
    assert policy.expected_duration == QueryDuration.SHORT


async def test_replica(session):
    """Test replica method on QueryBuilder."""
    users = DataSet.of("test", "users")

    # Test that the method exists and can be called with Replica enum
    query_builder = session.query(users).replica(Replica.SEQUENCE)
    assert query_builder is not None

    # Verify the policy was set
    policy = query_builder._policy
    assert policy is not None
    assert policy.replica == Replica.SEQUENCE


async def test_base_policy(session):
    """Test base_policy method on QueryBuilder."""
    users = DataSet.of("test", "users")

    # Test that the method exists and can be called
    base = BasePolicy()
    query_builder = session.query(users).base_policy(base)
    assert query_builder is not None

    # Verify the policy was set
    policy = query_builder._policy
    assert policy is not None
    assert policy.base_policy is not None


async def test_chaining_policy_fields(session):
    """Test that multiple policy fields can be chained."""
    users = DataSet.of("test", "users")

    # Test chaining multiple policy methods
    query_builder = (
        session.query(users)
            .records_per_second(1000)
            .max_records(10000)
            .expected_duration(QueryDuration.SHORT)
            .replica(Replica.SEQUENCE)
    )
    assert query_builder is not None

    # Verify all fields were set
    policy = query_builder._policy
    assert policy is not None
    assert policy.records_per_second == 1000
    assert policy.max_records == 10000
    assert policy.expected_duration == QueryDuration.SHORT
    assert policy.replica == Replica.SEQUENCE


async def test_policy_fields_with_existing_policy(session):
    """Test that policy fields work with an existing QueryPolicy."""
    users = DataSet.of("test", "users")

    # Create a policy and set it
    policy = QueryPolicy()
    query_builder = session.query(users).with_policy(policy)

    # Then add additional fields
    query_builder = query_builder.records_per_second(1000).max_records(10000)
    assert query_builder is not None

