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

"""``session_for`` hands the behavior to the owning client and leaves the receiver alone."""

from unittest.mock import MagicMock

from aerospike_sdk.aio.session import Session
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.session import Session as SyncSession


def test_async_session_for_delegates_to_the_client():
    client = MagicMock()
    client._async_client = MagicMock()
    session = Session(client=client, behavior=Behavior.DEFAULT)

    derived = session.session_for(Behavior.READ_FAST)

    client.create_session.assert_called_once_with(Behavior.READ_FAST)
    assert derived is client.create_session.return_value
    assert session._behavior is Behavior.DEFAULT


def test_sync_session_for_delegates_to_the_client():
    client = MagicMock()
    client.underlying_client = MagicMock()
    session = SyncSession(client=client, behavior=Behavior.DEFAULT)

    derived = session.session_for(Behavior.READ_FAST)

    client.create_session.assert_called_once_with(Behavior.READ_FAST)
    assert derived is client.create_session.return_value
