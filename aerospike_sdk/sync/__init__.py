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

"""Sync client and operations for the Aerospike SDK API."""

from aerospike_sdk.sync.cluster import Cluster
from aerospike_sdk.sync.cluster_definition import ClusterDefinition, Host
from aerospike_sdk.sync.info import InfoCommands, SyncInfoCommands
from aerospike_sdk.sync.operations.index import IndexBuilder, SyncIndexBuilder
from aerospike_sdk.sync.operations.query import QueryBuilder, SyncQueryBuilder
from aerospike_sdk.sync.session import Session, SyncSession
from aerospike_sdk.sync.tls_builder import TlsBuilder
from aerospike_sdk.sync.transactional_session import (
    SyncTransactionalSession,
    TransactionalSession,
)

# The bare names are the committed convention (path-differentiated from the
# aio classes of the same name — e.g. ``aerospike_sdk.sync.Session`` vs
# ``aerospike_sdk.aio.Session``); the Sync*-prefixed aliases stay importable
# for one deprecation cycle (removed at GA).
__all__ = [
    "Cluster",
    "ClusterDefinition",
    "Host",
    "IndexBuilder",
    "InfoCommands",
    "QueryBuilder",
    "Session",
    "SyncInfoCommands",
    "SyncIndexBuilder",
    "SyncQueryBuilder",
    "SyncSession",
    "SyncTransactionalSession",
    "TlsBuilder",
    "TransactionalSession",
]
