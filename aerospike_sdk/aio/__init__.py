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

"""Async client and operations for the Aerospike SDK API."""

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.aio.cluster_definition import ClusterDefinition, Host
from aerospike_sdk.aio.info import InfoCommands
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.aio.pool import AsyncPool
from aerospike_sdk.aio.transactional_session import TransactionalSession
from aerospike_sdk.aio.session import NamespaceScStatus, Session

__all__ = [
    "AsyncPool",
    "Cluster",
    "ClusterDefinition",
    "Client",
    "Host",
    "InfoCommands",
    "NamespaceScStatus",
    "IndexBuilder",
    "QueryBuilder",
    "Session",
    "TransactionalSession",
]

