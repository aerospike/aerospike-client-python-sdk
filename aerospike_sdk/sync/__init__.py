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
from aerospike_sdk.sync.info import SyncInfoCommands
from aerospike_sdk.sync.operations.batch import SyncBatchOperationBuilder
from aerospike_sdk.sync.operations.index import SyncIndexBuilder
from aerospike_sdk.sync.operations.query import SyncQueryBuilder
from aerospike_sdk.sync.session import SyncSession
from aerospike_sdk.sync.tls_builder import TlsBuilder
from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

__all__ = [
    "Cluster",
    "ClusterDefinition",
    "Host",
    "SyncBatchOperationBuilder",
    "SyncInfoCommands",
    "SyncIndexBuilder",
    "SyncQueryBuilder",
    "SyncSession",
    "SyncTransactionalSession",
    "TlsBuilder",
]


def __getattr__(name: str):
    # Deprecated connection primitive, importable for one deprecation cycle.
    if name == "SyncClient":
        import warnings

        warnings.warn(
            "aerospike_sdk.sync.SyncClient is deprecated; connect with "
            "aerospike_sdk.sync.ClusterDefinition(...).connect() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        from aerospike_sdk.sync.client import SyncClient

        return SyncClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

