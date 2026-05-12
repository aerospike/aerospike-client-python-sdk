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

"""Aerospike Python SDK — high-level API built on the ``aerospike_async`` package."""

from aerospike_async import (
    AbortStatus,
    AuthMode,
    BitPolicy,
    BitwiseOverflowActions,
    BitwiseResizeFlags,
    BitWriteFlags,
    CdtOperation,
    CommitStatus,
    CTX,
    ExpType,
    HLLWriteFlags,
    ListReturnType,
    ListWriteFlags,
    LoopVarPart,
    MapReturnType,
    MapWriteFlags,
    ModifyFlags,
    RegexFlag,
    SelectFlags,
    SpecialValue,
    Txn,
    TxnState,
)

from aerospike_sdk.aio import Client, Session, TransactionalSession, ClusterDefinition, Host
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.ael.exceptions import AelParseException
from aerospike_sdk.ael.filter_gen import Index, IndexContext, IndexTypeEnum, ParseResult
from aerospike_sdk.ael.parser import parse_ctx, parse_ael, parse_ael_with_index
from aerospike_sdk.exceptions import (
    AerospikeError,
    AuthenticationError,
    AuthorizationError,
    BackoffError,
    CommitError,
    ConnectionError,
    GenerationError,
    InvalidNamespaceError,
    InvalidNodeError,
    MaxErrorRate,
    QueryTerminatedError,
    QuotaError,
    SecurityError,
    SerializationError,
    TimeoutError,
)
from aerospike_sdk.error_strategy import ErrorHandler, ErrorStrategy, OnError
from aerospike_sdk.exp import Exp, val, in_list, map_keys, map_values
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.operation_result import OperationResult
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.record_result import RecordResult
from aerospike_sdk.record_stream import RecordStream
from aerospike_sdk.sync import SyncClient, SyncTransactionalSession
from aerospike_sdk.sync.record_stream import SyncRecordStream
from aerospike_sdk.sync.session import SyncSession

__version__ = "0.1.0"

__all__ = [
    "AbortStatus",
    "AerospikeError",
    "AuthenticationError",
    "AuthMode",
    "AuthorizationError",
    "BitPolicy",
    "BitwiseOverflowActions",
    "BitwiseResizeFlags",
    "BitWriteFlags",
    "BackoffError",
    "Behavior",
    "CdtOperation",
    "ClusterDefinition",
    "CommitError",
    "CommitStatus",
    "ConnectionError",
    "CTX",
    "DataSet",
    "AelParseException",
    "ErrorHandler",
    "ErrorStrategy",
    "Exp",
    "ExpType",
    "Client",
    "in_list",
    "GenerationError",
    "Host",
    "HllConfig",
    "HLLWriteFlags",
    "Index",
    "IndexContext",
    "IndexTypeEnum",
    "InvalidNamespaceError",
    "InvalidNodeError",
    "ListReturnType",
    "ListWriteFlags",
    "LoopVarPart",
    "MapReturnType",
    "MapWriteFlags",
    "MaxErrorRate",
    "ModifyFlags",
    "OnError",
    "OperationResult",
    "parse_ctx",
    "parse_ael",
    "parse_ael_with_index",
    "ParseResult",
    "QueryHint",
    "QueryTerminatedError",
    "QuotaError",
    "RecordResult",
    "RecordStream",
    "RegexFlag",
    "SecurityError",
    "SelectFlags",
    "SerializationError",
    "Session",
    "SpecialValue",
    "SyncClient",
    "SyncRecordStream",
    "SyncSession",
    "SyncTransactionalSession",
    "TimeoutError",
    "TransactionalSession",
    "Txn",
    "TxnState",
    "map_keys",
    "map_values",
    "val",
]

