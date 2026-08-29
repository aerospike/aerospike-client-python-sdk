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
    AdminPolicy,
    AuthMode,
    BitPolicy,
    BitwiseOverflowActions,
    BitwiseResizeFlags,
    BitWriteFlags,
    CdtOperation,
    CollectionIndexType,
    CommandType,
    CommitStatus,
    CTX,
    ErrorDetailVerbosity,
    DropIndexTask,
    ExecuteTask,
    ExpressionTrace,
    ExpType,
    Filter,
    HLLWriteFlags,
    Key,
    LatencyUnit,
    ListOrderType,
    ListReturnType,
    ListSortFlags,
    ListWriteFlags,
    LoopVarPart,
    MapOrder,
    MapReturnType,
    MapWriteFlags,
    SortedMap,
    ModifyFlags,
    QueryDuration,
    RegexFlag,
    IndexTask,
    RegisterTask,
    ResultCode,
    Sampler,
    SelectFlags,
    SpecialValue,
    StringNumericType,
    StringOperation,
    StringRegexFlags,
    StringWriteFlags,
    SubCode,
    Txn,
    TxnState,
    UDFLang,
    UdfRemoveTask,
    Version,
)

from aerospike_sdk.aio import AsyncPool, Session, TransactionalSession, ClusterDefinition, Host
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import (
    AerospikeError,
    AuthenticationError,
    AuthorizationError,
    BackoffError,
    BinError,
    BinExistsError,
    BinNotFoundError,
    BinOpInvalidError,
    BinTypeError,
    CapacityError,
    CommitError,
    ConnectionError,
    ElementError,
    ElementExistsError,
    ElementNotFoundError,
    FilteredOutError,
    GenerationError,
    BatchError,
    IndexAlreadyExistsError,
    IndexNotFoundError,
    InvalidNamespaceError,
    InvalidNodeError,
    KeyBusyError,
    MaxErrorRate,
    QueryError,
    QueryTerminatedError,
    QuotaError,
    RecordExistsError,
    RecordNotFoundError,
    RecordTooBigError,
    SecondaryIndexError,
    SecurityError,
    SerializationError,
    TimeoutError,
    TransactionError,
    UdfError,
)
from aerospike_sdk.error_strategy import ErrorHandler, ErrorStrategy, OnError
from aerospike_sdk.exp import Exp, val, in_list, map_keys, map_values
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.metrics import (
    DerivedHistogram,
    LatencyType,
    MetricsPolicy,
    MetricsSnapshot,
)
from aerospike_sdk.loggers import SdkLoggers, refresh_log_levels
from aerospike_sdk.operation_result import OperationResult
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.record_result import RecordResult
from aerospike_sdk.record_stream import RecordStream
from aerospike_sdk.sync import SyncTransactionalSession
from aerospike_sdk.sync.record_stream import SyncRecordStream
from aerospike_sdk.sync.session import SyncSession

# Deprecated connection primitives, kept importable for one deprecation
# cycle behind the module __getattr__ below. ClusterDefinition -> Cluster ->
# Session is the supported entry.
_DEPRECATED_ENTRY_POINTS = {
    "Client": (
        "aerospike_sdk.Client is deprecated; connect with "
        "aerospike_sdk.ClusterDefinition(...).connect() instead"
    ),
    "SyncClient": (
        "aerospike_sdk.SyncClient is deprecated; connect with "
        "aerospike_sdk.sync.ClusterDefinition(...).connect() instead"
    ),
}


def __getattr__(name: str):
    if name in _DEPRECATED_ENTRY_POINTS:
        import warnings

        warnings.warn(_DEPRECATED_ENTRY_POINTS[name], DeprecationWarning, stacklevel=2)
        if name == "Client":
            from aerospike_sdk.aio.client import Client

            return Client
        from aerospike_sdk.sync.client import SyncClient

        return SyncClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

try:
    from importlib.metadata import version as _meta_version
    __version__ = _meta_version("aerospike-sdk")
except Exception:
    __version__ = "0.0.0"

__all__ = [
    "AbortStatus",
    "AdminPolicy",
    "AerospikeError",
    "AsyncPool",
    "AuthenticationError",
    "AuthMode",
    "AuthorizationError",
    "BitPolicy",
    "BitwiseOverflowActions",
    "BitwiseResizeFlags",
    "BitWriteFlags",
    "BackoffError",
    "BatchError",
    "BinError",
    "BinExistsError",
    "BinNotFoundError",
    "BinOpInvalidError",
    "BinTypeError",
    "Behavior",
    "CdtOperation",
    "CapacityError",
    "ClusterDefinition",
    "CollectionIndexType",
    "CommandType",
    "CommitError",
    "CommitStatus",
    "ConnectionError",
    "CTX",
    "DataSet",
    "DerivedHistogram",
    "ElementError",
    "ElementExistsError",
    "ElementNotFoundError",
    "Filter",
    "FilteredOutError",
    "ErrorHandler",
    "ErrorDetailVerbosity",
    "ErrorStrategy",
    "DropIndexTask",
    "ExecuteTask",
    "Exp",
    "ExpressionTrace",
    "ExpType",
    "in_list",
    "GenerationError",
    "Host",
    "HllConfig",
    "HLLWriteFlags",
    "IndexAlreadyExistsError",
    "IndexNotFoundError",
    "InvalidNamespaceError",
    "InvalidNodeError",
    "Key",
    "KeyBusyError",
    "LatencyType",
    "LatencyUnit",
    "ListOrderType",
    "ListReturnType",
    "ListSortFlags",
    "ListWriteFlags",
    "LoopVarPart",
    "MapOrder",
    "MapReturnType",
    "MapWriteFlags",
    "MaxErrorRate",
    "MetricsPolicy",
    "MetricsSnapshot",
    "ModifyFlags",
    "OnError",
    "OperationResult",
    "QueryDuration",
    "QueryHint",
    "QueryError",
    "QueryTerminatedError",
    "QuotaError",
    "RecordExistsError",
    "RecordNotFoundError",
    "RecordResult",
    "RecordStream",
    "RecordTooBigError",
    "RegexFlag",
    "IndexTask",
    "RegisterTask",
    "ResultCode",
    "Sampler",
    "SdkLoggers",
    "SecondaryIndexError",
    "SecurityError",
    "SelectFlags",
    "SortedMap",
    "SerializationError",
    "Session",
    "SpecialValue",
    "StringNumericType",
    "StringOperation",
    "StringRegexFlags",
    "StringWriteFlags",
    "SubCode",
    "SyncRecordStream",
    "SyncSession",
    "SyncTransactionalSession",
    "TimeoutError",
    "TransactionError",
    "UdfError",
    "TransactionalSession",
    "Txn",
    "TxnState",
    "UDFLang",
    "UdfRemoveTask",
    "Version",
    "map_keys",
    "map_values",
    "refresh_log_levels",
    "val",
]

