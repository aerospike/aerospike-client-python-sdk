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

"""Policy configuration for the Aerospike Python SDK."""

from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_registry import (
    get_all_behaviors,
    get_behavior,
    get_behavior_or_default,
)
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Scope, Settings
from aerospike_sdk.policy.system_settings import SystemSettings, TransactionSettings

__all__ = [
    "Behavior",
    "Mode",
    "OpKind",
    "OpShape",
    "Scope",
    "Settings",
    "SystemSettings",
    "TransactionSettings",
    "get_all_behaviors",
    "get_behavior",
    "get_behavior_or_default",
]
