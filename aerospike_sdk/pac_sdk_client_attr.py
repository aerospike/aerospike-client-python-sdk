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

"""Attribute name used to tag :class:`aerospike_async.Client` from the SDK.

Avoids import cycles between :mod:`aerospike_sdk.aio.client` and query builders.
The async SDK client sets this on connect and clears it on close.
"""

# Stamped on ``aerospike_async.Client`` by ``aerospike_sdk.aio.client.Client``.
PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL = (
    "_aerospike_sdk_cached_supports_server_compiled_ael"
)
