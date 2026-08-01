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

"""PSDK runtime feature gates (dark-launch until flipped)."""

from __future__ import annotations

from typing import Optional

# Hard-false so field 44 query selection and field 43 server-compiled AEL can
# merge without changing dev behavior. Flip to True when ready to enable.
PSDK_ENABLE_QUERY_SELECTION: bool = False
PSDK_ENABLE_SERVER_COMPILED_AEL: bool = False


def cached_ael_capability_kwargs(
    supports_server_compiled_ael: Optional[bool],
    supports_query_selection: Optional[bool],
) -> dict[str, bool]:
    """Build QueryBuilder capability kwargs from connect-time cache."""
    return {
        "supports_server_compiled_ael": bool(supports_server_compiled_ael),
        "supports_query_selection": bool(supports_query_selection),
    }
