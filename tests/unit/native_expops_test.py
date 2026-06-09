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

"""Wire-shape tests for the SDK's ``in_list`` / ``map_keys`` / ``map_values``
pass-throughs.

These helpers are now thin wrappers around the native PAC ``FilterExpression``
ExpOps introduced in server 8.1.2 (see the design spec
``[PRD] DX: enhance expression usability``). The tests pin the wire form via
base64 round-trip so any divergence between the SDK pass-through and the
canonical PAC factory shows up immediately.
"""

import inspect

import pytest
from aerospike_async import FilterExpression as Pac
from aerospike_sdk.exp import Exp, in_list, map_keys, map_values


class TestInList:
    """``in_list`` is a thin pass-through to the native PAC opcode."""

    def test_emits_native_in_list_opcode(self):
        sdk = in_list(Exp.string_val("admin"), Exp.list_bin("roles"))
        native = Pac.in_list(Pac.string_val("admin"), Pac.list_bin("roles"))
        assert sdk.base64() == native.base64()

    def test_signature_has_no_ctx_param(self):
        # The PRD-canonical signature is ``in_list(value, list)``. A ``ctx``
        # kwarg here would be a PSDK invention with no analog in the spec —
        # nested-CDT navigation is meant to live at the CTX layer instead
        # (``CTX.map_keys_in`` / ``CTX.and_filter``).
        params = list(inspect.signature(in_list).parameters)
        assert params == ["value", "list_exp"]


class TestMapKeys:
    """``map_keys`` is a thin pass-through to the native PAC opcode."""

    def test_emits_native_map_keys_opcode(self):
        sdk = map_keys(Exp.map_bin("scores"))
        native = Pac.map_keys(Pac.map_bin("scores"))
        assert sdk.base64() == native.base64()

    def test_signature_has_no_ctx_param(self):
        params = list(inspect.signature(map_keys).parameters)
        assert params == ["map_exp"]


class TestMapValues:
    """``map_values`` is a thin pass-through to the native PAC opcode."""

    def test_emits_native_map_values_opcode(self):
        sdk = map_values(Exp.map_bin("scores"))
        native = Pac.map_values(Pac.map_bin("scores"))
        assert sdk.base64() == native.base64()

    def test_signature_has_no_ctx_param(self):
        params = list(inspect.signature(map_values).parameters)
        assert params == ["map_exp"]


class TestPriorCtxKwargRemoved:
    """The previous swap-on-``ctx`` kwarg is gone — calling with ``ctx=`` errors.

    Pre-0.9.0-alpha.2 PSDK accepted a ``ctx`` kwarg that silently routed to a
    legacy compositional shim. That kwarg has been removed (PRD alignment;
    the canonical API has no ``ctx`` parameter). Test the breaking-change
    fence so anyone updating from the old behavior gets a clear ``TypeError``.
    """

    def test_in_list_rejects_ctx_kwarg(self):
        with pytest.raises(TypeError):
            in_list(Exp.string_val("a"), Exp.list_bin("b"), ctx=[])  # type: ignore[call-arg]

    def test_map_keys_rejects_ctx_kwarg(self):
        with pytest.raises(TypeError):
            map_keys(Exp.map_bin("b"), ctx=[])  # type: ignore[call-arg]

    def test_map_values_rejects_ctx_kwarg(self):
        with pytest.raises(TypeError):
            map_values(Exp.map_bin("b"), ctx=[])  # type: ignore[call-arg]
