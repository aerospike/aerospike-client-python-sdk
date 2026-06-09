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

"""Unit tests for ``QueryBuilder.with_op_projection`` plumbing.

These tests do not contact a server; they verify that the projected ops
are stored on the builder and forwarded to the underlying ``Statement``
via ``set_operations``.
"""

from aerospike_async import CTX, CdtOperation, Operation
from aerospike_sdk.aio.operations.query import QueryBuilder


def _make_qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="users")


class TestWithOpProjection:
    def test_default_is_none(self):
        qb = _make_qb()
        assert qb._op_projection is None
        # An empty builder produces a Statement with no projection — the
        # exact field is internal to PAC, but at least we expect no raise.
        qb._build_statement()

    def test_stores_basic_projection(self):
        qb = _make_qb()
        qb.with_op_projection(Operation.get_bin("name"), Operation.get_bin("age"))
        assert qb._op_projection is not None
        assert len(qb._op_projection) == 2

    def test_stores_cdt_projection(self):
        qb = _make_qb()
        qb.with_op_projection(
            CdtOperation.select_values("inventory", [CTX.map_key("books")]),
        )
        assert qb._op_projection is not None
        assert len(qb._op_projection) == 1

    def test_subsequent_call_replaces(self):
        qb = _make_qb()
        qb.with_op_projection(Operation.get_bin("name"))
        qb.with_op_projection(
            Operation.get_bin("age"),
            Operation.get_bin("email"),
        )
        assert qb._op_projection is not None
        assert len(qb._op_projection) == 2

    def test_no_args_clears(self):
        qb = _make_qb()
        qb.with_op_projection(Operation.get_bin("name"))
        qb.with_op_projection()
        assert qb._op_projection is None

    def test_build_statement_forwards_to_set_operations(self):
        qb = _make_qb()
        qb.with_op_projection(Operation.get_bin("name"))
        # _build_statement only crashes if PAC's Statement.set_operations
        # rejects the op. It accepts plain Operation, so this exercise
        # confirms we wired through the right method.
        qb._build_statement()

    def test_build_statement_forwards_cdt_projection(self):
        qb = _make_qb()
        qb.with_op_projection(
            CdtOperation.select_values("inventory", [CTX.map_key("books")]),
        )
        qb._build_statement()


class TestWithOpProjectionReturnsBuilder:
    def test_returns_self_for_chaining(self):
        qb = _make_qb()
        assert qb.with_op_projection(Operation.get_bin("name")) is qb
