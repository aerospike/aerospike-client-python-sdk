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

"""Top-K smoke test through the blocking dispatch, on both dataset query paths."""

import pytest

from aerospike_sdk import BinDataType, DataSet, Order
from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_query_selection, requires_query_top_k

_DS = DataSet.of(general_namespace(), "vector_top_k_sync")


@pytest.fixture(scope="module")
def session(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        s = cluster.create_session()
        for i in range(8):
            s.upsert(_DS.id(i)).put({"price": 10.0 * i, "in_stock": i % 2}).execute()
        yield s
        for i in range(8):
            s.delete(_DS.id(i)).execute()


@requires_query_top_k
@pytest.mark.parametrize(
    "where, expected",
    [
        (None, [70.0, 60.0, 50.0]),
        pytest.param(
            "$.in_stock == 1", [70.0, 50.0, 30.0], marks=requires_query_selection,
        ),
    ],
    ids=["query", "explain_then_plan"],
)
def test_ranked_query(session, where, expected):
    builder = session.query(_DS)
    if where is not None:
        builder = builder.where(where)
    stream = builder.order_by("price", BinDataType.DOUBLE, Order.DESC).top_k(3).execute()
    assert [row.record_or_raise().bins["price"] for row in stream] == expected
