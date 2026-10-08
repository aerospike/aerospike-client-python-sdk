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

"""Planner tests for collection-index CDT predicates.

Which predicates still run with ``allow_scans_with_where=False`` follows from
the plan: anything that plans onto a collection index does, and anything that
falls back to the primary index is refused. On this fixture the planner serves:

* ``.exists()`` (bare or ``== true``) on a map key, a map value, or a list value;
* integer-bound range selectors ``[=lo:hi]``, ``{@lo:hi}`` and ``{=lo:hi}``;
* ``'v' in`` a top-level or CDT-context list;
* ``.count() > 0``.

These fall back to a scan, so disallowing scans refuses them:

* positional existence such as ``[0].exists()``;
* range selectors with string bounds;
* ``.count() == 0`` and ``.exists() == false``.

PAC's query plan does not expose the index-range bytes, so plans are
checked by selection, index name and collection type only.
"""

from __future__ import annotations

import pytest

from aerospike_sdk import CollectionIndexType, DataSet

from tests.integration.query_selection_helpers import (
    CDT_INT_LIST_BIN,
    CDT_INT_LIST_INDEX,
    CDT_INT_MAP_BIN,
    CDT_INT_MAP_KEYS_INDEX,
    CDT_INT_MAP_VALUES_INDEX,
    CDT_LIST_BIN,
    CDT_LIST_RANGE,
    CDT_LIST_STR_BIN,
    CDT_LIST_STR_INDEX,
    CDT_LIST_STR_TARGET,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_MAP_KEY_RANGE,
    CDT_MAP_VALUE_RANGE,
    CDT_MAP_VALUE_TARGET,
    CDT_MAP_VALUES_INDEX,
    CDT_NESTED_BIN,
    CDT_NESTED_INDEX,
    CDT_NESTED_KEY,
    CDT_NESTED_TARGET,
    CDT_SET_NAME,
    CDT_SIZE,
    CDT_STR_MAP_KEY_RANGE,
    CDT_STR_MAP_VALUE_RANGE,
    NS,
    QuerySelection,
    count_matches_async,
    explain_plan_async,
)
from tests.pac_compat import requires_query_selection

CDT_DS = DataSet.of(NS, CDT_SET_NAME)
HALF = CDT_SIZE // 2

_MAP_KEY_EXISTS = f"$.{CDT_MAP_BIN}.{CDT_MAP_KEY}.exists()"
_MAP_VALUE_EXISTS = f"$.{CDT_MAP_BIN}.{{={CDT_MAP_VALUE_TARGET}}}.exists()"
_LIST_VALUE_EXISTS = f"$.{CDT_LIST_STR_BIN}.[={CDT_LIST_STR_TARGET}].exists()"
_LIST_POSITION_EXISTS = f"$.{CDT_LIST_BIN}.[0].exists()"
_LIST_RANGE = f"[={CDT_LIST_RANGE[0]}:{CDT_LIST_RANGE[1]}]"
_INT_LIST_RANGE_EXISTS = f"$.{CDT_INT_LIST_BIN}.{_LIST_RANGE}.exists()"
_INT_MAP_KEY_RANGE_EXISTS = (
    f"$.{CDT_INT_MAP_BIN}.{{@{CDT_MAP_KEY_RANGE[0]}:{CDT_MAP_KEY_RANGE[1]}}}.exists()"
)
_INT_MAP_VALUE_RANGE_EXISTS = (
    f"$.{CDT_INT_MAP_BIN}.{{={CDT_MAP_VALUE_RANGE[0]}:{CDT_MAP_VALUE_RANGE[1]}}}.exists()"
)
_STR_MAP_KEY_RANGE_EXISTS = (
    f"$.{CDT_MAP_BIN}.{{@{CDT_STR_MAP_KEY_RANGE[0]}:{CDT_STR_MAP_KEY_RANGE[1]}}}.exists()"
)
_STR_MAP_VALUE_RANGE_EXISTS = (
    f"$.{CDT_MAP_BIN}.{{={CDT_STR_MAP_VALUE_RANGE[0]}:{CDT_STR_MAP_VALUE_RANGE[1]}}}.exists()"
)
_IN_LIST = f"'{CDT_LIST_STR_TARGET}' in $.{CDT_LIST_STR_BIN}"
_IN_NESTED_LIST = f"'{CDT_NESTED_TARGET}' in $.{CDT_NESTED_BIN}.{CDT_NESTED_KEY}"
_NESTED_VALUE_EXISTS = (
    f"$.{CDT_NESTED_BIN}.{CDT_NESTED_KEY}.[={CDT_NESTED_TARGET}].exists()"
)
_POSITIVE_COUNT = f"$.{CDT_INT_LIST_BIN}.{_LIST_RANGE}.count() > 0"
_ZERO_COUNT = f"$.{CDT_INT_LIST_BIN}.{_LIST_RANGE}.count() == 0"
_MAP_KEY_NOT_EXISTS = f"{_MAP_KEY_EXISTS} == false"


def _in_range(values, bounds) -> bool:
    """AEL interval selectors are begin-inclusive and end-exclusive."""
    return any(bounds[0] <= v < bounds[1] for v in values)


async def _assert_plan(cluster, where, index_name, collection_type):
    plan = await explain_plan_async(
        cluster.client.underlying_client, where, set_name=CDT_SET_NAME,
    )
    if index_name is None:
        assert plan.selection == QuerySelection.PRIMARY_INDEX
        assert plan.index_name is None
    else:
        assert plan.selection == QuerySelection.SECONDARY_INDEX
        assert plan.index_name == index_name
        assert plan.index_type == collection_type


_EXISTS_SHAPES = [
    pytest.param(_MAP_KEY_EXISTS, CDT_MAP_INDEX, CollectionIndexType.MAP_KEYS, id="map_key"),
    pytest.param(
        _MAP_VALUE_EXISTS, CDT_MAP_VALUES_INDEX, CollectionIndexType.MAP_VALUES, id="map_value",
    ),
    pytest.param(_LIST_VALUE_EXISTS, CDT_LIST_STR_INDEX, CollectionIndexType.LIST, id="list_value"),
    pytest.param(_LIST_POSITION_EXISTS, None, None, id="list_position"),
]


class TestQueryPlannerCollectionCdt:
    @requires_query_selection
    @pytest.mark.parametrize("eq_true", [False, True], ids=["bare", "eq_true"])
    @pytest.mark.parametrize(("exists", "index_name", "collection_type"), _EXISTS_SHAPES)
    async def test_plan_exists_unwraps_comparison_against_true(
        self, query_selection_cluster, eq_true, exists, index_name, collection_type,
    ):
        """Bare ``.exists()`` and ``.exists() == true`` share a plan.

        A positional list path stays on the primary index: a value index
        cannot answer whether position 0 exists.
        """
        where = f"{exists} == true" if eq_true else exists
        await _assert_plan(query_selection_cluster, where, index_name, collection_type)

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "index_name", "collection_type"),
        [
            pytest.param(
                _INT_LIST_RANGE_EXISTS, CDT_INT_LIST_INDEX, CollectionIndexType.LIST,
                id="list_values",
            ),
            pytest.param(
                _INT_MAP_KEY_RANGE_EXISTS, CDT_INT_MAP_KEYS_INDEX, CollectionIndexType.MAP_KEYS,
                id="map_keys",
            ),
            pytest.param(
                _INT_MAP_VALUE_RANGE_EXISTS, CDT_INT_MAP_VALUES_INDEX,
                CollectionIndexType.MAP_VALUES, id="map_values",
            ),
        ],
    )
    async def test_plan_integer_range_exists_selects_collection_index(
        self, query_selection_cluster, where, index_name, collection_type,
    ):
        await _assert_plan(query_selection_cluster, where, index_name, collection_type)

    @requires_query_selection
    @pytest.mark.parametrize(
        "where",
        [
            pytest.param(_STR_MAP_KEY_RANGE_EXISTS, id="map_keys"),
            pytest.param(_STR_MAP_VALUE_RANGE_EXISTS, id="map_values"),
        ],
    )
    async def test_string_bound_range_selectors_fall_back_to_primary_index(
        self, query_selection_cluster, where,
    ):
        """String bounds leave the probe unresolved, so the range runs as a residual.

        The planner parses interval bounds as integers. The row set is the
        same either way; only the access path differs, which is why disallowing
        scans refuses them.
        """
        await _assert_plan(query_selection_cluster, where, None, None)
        count = await count_matches_async(
            query_selection_cluster.session, CDT_DS, where, CDT_MAP_BIN, needs_scan=True,
        )
        assert count == HALF

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "index_name"),
        [
            pytest.param(_IN_LIST, CDT_LIST_STR_INDEX, id="top_level"),
            pytest.param(_IN_NESTED_LIST, CDT_NESTED_INDEX, id="ctx_nested"),
        ],
    )
    async def test_plan_in_selects_top_level_and_nested_list_indexes(
        self, query_selection_cluster, where, index_name,
    ):
        """``in`` has its own planner opcode, and the nested index carries a CDT context."""
        await _assert_plan(query_selection_cluster, where, index_name, CollectionIndexType.LIST)

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "index_name", "collection_type"),
        [
            pytest.param(
                _NESTED_VALUE_EXISTS, CDT_NESTED_INDEX, CollectionIndexType.LIST,
                id="nested_exists",
            ),
            pytest.param(
                _POSITIVE_COUNT, CDT_INT_LIST_INDEX, CollectionIndexType.LIST,
                id="positive_count",
            ),
            pytest.param(
                f"true == {_MAP_KEY_EXISTS}", CDT_MAP_INDEX, CollectionIndexType.MAP_KEYS,
                id="reversed_exists",
            ),
            pytest.param(_ZERO_COUNT, None, None, id="zero_count"),
            pytest.param(_MAP_KEY_NOT_EXISTS, None, None, id="exists_false"),
        ],
    )
    async def test_plan_containment_implication_boundaries(
        self, query_selection_cluster, where, index_name, collection_type,
    ):
        """Only predicates that imply containment can drive an index.

        A positive count or a true ``exists()`` implies the value is present;
        a zero count or a false ``exists()`` does not, so those stay residual.
        """
        await _assert_plan(query_selection_cluster, where, index_name, collection_type)

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "bin_name", "check"),
        [
            pytest.param(
                _INT_LIST_RANGE_EXISTS, CDT_INT_LIST_BIN,
                lambda values: _in_range(values, CDT_LIST_RANGE), id="list_values",
            ),
            pytest.param(
                _INT_MAP_KEY_RANGE_EXISTS, CDT_INT_MAP_BIN,
                lambda m: _in_range(m.keys(), CDT_MAP_KEY_RANGE), id="map_keys",
            ),
            pytest.param(
                _INT_MAP_VALUE_RANGE_EXISTS, CDT_INT_MAP_BIN,
                lambda m: _in_range(m.values(), CDT_MAP_VALUE_RANGE), id="map_values",
            ),
        ],
    )
    async def test_execute_range_selectors_without_for_bin_return_matching_rows(
        self, query_selection_cluster, where, bin_name, check,
    ):
        """Integer ranges are index-served, so they run with scans disallowed."""
        count = await count_matches_async(
            query_selection_cluster.session, CDT_DS, where, bin_name,
            needs_scan=False, check=check,
        )
        assert count == HALF

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "bin_name", "check", "needs_scan", "expected"),
        [
            pytest.param(
                _MAP_KEY_EXISTS, CDT_MAP_BIN, lambda m: CDT_MAP_KEY in m, False, HALF,
                id="map_key",
            ),
            pytest.param(
                _MAP_VALUE_EXISTS, CDT_MAP_BIN, lambda m: CDT_MAP_VALUE_TARGET in m.values(),
                False, HALF, id="map_value",
            ),
            pytest.param(
                _LIST_VALUE_EXISTS, CDT_LIST_STR_BIN, lambda values: CDT_LIST_STR_TARGET in values,
                False, HALF, id="list_value",
            ),
            pytest.param(
                _LIST_POSITION_EXISTS, CDT_LIST_BIN, lambda values: len(values) == 1,
                True, CDT_SIZE, id="list_position",
            ),
        ],
    )
    async def test_execute_cdt_exists_without_for_bin_returns_matching_rows(
        self, query_selection_cluster, where, bin_name, check, needs_scan, expected,
    ):
        """Value existence is index-served; positional existence needs the scan fallback."""
        count = await count_matches_async(
            query_selection_cluster.session, CDT_DS, where, bin_name,
            needs_scan=needs_scan, check=check,
        )
        assert count == expected

    @requires_query_selection
    @pytest.mark.parametrize(
        ("where", "bin_name", "needs_scan"),
        [
            pytest.param(_IN_LIST, CDT_LIST_STR_BIN, False, id="in_top_level"),
            pytest.param(_IN_NESTED_LIST, CDT_NESTED_BIN, False, id="in_ctx_nested"),
            pytest.param(_NESTED_VALUE_EXISTS, CDT_NESTED_BIN, False, id="nested_exists"),
            pytest.param(_POSITIVE_COUNT, CDT_INT_LIST_BIN, False, id="positive_count"),
            pytest.param(_ZERO_COUNT, CDT_INT_LIST_BIN, True, id="zero_count"),
            pytest.param(_MAP_KEY_NOT_EXISTS, CDT_MAP_BIN, True, id="exists_false"),
        ],
    )
    async def test_execute_in_nested_and_count_predicates_return_golden_rows(
        self, query_selection_cluster, where, bin_name, needs_scan,
    ):
        """Index-served and scan-planned predicates return the same golden half of the rows.

        Candidate extraction changes the access path without changing what the
        residual filter returns. The scan-planned cases are the ones the plan
        tests above put on the primary index, and only they need the scan fallback.
        """
        count = await count_matches_async(
            query_selection_cluster.session, CDT_DS, where, bin_name,
            needs_scan=needs_scan,
        )
        assert count == HALF
