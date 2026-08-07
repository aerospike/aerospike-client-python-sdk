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

"""Unit tests for the Mode-axis ``requires_mode`` marker and its skip decision.

A bug in skip-enforcement removes coverage *silently* rather than failing loudly, so the
decision logic and the fixture wiring are both pinned here — no live cluster needed.
"""

import pytest

from tests.integration.namespace import requires_mode, requires_mode_skip_reason

pytest_plugins = ["pytester"]


class TestSkipReason:
    def test_ap_on_ap_runs(self):
        assert requires_mode_skip_reason("ap", is_sc=False) is None

    def test_sc_on_sc_runs(self):
        assert requires_mode_skip_reason("sc", is_sc=True) is None

    def test_ap_on_sc_skips(self):
        reason = requires_mode_skip_reason("ap", is_sc=True)
        assert reason is not None and "requires 'ap'" in reason and "'sc'" in reason

    def test_sc_on_ap_skips(self):
        reason = requires_mode_skip_reason("sc", is_sc=False)
        assert reason is not None and "requires 'sc'" in reason and "'ap'" in reason


class TestMarker:
    def test_returns_named_marker(self):
        marker = requires_mode("ap")
        assert marker.name == "requires_mode"
        assert marker.args == ("ap",)

    def test_rejects_unknown_mode(self):
        with pytest.raises(ValueError):
            requires_mode("nope")


_ENFORCE_CONFTEST = '''
import pytest
from tests.integration.namespace import requires_mode_skip_reason


def pytest_configure(config):
    config.addinivalue_line("markers", "requires_mode(mode): mode gate")


@pytest.fixture
def general_namespace_is_sc(request):
    return bool(getattr(request.module, "IS_SC", False))


@pytest.fixture(autouse=True)
def _enforce_requires_mode(request):
    marker = request.node.get_closest_marker("requires_mode")
    if marker is None:
        return
    reason = requires_mode_skip_reason(
        marker.args[0], request.getfixturevalue("general_namespace_is_sc"))
    if reason:
        pytest.skip(reason)
'''


class TestEnforcement:
    def test_runs_and_skips_by_mode(self, pytester):
        pytester.makeconftest(_ENFORCE_CONFTEST)
        pytester.makepyfile(
            """
            import pytest
            IS_SC = False  # AP mode
            @pytest.mark.requires_mode("ap")
            def test_ap_runs(): assert True
            @pytest.mark.requires_mode("sc")
            def test_sc_skips(): assert True
            def test_unmarked_runs(): assert True
            """,
        )
        # Sub-tests are sync; disable pytest-asyncio so its unset-loop-scope deprecation
        # warning isn't emitted (and attributed) here.
        result = pytester.runpytest("-p", "no:cacheprovider", "-p", "no:asyncio")
        result.assert_outcomes(passed=2, skipped=1)

    def test_unmarked_never_resolves_mode(self, pytester):
        # Laziness: an unmarked test must not resolve general_namespace_is_sc — here the
        # fixture raises if resolved, so the unmarked test passing proves it was skipped.
        pytester.makeconftest(
            '''
            import pytest
            from tests.integration.namespace import requires_mode_skip_reason

            def pytest_configure(config):
                config.addinivalue_line("markers", "requires_mode(mode): mode gate")

            @pytest.fixture
            def general_namespace_is_sc():
                raise AssertionError("resolved for an unmarked test")

            @pytest.fixture(autouse=True)
            def _enforce_requires_mode(request):
                marker = request.node.get_closest_marker("requires_mode")
                if marker is None:
                    return
                reason = requires_mode_skip_reason(
                    marker.args[0], request.getfixturevalue("general_namespace_is_sc"))
                if reason:
                    pytest.skip(reason)
            ''',
        )
        pytester.makepyfile("def test_unmarked_runs(): assert True")
        # Sub-tests are sync; disable pytest-asyncio so its unset-loop-scope deprecation
        # warning isn't emitted (and attributed) here.
        result = pytester.runpytest("-p", "no:cacheprovider", "-p", "no:asyncio")
        result.assert_outcomes(passed=1)
