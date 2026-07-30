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

"""Tests for the SDK config loader: parsing, profiles, precedence, fail-soft."""

from datetime import timedelta

import pytest

from aerospike_sdk.policy.sdk_config_loader import (
    ENV_VAR,
    config_path_from_env,
    fill_hard_defaults,
    load_profiles,
    merge_settings,
    parse_duration,
    parse_sdk_config,
    resolve_for_cluster,
    resolve_from_env,
)
from aerospike_sdk.policy.system_settings import SystemSettings, TransactionSettings

_FULL = """
version: "1.0.0"
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 10
      maximumConnectionsPerNode: 300
      maximumSocketIdleTime: 55s
    circuitBreaker:
      numTendIntervalsInErrorWindow: 2
      maximumErrorsInErrorWindow: 100
    refresh:
      tendInterval: 1s
    transactions:
      implicitBatchWriteTransactions: true
      sleepBetweenAttempts: 1000ms
      numberOfAttempts: 5
"""


class TestParseDuration:
    """Suffix-form durations map to timedelta; anything else raises."""

    def test_milliseconds(self):
        assert parse_duration("250ms") == timedelta(milliseconds=250)

    def test_seconds(self):
        assert parse_duration("55s") == timedelta(seconds=55)

    def test_minutes(self):
        assert parse_duration("5m") == timedelta(minutes=5)

    def test_hours(self):
        assert parse_duration("2h") == timedelta(hours=2)

    def test_fractional(self):
        assert parse_duration("1.5s") == timedelta(milliseconds=1500)

    def test_days(self):
        assert parse_duration("2d") == timedelta(days=2)

    def test_long_spellings(self):
        assert parse_duration("250millis") == timedelta(milliseconds=250)
        assert parse_duration("5 minutes") == timedelta(minutes=5)
        assert parse_duration("100nanos") == timedelta(microseconds=0.1)

    def test_bare_number_rejected(self):
        with pytest.raises(ValueError):
            parse_duration("1000")

    def test_unknown_unit_rejected(self):
        with pytest.raises(ValueError):
            parse_duration("10y")

    def test_non_string_rejected(self):
        with pytest.raises(ValueError):
            parse_duration(1000)


class TestParseSdkConfig:
    """YAML text parses into per-profile SystemSettings, fail-soft per field."""

    def test_full_document(self):
        profiles = parse_sdk_config(_FULL)
        settings = profiles["DEFAULT"]
        assert settings.min_connections_per_node == 10
        assert settings.max_connections_per_node == 300
        assert settings.max_socket_idle_time == timedelta(seconds=55)
        assert settings.num_tend_intervals_in_error_window == 2
        assert settings.max_errors_in_error_window == 100
        assert settings.tend_interval == timedelta(seconds=1)
        assert settings.transactions.implicit_batch_write_transactions is True
        assert settings.transactions.sleep_between_attempts == timedelta(seconds=1)
        assert settings.transactions.number_of_attempts == 5

    def test_transactions_only(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    transactions:\n      implicitBatchWriteTransactions: false\n"
        )
        settings = profiles["DEFAULT"]
        assert settings.transactions.implicit_batch_write_transactions is False
        assert settings.transactions.number_of_attempts is None
        assert settings.max_connections_per_node is None

    def test_missing_system_section(self):
        assert parse_sdk_config('version: "1.0.0"\n') == {}

    def test_empty_document(self):
        assert parse_sdk_config("") == {}

    def test_malformed_yaml_raises(self):
        with pytest.raises(ValueError):
            parse_sdk_config("system: [unbalanced : bracket\n")

    def test_non_mapping_root_raises(self):
        with pytest.raises(ValueError):
            parse_sdk_config("- a\n- b\n")

    def test_unknown_section_ignored(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    nonsense:\n      key: 1\n"
            "    refresh:\n      tendInterval: 2s\n"
        )
        assert profiles["DEFAULT"].tend_interval == timedelta(seconds=2)

    def test_behaviors_section_ignored(self):
        profiles = parse_sdk_config(
            "behaviors:\n  fastReads: {}\n"
            "system:\n  DEFAULT:\n    refresh:\n      tendInterval: 2s\n"
        )
        assert profiles["DEFAULT"].tend_interval == timedelta(seconds=2)

    def test_unknown_key_ignored(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    connections:\n"
            "      maximumConnectionsPerNode: 40\n      bogusKey: 7\n"
        )
        assert profiles["DEFAULT"].max_connections_per_node == 40

    def test_bad_value_skipped_rest_applies(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    connections:\n"
            "      maximumConnectionsPerNode: not_a_number\n"
            "      minimumConnectionsPerNode: 5\n"
        )
        settings = profiles["DEFAULT"]
        assert settings.max_connections_per_node is None
        assert settings.min_connections_per_node == 5

    def test_bool_where_int_expected_skipped(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    transactions:\n      numberOfAttempts: true\n"
        )
        assert profiles["DEFAULT"].transactions.number_of_attempts is None

    def test_bad_duration_skipped(self):
        profiles = parse_sdk_config("system:\n  DEFAULT:\n    refresh:\n      tendInterval: fast\n")
        assert profiles["DEFAULT"].tend_interval is None


class TestResolveForCluster:
    """Cluster-name profile layers per-field on DEFAULT."""

    _PROFILES = parse_sdk_config(
        _FULL
        + "  prod-cluster:\n"
        + "    connections:\n      maximumConnectionsPerNode: 500\n"
        + "    transactions:\n      implicitBatchWriteTransactions: false\n"
    )

    def test_named_profile_wins_per_field(self):
        resolved = resolve_for_cluster(self._PROFILES, "prod-cluster")
        assert resolved.max_connections_per_node == 500
        assert resolved.transactions.implicit_batch_write_transactions is False

    def test_named_profile_falls_through_to_default(self):
        resolved = resolve_for_cluster(self._PROFILES, "prod-cluster")
        assert resolved.min_connections_per_node == 10
        assert resolved.transactions.number_of_attempts == 5

    def test_unknown_cluster_name_uses_default_only(self):
        resolved = resolve_for_cluster(self._PROFILES, "staging")
        assert resolved == self._PROFILES["DEFAULT"]

    def test_no_cluster_name_uses_default_only(self):
        resolved = resolve_for_cluster(self._PROFILES, None)
        assert resolved == self._PROFILES["DEFAULT"]

    def test_no_default_with_matching_profile(self):
        profiles = parse_sdk_config(
            "system:\n  prod-cluster:\n    refresh:\n      tendInterval: 3s\n"
        )
        resolved = resolve_for_cluster(profiles, "prod-cluster")
        assert resolved.tend_interval == timedelta(seconds=3)

    def test_no_applicable_profile(self):
        profiles = parse_sdk_config(
            "system:\n  prod-cluster:\n    refresh:\n      tendInterval: 3s\n"
        )
        assert resolve_for_cluster(profiles, None) is None


class TestMergeSettings:
    """Per-field, null-skipping merge; higher layer wins where it has a value."""

    def test_file_wins_where_set(self):
        file_layer = SystemSettings(max_connections_per_node=300)
        programmatic = SystemSettings(max_connections_per_node=50, conn_pools_per_node=4)
        merged = merge_settings(file_layer, programmatic)
        assert merged.max_connections_per_node == 300
        assert merged.conn_pools_per_node == 4

    def test_nested_transactions_merge(self):
        file_layer = SystemSettings(
            transactions=TransactionSettings(implicit_batch_write_transactions=False),
        )
        programmatic = SystemSettings(
            transactions=TransactionSettings(number_of_attempts=3),
        )
        merged = merge_settings(file_layer, programmatic)
        assert merged.transactions.implicit_batch_write_transactions is False
        assert merged.transactions.number_of_attempts == 3

    def test_absent_layers(self):
        settings = SystemSettings(max_connections_per_node=10)
        assert merge_settings(None, settings) is settings
        assert merge_settings(settings, None) is settings
        assert merge_settings(None, None) is None


class TestFillHardDefaults:
    """The bottom layer fills SDK-runtime defaults without touching set values."""

    def test_implicit_default_true(self):
        resolved = fill_hard_defaults(SystemSettings())
        assert resolved.transactions.implicit_batch_write_transactions is True

    def test_explicit_false_preserved(self):
        settings = SystemSettings(
            transactions=TransactionSettings(implicit_batch_write_transactions=False),
        )
        assert fill_hard_defaults(settings).transactions.implicit_batch_write_transactions is False

    def test_none_settings(self):
        resolved = fill_hard_defaults(None)
        assert resolved.transactions.implicit_batch_write_transactions is True
        assert resolved.max_connections_per_node is None

    def test_retry_defaults(self):
        resolved = fill_hard_defaults(SystemSettings())
        assert resolved.transactions.number_of_attempts == 5
        assert resolved.transactions.sleep_between_attempts == timedelta(seconds=1)

    def test_explicit_retry_values_preserved(self):
        settings = SystemSettings(
            transactions=TransactionSettings(
                number_of_attempts=2,
                sleep_between_attempts=timedelta(milliseconds=250),
            ),
        )
        resolved = fill_hard_defaults(settings)
        assert resolved.transactions.number_of_attempts == 2
        assert resolved.transactions.sleep_between_attempts == timedelta(milliseconds=250)
        assert resolved.transactions.implicit_batch_write_transactions is True


class TestEnvResolution:
    """AEROSPIKE_SDK_CONFIG_URL resolves to a path; only file sources apply."""

    def test_unset(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        assert config_path_from_env() is None

    def test_bare_path(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "/etc/aerospike/sdk.yaml")
        assert config_path_from_env() == "/etc/aerospike/sdk.yaml"

    def test_file_url(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "file:///etc/aerospike/sdk.yaml")
        assert config_path_from_env() == "/etc/aerospike/sdk.yaml"

    def test_unsupported_scheme_ignored(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "https://config.example.com/sdk.yaml")
        assert config_path_from_env() is None

    def test_resolve_from_env_full_pipeline(self, monkeypatch, tmp_path):
        path = tmp_path / "sdk.yaml"
        path.write_text(_FULL)
        monkeypatch.setenv(ENV_VAR, str(path))
        programmatic = SystemSettings(conn_pools_per_node=2, max_connections_per_node=50)
        settings, config_path = resolve_from_env(None, programmatic)
        assert config_path == str(path)
        assert settings.max_connections_per_node == 300
        assert settings.conn_pools_per_node == 2
        assert settings.transactions.implicit_batch_write_transactions is True

    def test_resolve_from_env_unset_uses_programmatic(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        programmatic = SystemSettings(max_connections_per_node=50)
        settings, config_path = resolve_from_env(None, programmatic)
        assert config_path is None
        assert settings.max_connections_per_node == 50
        assert settings.transactions.implicit_batch_write_transactions is True


class TestLoadProfilesFailSoft:
    """File-level problems return None (warn + continue), never raise."""

    def test_missing_file(self, tmp_path):
        assert load_profiles(str(tmp_path / "nope.yaml")) is None

    def test_malformed_file(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("system: [unbalanced : bracket\n")
        assert load_profiles(str(path)) is None

    def test_good_file(self, tmp_path):
        path = tmp_path / "good.yaml"
        path.write_text(_FULL)
        profiles = load_profiles(str(path))
        assert profiles is not None
        assert profiles["DEFAULT"].max_connections_per_node == 300
