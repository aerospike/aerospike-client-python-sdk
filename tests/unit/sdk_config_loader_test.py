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

import logging
from datetime import timedelta

import pytest

from aerospike_sdk.policy.sdk_config_loader import (
    parse_behaviors,
    load_at_connect,
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
      minimum_connections_per_node: 10
      maximum_connections_per_node: 300
      maximum_socket_idle_time: 55s
    circuit_breaker:
      num_tend_intervals_in_error_window: 2
      maximum_errors_in_error_window: 100
    refresh:
      tend_interval: 1s
    transactions:
      implicit_batch_write_transactions: true
      sleep_between_attempts: 1000ms
      number_of_attempts: 5
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
            "system:\n  DEFAULT:\n    transactions:\n      implicit_batch_write_transactions: false\n"
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
            "    refresh:\n      tend_interval: 2s\n"
        )
        assert profiles["DEFAULT"].tend_interval == timedelta(seconds=2)

    def test_behaviors_section_ignored(self):
        profiles = parse_sdk_config(
            "behaviors:\n  fastReads: {}\n"
            "system:\n  DEFAULT:\n    refresh:\n      tend_interval: 2s\n"
        )
        assert profiles["DEFAULT"].tend_interval == timedelta(seconds=2)

    def test_unknown_key_ignored(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    connections:\n"
            "      maximum_connections_per_node: 40\n      bogusKey: 7\n"
        )
        assert profiles["DEFAULT"].max_connections_per_node == 40

    def test_bad_value_skipped_rest_applies(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    connections:\n"
            "      maximum_connections_per_node: not_a_number\n"
            "      minimum_connections_per_node: 5\n"
        )
        settings = profiles["DEFAULT"]
        assert settings.max_connections_per_node is None
        assert settings.min_connections_per_node == 5

    def test_bool_where_int_expected_skipped(self):
        profiles = parse_sdk_config(
            "system:\n  DEFAULT:\n    transactions:\n      number_of_attempts: true\n"
        )
        assert profiles["DEFAULT"].transactions.number_of_attempts is None

    def test_bad_duration_skipped(self):
        profiles = parse_sdk_config("system:\n  DEFAULT:\n    refresh:\n      tend_interval: fast\n")
        assert profiles["DEFAULT"].tend_interval is None


class TestResolveForCluster:
    """Cluster-name profile layers per-field on DEFAULT."""

    _PROFILES = parse_sdk_config(
        _FULL
        + "  prod-cluster:\n"
        + "    connections:\n      maximum_connections_per_node: 500\n"
        + "    transactions:\n      implicit_batch_write_transactions: false\n"
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
            "system:\n  prod-cluster:\n    refresh:\n      tend_interval: 3s\n"
        )
        resolved = resolve_for_cluster(profiles, "prod-cluster")
        assert resolved.tend_interval == timedelta(seconds=3)

    def test_no_applicable_profile(self):
        profiles = parse_sdk_config(
            "system:\n  prod-cluster:\n    refresh:\n      tend_interval: 3s\n"
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


class TestUnrecognizedKeys:
    """An unrecognized key applies nothing, so it has to say so.

    The keys below are the previous on-disk spelling. They are the realistic
    failure: a config written against the old convention still parses, and
    without a warning the only evidence is settings that quietly never took
    effect.
    """

    LEGACY = """
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 10
    circuitBreaker:
      maximumErrorsInErrorWindow: 100
"""

    def test_legacy_keys_are_reported_not_applied(self, caplog):
        with caplog.at_level(logging.WARNING):
            profiles = parse_sdk_config(self.LEGACY)
        assert profiles["DEFAULT"].min_connections_per_node is None
        assert "minimumConnectionsPerNode" in caplog.text
        assert "circuitBreaker" in caplog.text

    def test_current_spelling_applies(self, caplog):
        current = (
            self.LEGACY.replace("minimumConnectionsPerNode", "minimum_connections_per_node")
            .replace("circuitBreaker", "circuit_breaker")
            .replace("maximumErrorsInErrorWindow", "maximum_errors_in_error_window")
        )
        with caplog.at_level(logging.WARNING):
            profiles = parse_sdk_config(current)
        assert profiles["DEFAULT"].min_connections_per_node == 10
        assert profiles["DEFAULT"].max_errors_in_error_window == 100
        assert "unrecognized" not in caplog.text

    METRICS_CAMEL = """
system:
  DEFAULT:
    metrics:
      enabled: true
      latencyUnit: microseconds
      latencyColumns: 18
      reportDir: /tmp/x
      exportInterval: 5s
      sampler:
        Range: 100
        Threshold: 10
      extended:
        usage:
          Enabled: true
"""

    def test_camel_case_metrics_keys_are_reported_not_applied(self, caplog):
        """snake_case only -- no camelCase aliases, per the cross-SDK naming rule.

        Java may alias camelCase while migrating; Python does not, so a config
        written against the Java programmatic names has to be told it did
        nothing rather than silently collecting with default settings.
        """
        with caplog.at_level(logging.WARNING):
            metrics = parse_sdk_config(self.METRICS_CAMEL)["DEFAULT"].metrics
        # The one snake_case key applies; every camelCase sibling does not.
        assert metrics.enabled is True
        assert metrics.latency_unit is None
        assert metrics.latency_columns is None
        assert metrics.report_dir is None
        assert metrics.export_interval is None
        assert metrics.sampler_range is None
        assert metrics.sampler_threshold is None
        assert metrics.usage_enabled is None
        for key in ("latencyUnit", "latencyColumns", "reportDir", "exportInterval",
                    "Range", "Threshold", "Enabled"):
            assert key in caplog.text, f"{key} was dropped without saying so"

    def test_every_drop_names_its_profile(self, caplog):
        """A multi-profile file has to say which profile the bad key is in."""
        doc = """
system:
  prod:
    metrics:
      latencyUnit: microseconds
  staging:
    connections:
      minimumConnectionsPerNode: 10
"""
        with caplog.at_level(logging.WARNING):
            parse_sdk_config(doc)
        assert "prod.metrics.latencyUnit" in caplog.text
        assert "staging.connections.minimumConnectionsPerNode" in caplog.text

    def test_strict_mode_raises_on_a_camel_case_key(self):
        """`strict=True` turns the warning into a failure naming the key."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "sdk.yaml")
            with open(path, "w") as handle:
                handle.write(self.METRICS_CAMEL)
            previous = os.environ.get("AEROSPIKE_SDK_CONFIG_URL")
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = path
            try:
                with pytest.raises(ValueError) as excinfo:
                    load_at_connect(None, None, strict=True)
                assert "latencyUnit" in str(excinfo.value)
            finally:
                if previous is None:
                    os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
                else:
                    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = previous

    def test_extended_block_warns(self, caplog):
        """`extended.*` is deliberately not in the schema; it must not look accepted."""
        doc = """
system:
  DEFAULT:
    metrics:
      extended:
        operational:
          enabled: true
"""
        with caplog.at_level(logging.WARNING):
            parse_sdk_config(doc)
        assert "metrics" in caplog.text

    def test_unknown_behavior_block_warns(self, caplog):
        doc = """
behaviors:
  b:
    allOperations:
      abandon_call_after: 1s
"""
        with caplog.at_level(logging.WARNING):
            specs = parse_behaviors(doc)
        assert specs["b"].patches == {}
        assert "allOperations" in caplog.text


class TestStrictConfig:
    """Strict mode turns the warning into a connect-time failure."""

    BAD = """
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 10
"""

    def test_strict_raises_naming_the_key(self, tmp_path, monkeypatch):
        cfg = tmp_path / "sdk.yaml"
        cfg.write_text(self.BAD)
        monkeypatch.setenv("AEROSPIKE_SDK_CONFIG_URL", str(cfg))
        with pytest.raises(ValueError, match="minimumConnectionsPerNode"):
            load_at_connect(None, None, strict=True)

    def test_default_is_fail_soft(self, tmp_path, monkeypatch):
        cfg = tmp_path / "sdk.yaml"
        cfg.write_text(self.BAD)
        monkeypatch.setenv("AEROSPIKE_SDK_CONFIG_URL", str(cfg))
        settings, path, _raw = load_at_connect(None, None)
        assert settings is not None
        assert path == str(cfg)

    def test_strict_accepts_a_clean_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "sdk.yaml"
        cfg.write_text(self.BAD.replace(
            "minimumConnectionsPerNode", "minimum_connections_per_node"))
        monkeypatch.setenv("AEROSPIKE_SDK_CONFIG_URL", str(cfg))
        settings, _path, _raw = load_at_connect(None, None, strict=True)
        assert settings.min_connections_per_node == 10


class TestMetricsSection:
    """The ``metrics`` block builds a MetricsSettings group."""

    FULL = """
system:
  DEFAULT:
    metrics:
      enabled: true
      latency_unit: microseconds
      latency_columns: 18
      latency_shift: 2
      sampler:
        range: 1000
        threshold: 100
      labels:
        owner: platform-team
"""

    def test_full_block(self):
        m = parse_sdk_config(self.FULL)["DEFAULT"].metrics
        assert m.enabled is True
        assert m.latency_unit == "microseconds"
        assert m.latency_columns == 18
        assert m.latency_shift == 2
        assert m.sampler_range == 1000
        assert m.sampler_threshold == 100
        assert m.labels == {"owner": "platform-team"}

    def test_usage_counters_come_from_the_extended_group(self):
        """``metrics.extended.usage.enabled`` is the cross-SDK key for them."""
        text = ("system:\n  DEFAULT:\n    metrics:\n      enabled: true\n"
                "      extended:\n        usage:\n          enabled: true\n")
        assert parse_sdk_config(text)["DEFAULT"].metrics.usage_enabled is True

    def test_usage_defaults_to_unset_when_the_group_is_absent(self):
        m = parse_sdk_config(self.FULL)["DEFAULT"].metrics
        assert m.usage_enabled is None

    def test_extended_operational_is_rejected_but_usage_still_lands(self, caplog):
        """The core cannot split Tier 0 from operational, so that key is a drop.

        Accepting it silently would promise a split the snapshot cannot deliver;
        the sibling ``usage`` group is unaffected because this SDK records those
        counters itself.
        """
        text = ("system:\n  DEFAULT:\n    metrics:\n      enabled: true\n"
                "      extended:\n        usage:\n          enabled: true\n"
                "        operational:\n          enabled: true\n")
        with caplog.at_level(logging.WARNING):
            m = parse_sdk_config(text)["DEFAULT"].metrics
        assert m.usage_enabled is True
        assert "metrics.extended.operational" in caplog.text

    def test_absent_block_leaves_everything_unset(self):
        m = parse_sdk_config("system:\n  DEFAULT:\n    connections:\n"
                             "      minimum_connections_per_node: 1\n")["DEFAULT"].metrics
        assert m.enabled is None
        assert m.latency_unit is None

    def test_bad_latency_unit_is_skipped_not_fatal(self, caplog):
        with caplog.at_level(logging.WARNING):
            m = parse_sdk_config(
                "system:\n  DEFAULT:\n    metrics:\n"
                "      enabled: true\n      latency_unit: fortnights\n")["DEFAULT"].metrics
        assert m.enabled is True          # the rest of the block still applies
        assert m.latency_unit is None
        assert "fortnights" in caplog.text

    def test_extended_group_is_not_a_recognized_key(self, caplog):
        """The spec nests shape keys under `extended`; we do not, so it must warn."""
        with caplog.at_level(logging.WARNING):
            parse_sdk_config(
                "system:\n  DEFAULT:\n    metrics:\n      extended:\n"
                "        operational:\n          enabled: true\n")
        assert "metrics.extended" in caplog.text

    def test_policy_round_trip(self):
        from aerospike_sdk import LatencyUnit
        from aerospike_sdk.metrics import policy_from_settings
        policy = policy_from_settings(parse_sdk_config(self.FULL)["DEFAULT"].metrics)
        assert policy.latency_unit == LatencyUnit.MICROSECONDS
        assert policy.latency_columns == 18
        assert policy.latency_shift == 2
        assert policy.labels == [{"owner": "platform-team"}]

    def test_metrics_merges_per_field(self):
        higher = parse_sdk_config(
            "system:\n  c1:\n    metrics:\n      latency_columns: 24\n")["c1"]
        lower = parse_sdk_config(self.FULL)["DEFAULT"]
        merged = merge_settings(higher, lower)
        assert merged.metrics.latency_columns == 24        # higher wins
        assert merged.metrics.latency_unit == "microseconds"  # falls through

