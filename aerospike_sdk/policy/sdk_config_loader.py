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

"""Loader for the SDK-level configuration file (``AEROSPIKE_SDK_CONFIG_URL``).

Parses the YAML ``system:`` section into per-profile
:class:`~aerospike_sdk.policy.system_settings.SystemSettings` and resolves the
effective settings for a cluster by layering, highest to lowest:

1. file cluster-name profile (``system.<cluster-name>``)
2. file ``DEFAULT`` profile (``system.DEFAULT``)
3. programmatic settings (``with_system_settings``)
4. hard defaults

Merging is per-field and null-skipping at every layer. All file handling is
fail-soft: a missing, unreadable, or malformed file never prevents a client
from connecting; a single unparseable field is skipped while the rest of the
file still applies.

On-disk keys are ``snake_case``, the canonical cross-SDK config format, and are
translated to the SDK's own field names on read; neither spelling leaks into the
API surface.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
from contextvars import ContextVar
from dataclasses import dataclass, field, fields, replace
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Mapping, Optional

import yaml
from aerospike_async import ReadModeAP, ReadModeSC, Replica

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_registry import get_behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from aerospike_sdk.policy.system_settings import (
    MetricsSettings,
    SystemSettings,
    TransactionSettings,
)

log = logging.getLogger(SdkLoggers.BEHAVIOR)

# Keys and blocks the loader did not recognize, collected for the duration of one
# parse. Warning on each is what makes a stale or mistyped config visible at all:
# the settings it carries simply do not apply, and without a warning the file
# looks like it worked. Strict mode turns that report into a connect-time error;
# it is deliberately a ContextVar rather than a parameter threaded through five
# functions, and deliberately unset on the reload path, which must stay
# fail-soft because a raise in the background monitor has nowhere to go.
_drops: ContextVar[Optional[List[str]]] = ContextVar("_sdk_config_drops", default=None)


def _record_drop(description: str) -> None:
    """Warn that one key or block was not recognized, and note it for strict mode."""
    log.warning("SDK config: unrecognized %s; ignored", description)
    collected = _drops.get()
    if collected is not None:
        collected.append(description)


@contextlib.contextmanager
def _collect_drops() -> Iterator[List[str]]:
    """Collect unrecognized keys reported during one parse."""
    collected: List[str] = []
    token = _drops.set(collected)
    try:
        yield collected
    finally:
        _drops.reset(token)

# Prefer PyYAML's libyaml (C) loader when the wheel bundles it; the pure-Python
# SafeLoader is the drop-in fallback. Both are safe loaders (no object tags).
_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

ENV_VAR = "AEROSPIKE_SDK_CONFIG_URL"

DEFAULT_PROFILE = "DEFAULT"

# Hard defaults — the bottom precedence layer, applied after all merging.
_HARD_DEFAULT_IMPLICIT_BATCH_WRITE_TXNS = True
_HARD_DEFAULT_TXN_SLEEP_BETWEEN_ATTEMPTS = timedelta(seconds=1)
_HARD_DEFAULT_TXN_NUMBER_OF_ATTEMPTS = 5

# Multi-character alternatives listed before their single-character prefixes
# so e.g. "ms" is never consumed as "m".
_DURATION_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*"
    r"(nanoseconds?|nanos?|ns|microseconds?|micros?|us|milliseconds?|millis?|ms"
    r"|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)\s*$"
)

_DURATION_SECONDS = {
    "ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0,
}

_DURATION_ALIASES = {
    "nano": "ns", "nanos": "ns", "nanosecond": "ns", "nanoseconds": "ns",
    "micro": "us", "micros": "us", "microsecond": "us", "microseconds": "us",
    "milli": "ms", "millis": "ms", "millisecond": "ms", "milliseconds": "ms",
    "sec": "s", "secs": "s", "second": "s", "seconds": "s",
    "min": "m", "mins": "m", "minute": "m", "minutes": "m",
    "hr": "h", "hrs": "h", "hour": "h", "hours": "h",
    "day": "d", "days": "d",
}

# On-disk key -> Settings field, per section. Values carry the
# target field name and the value converter ("duration", bool, or int).
_KeyMap = Dict[str, tuple]

_CONNECTIONS_KEYS: _KeyMap = {
    "minimum_connections_per_node": ("min_connections_per_node", int),
    "maximum_connections_per_node": ("max_connections_per_node", int),
    "maximum_socket_idle_time": ("max_socket_idle_time", "duration"),
}
_CIRCUIT_BREAKER_KEYS: _KeyMap = {
    "num_tend_intervals_in_error_window": ("num_tend_intervals_in_error_window", int),
    "maximum_errors_in_error_window": ("max_errors_in_error_window", int),
}
_REFRESH_KEYS: _KeyMap = {
    "tend_interval": ("tend_interval", "duration"),
}
_TRANSACTIONS_KEYS: _KeyMap = {
    "implicit_batch_write_transactions": ("implicit_batch_write_transactions", bool),
    "sleep_between_attempts": ("sleep_between_attempts", "duration"),
    "number_of_attempts": ("number_of_attempts", int),
}

# Client metrics. The histogram shape keys sit at the `metrics` root rather
# than under the spec's `extended.operational` group: collection is a single
# on/off here, so nesting them under a group whose own `enabled` flag cannot be
# honored would advertise a tier split that does not exist. `extended.*` is
# therefore not a recognized key and reports itself as unrecognized.
_METRICS_HANDLED_ELSEWHERE = frozenset({"sampler", "labels", "extended"})
_METRICS_KEYS: _KeyMap = {
    "enabled": ("enabled", bool),
    "latency_unit": ("latency_unit", "latency_unit"),
    "latency_columns": ("latency_columns", int),
    "latency_shift": ("latency_shift", int),
    "export_interval": ("export_interval", "duration"),
    "exporter": ("exporter", str),
    "report_dir": ("report_dir", str),
    "report_size_limit": ("report_size_limit", "size"),
}
# `sampler` and `labels` are mappings rather than scalars, so they are parsed
# separately from the table above.
_SAMPLER_KEYS: _KeyMap = {
    "range": ("sampler_range", int),
    "threshold": ("sampler_threshold", int),
}

# `metrics.extended.usage.enabled` per the cross-SDK key names. Only the `usage`
# child is recognized: `extended.operational.enabled` would promise a Tier 0 /
# operational split the client core cannot make, so it falls through to the
# unrecognized-key warning rather than being silently accepted and ignored.
_USAGE_KEYS: _KeyMap = {
    "enabled": ("usage_enabled", bool),
}

_SECTIONS: Dict[str, _KeyMap] = {
    "connections": _CONNECTIONS_KEYS,
    "circuit_breaker": _CIRCUIT_BREAKER_KEYS,
    "refresh": _REFRESH_KEYS,
    "transactions": _TRANSACTIONS_KEYS,
    "metrics": _METRICS_KEYS,
}

# behaviors: selector blocks -> Settings scopes.
_BEHAVIOR_SELECTOR_SCOPES: Dict[str, Scope] = {
    "all_operations": Scope.ALL,
    "retryable_writes": Scope.WRITES_RETRYABLE,
    "non_retryable_writes": Scope.WRITES_NON_RETRYABLE,
    "consistency_mode_reads": Scope.READS_SC,
    "availability_mode_reads": Scope.READS_AP,
    "batch_reads": Scope.READS_BATCH,
    "batch_writes": Scope.WRITES_BATCH,
    "query": Scope.READS_QUERY,
}

# Behavior profile root keys that are not selector blocks. Blocks with no
# Settings scope yet (system_txn_verify / system_txn_roll) share the
# unknown-block path.
_BEHAVIOR_META_KEYS = frozenset({"parent", "name"})

# behaviors: policy fields -> Settings fields. Fields with no Settings
# equivalent (connection timeouts) fall through to the unrecognized-key warning.
_BEHAVIOR_FIELD_KEYS: _KeyMap = {
    "abandon_call_after": ("total_timeout", "duration"),
    "wait_for_call_to_complete": ("socket_timeout", "duration"),
    "delay_between_retries": ("retry_delay", "duration"),
    "maximum_number_of_call_attempts": ("max_retries", "attempts"),
    "replica_order": ("replica", Replica),
    "send_key": ("send_key", bool),
    "use_compression": ("use_compression", bool),
    "reset_ttl_on_read_at_percent": ("read_touch_ttl_percent", int),
    "read_consistency": ("read_mode_sc", ReadModeSC),
    "consistency": ("read_mode_sc", ReadModeSC),
    "migration_read_consistency": ("read_mode_ap", ReadModeAP),
    "use_durable_delete": ("durable_delete", bool),
    "max_concurrent_servers": ("max_concurrent_nodes", int),
    "allow_inline_memory_access": ("allow_inline", bool),
    "allow_inline_ssd_access": ("allow_inline_ssd", bool),
    "record_queue_size": ("record_queue_size", int),
    "allow_scans_with_where": ("allow_scans_with_where", bool),
    "error_detail_verbosity": ("error_detail_verbosity", int),
}


_SIZE_UNITS = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3}


def parse_size(value: object) -> int:
    """Parse a suffixed byte size (``512b``, ``10mb``, ``1gb``), or a bare count.

    Sizes carry their unit in the value rather than the key name, so the same
    key accepts any scale.

    Args:
        value: The raw YAML value: an integer byte count, or a string with a
            ``b`` / ``kb`` / ``mb`` / ``gb`` suffix.

    Returns:
        The size in bytes.

    Raises:
        ValueError: If the value is neither an integer nor a suffixed string.

    Example::

        parse_size("10mb")   # 10485760
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise ValueError(f"size must be an integer or a suffixed string, got {value!r}")
    text = value.strip().lower()
    for suffix in ("gb", "mb", "kb", "b"):
        if text.endswith(suffix):
            number = text[: -len(suffix)].strip()
            try:
                return int(float(number) * _SIZE_UNITS[suffix])
            except ValueError as exc:
                raise ValueError(f"size {value!r} has no numeric part") from exc
    raise ValueError(f"size {value!r} needs a b/kb/mb/gb suffix")


def parse_duration(value: object) -> timedelta:
    """Parse a suffixed duration string (``250ms``, ``1s``, ``5m``, ``2h``).

    Accepts short and long unit spellings from nanoseconds to days
    (``ns``/``nanos``, ``us``, ``ms``/``millis``, ``s``/``seconds``,
    ``m``/``minutes``, ``h``/``hours``, ``d``/``days``).

    Args:
        value: The raw YAML value; must be a string with a unit suffix.

    Returns:
        The equivalent :class:`datetime.timedelta`.

    Raises:
        ValueError: If the value is not a string or does not match the
            ``<number><unit>`` form.
    """
    if not isinstance(value, str):
        raise ValueError(f"duration must be a string like '250ms' or '1s', got {value!r}")
    match = _DURATION_RE.match(value)
    if match is None:
        raise ValueError(f"unrecognized duration {value!r}; use e.g. '250ms', '1s', '5m'")
    unit = match.group(2)
    unit = _DURATION_ALIASES.get(unit, unit)
    return timedelta(seconds=float(match.group(1)) * _DURATION_SECONDS[unit])


# The two latency units, spelled as the shared config format writes them —
# lower case, unlike the enum-valued keys elsewhere in the file, which name
# their member directly.
_LATENCY_UNITS = ("milliseconds", "microseconds")


def _convert(raw: object, converter: object, disk_key: str) -> object:
    """Convert one raw YAML value, raising ValueError on a type mismatch."""
    if converter == "duration":
        return parse_duration(raw)
    if converter == "size":
        return parse_size(raw)
    if converter == "latency_unit":
        if not isinstance(raw, str) or raw.lower() not in _LATENCY_UNITS:
            raise ValueError(
                f"{disk_key} must be one of {', '.join(_LATENCY_UNITS)}, got {raw!r}"
            )
        return raw.lower()
    if converter == "attempts":
        # The file counts the initial call as an attempt; the policy model
        # counts retries after it.
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 1:
            raise ValueError(f"{disk_key} must be an integer >= 1, got {raw!r}")
        return raw - 1
    if converter is bool:
        if not isinstance(raw, bool):
            raise ValueError(f"{disk_key} must be a boolean, got {raw!r}")
        return raw
    if converter is str:
        if not isinstance(raw, str):
            raise ValueError(f"{disk_key} must be a string, got {raw!r}")
        return raw
    if converter is int:
        # bool is an int subclass; a YAML `true` is not a count.
        if not isinstance(raw, int) or isinstance(raw, bool):
            raise ValueError(f"{disk_key} must be an integer, got {raw!r}")
        return raw
    if isinstance(converter, type):
        # Enum-valued field: the YAML value names a member (e.g. SEQUENCE).
        if isinstance(raw, str) and not raw.startswith("_"):
            member = getattr(converter, raw, None)
            if member is not None:
                return member
        raise ValueError(f"{disk_key} must name a {converter.__name__} value, got {raw!r}")
    raise AssertionError(f"unknown converter for {disk_key}")


def _parse_section(
    section_name: str, section: object, key_map: Mapping[str, tuple],
) -> Dict[str, object]:
    """Translate one on-disk section mapping into Settings kwargs.

    Unrecognized keys are ignored with a warning; a value that fails to convert
    is skipped with a warning while the rest of the section still applies.
    """
    if not isinstance(section, Mapping):
        log.warning("SDK config: section %r is not a mapping; ignored", section_name)
        return {}
    kwargs: Dict[str, object] = {}
    for disk_key, raw in section.items():
        if key_map is _METRICS_KEYS and disk_key in _METRICS_HANDLED_ELSEWHERE:
            continue  # mappings, parsed by _parse_metrics_extras
        entry = key_map.get(disk_key)
        if entry is None:
            _record_drop(f"key {section_name}.{disk_key}")
            continue
        field_name, converter = entry
        try:
            kwargs[field_name] = _convert(raw, converter, disk_key)
        except ValueError as exc:
            log.warning("SDK config: skipping %s.%s: %s", section_name, disk_key, exc)
    return kwargs


def _parse_profile(name: str, profile: object) -> SystemSettings:
    """Build a :class:`SystemSettings` from one profile mapping."""
    if not isinstance(profile, Mapping):
        log.warning("SDK config: profile %r is not a mapping; treated as empty", name)
        return SystemSettings()
    settings_kwargs: Dict[str, object] = {}
    txn_kwargs: Dict[str, object] = {}
    metrics_kwargs: Dict[str, object] = {}
    for section_name, section in profile.items():
        key_map = _SECTIONS.get(section_name)
        if key_map is None:
            _record_drop(f"section {section_name!r} in profile {name!r}")
            continue
        parsed = _parse_section(section_name, section, key_map)
        if section_name == "transactions":
            txn_kwargs.update(parsed)
        elif section_name == "metrics":
            metrics_kwargs.update(parsed)
            metrics_kwargs.update(_parse_metrics_extras(name, section))
        else:
            settings_kwargs.update(parsed)
    if txn_kwargs:
        settings_kwargs["transactions"] = TransactionSettings(**txn_kwargs)  # type: ignore[arg-type]
    if metrics_kwargs:
        settings_kwargs["metrics"] = MetricsSettings(**metrics_kwargs)  # type: ignore[arg-type]
    return SystemSettings(**settings_kwargs)  # type: ignore[arg-type]


def _parse_metrics_extras(profile_name: str, section: object) -> Dict[str, object]:
    """Parse the two ``metrics`` values that are mappings, not scalars.

    ``sampler`` is ``{range, threshold}``, ``labels`` is an arbitrary string
    map, and ``extended`` carries the ``usage`` group, so none of them fit the
    scalar key table.
    """
    if not isinstance(section, Mapping):
        return {}
    extras: Dict[str, object] = {}
    sampler = section.get("sampler")
    if sampler is not None:
        extras.update(_parse_section(f"{profile_name}.metrics.sampler", sampler, _SAMPLER_KEYS))
    extended = section.get("extended")
    if isinstance(extended, Mapping):
        usage = extended.get("usage")
        if usage is not None:
            extras.update(
                _parse_section(f"{profile_name}.metrics.extended.usage", usage, _USAGE_KEYS)
            )
        for name in extended:
            if name != "usage":
                _record_drop(f"key metrics.extended.{name} in profile {profile_name!r}")
    elif extended is not None:
        _record_drop(f"key metrics.extended in profile {profile_name!r} (not a mapping)")
    labels = section.get("labels")
    if labels is not None:
        if isinstance(labels, Mapping) and all(
            isinstance(k, str) and isinstance(v, (str, int, float, bool))
            for k, v in labels.items()
        ):
            extras["labels"] = {str(k): str(v) for k, v in labels.items()}
        else:
            log.warning(
                "SDK config: metrics.labels in profile %r is not a string map; ignored",
                profile_name,
            )
    return extras


def _load_yaml_doc(text: str) -> Optional[Mapping]:
    """Parse YAML text to the document mapping shared by both sections.

    Raises:
        ValueError: If the text is not valid YAML or the document root is
            not a mapping. (Field- and section-level problems never raise;
            they are skipped fail-soft.)
    """
    try:
        doc = yaml.load(text, Loader=_YAML_LOADER)  # noqa: S506 — safe loader variant
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid YAML: {exc}") from exc
    if doc is None:
        return None
    if not isinstance(doc, Mapping):
        raise ValueError(f"config root must be a mapping, got {type(doc).__name__}")
    return doc


def _profiles_from_doc(doc: Mapping) -> Dict[str, SystemSettings]:
    """Extract the ``system:`` per-profile settings map from a parsed doc."""
    system = doc.get("system")
    if system is None:
        log.debug("SDK config: no 'system' section; nothing to apply")
        return {}
    if not isinstance(system, Mapping):
        log.warning("SDK config: 'system' is not a mapping; ignored")
        return {}
    return {str(name): _parse_profile(str(name), profile) for name, profile in system.items()}


def parse_sdk_config(text: str) -> Dict[str, SystemSettings]:
    """Parse SDK config YAML text into a per-profile settings map.

    Args:
        text: The YAML document (the whole config file).

    Returns:
        Mapping of profile name (for example ``"DEFAULT"``) to the
        :class:`SystemSettings` parsed from ``system.<profile>``. Empty when
        the document has no usable ``system:`` section.

    Raises:
        ValueError: If the text is not valid YAML or the document root is
            not a mapping. (Field- and section-level problems never raise;
            they are skipped fail-soft.)
    """
    doc = _load_yaml_doc(text)
    return _profiles_from_doc(doc) if doc is not None else {}


@dataclass(frozen=True)
class BehaviorSpec:
    """Parsed form of one ``behaviors:`` profile, comparable for change gating."""

    name: str
    parent: Optional[str]
    patches: Dict[Scope, Settings] = field(default_factory=dict)


def _parse_behavior_profile(name: str, profile: object) -> Optional[BehaviorSpec]:
    """Parse one behavior profile mapping into a :class:`BehaviorSpec`."""
    if not isinstance(profile, Mapping):
        log.warning("SDK config: behavior %r is not a mapping; ignored", name)
        return None
    parent = profile.get("parent")
    if parent is not None and not isinstance(parent, str):
        log.warning("SDK config: behavior %r has a non-string parent; ignored", name)
        parent = None
    patches: Dict[Scope, Settings] = {}
    for block_name, block in profile.items():
        if block_name in _BEHAVIOR_META_KEYS:
            continue
        scope = _BEHAVIOR_SELECTOR_SCOPES.get(block_name)
        if scope is None:
            _record_drop(f"block {block_name!r} in behavior {name!r}")
            continue
        kwargs = _parse_section(f"{name}.{block_name}", block, _BEHAVIOR_FIELD_KEYS)
        if kwargs:
            patches[scope] = Settings(**kwargs)  # type: ignore[arg-type]
    return BehaviorSpec(name=name, parent=parent, patches=patches)


def _behaviors_from_doc(doc: Mapping) -> Dict[str, BehaviorSpec]:
    """Extract the ``behaviors:`` spec map from a parsed doc."""
    behaviors = doc.get("behaviors")
    if behaviors is None:
        return {}
    if not isinstance(behaviors, Mapping):
        log.warning("SDK config: 'behaviors' is not a mapping; ignored")
        return {}
    specs: Dict[str, BehaviorSpec] = {}
    for name, profile in behaviors.items():
        spec = _parse_behavior_profile(str(name), profile)
        if spec is not None:
            specs[str(name)] = spec
    return specs


def parse_behaviors(text: str) -> Dict[str, BehaviorSpec]:
    """Parse the ``behaviors:`` section of SDK config YAML text.

    Returns:
        Mapping of behavior name to its parsed :class:`BehaviorSpec`, in
        file order. Empty when the document has no usable ``behaviors:``
        section.

    Raises:
        ValueError: If the text is not valid YAML or the document root is
            not a mapping (matching :func:`parse_sdk_config`).
    """
    doc = _load_yaml_doc(text)
    return _behaviors_from_doc(doc) if doc is not None else {}


def _topo_order(specs: Mapping[str, BehaviorSpec]) -> list[BehaviorSpec]:
    """Order specs so parents are applied before children.

    Parents outside the file (already-registered behaviors) impose no
    ordering. A parent cycle is broken with a warning; the remaining specs
    are applied in file order and resolve their parent via the registry.
    """
    ordered: list[BehaviorSpec] = []
    emitted: set[str] = set()
    pending = dict(specs)
    while pending:
        progressed = False
        for name in list(pending):
            spec = pending[name]
            if spec.parent is None or spec.parent not in pending or spec.parent == name:
                ordered.append(pending.pop(name))
                emitted.add(name)
                progressed = True
        if not progressed:
            log.warning(
                "SDK config: behavior parent cycle among %r; applying in file order",
                sorted(pending),
            )
            ordered.extend(pending.values())
            break
    return ordered


# Factory patches of Behavior.DEFAULT, captured before the first file apply
# so a reload can re-layer file patches on the pristine defaults.
_default_factory_patches: Optional[Dict[Scope, Settings]] = None

# Last-applied behavior specs (module-global, like the registry they gate).
# A reload skips any behavior whose spec is unchanged.
_last_applied_behaviors: Dict[str, BehaviorSpec] = {}


def _layer_on_factory_default(patches: Dict[Scope, Settings]) -> Dict[Scope, Settings]:
    """Merge file patches for DEFAULT onto its captured factory patches."""
    assert _default_factory_patches is not None
    merged = dict(_default_factory_patches)
    for scope, patch in patches.items():
        base = merged.get(scope)
        merged[scope] = Settings.merge(base, patch) if base is not None else patch
    return merged


def apply_behaviors(specs: Mapping[str, BehaviorSpec]) -> None:
    """Create or reload registered behaviors from parsed file specs.

    Existing behaviors are reloaded in place (registry entry and live
    references keep working; bound sessions get rebuilt policies pushed).
    A behavior whose parent changed is replaced with a new registration.
    Behaviors whose spec is unchanged since the last apply are skipped.
    """
    global _default_factory_patches
    if _default_factory_patches is None:
        _default_factory_patches = dict(Behavior.DEFAULT._patches)

    for spec in _topo_order(specs):
        if _last_applied_behaviors.get(spec.name) == spec:
            continue
        if spec.name == Behavior.DEFAULT.name:
            Behavior.DEFAULT._reload_patches(_layer_on_factory_default(spec.patches))
            _last_applied_behaviors[spec.name] = spec
            log.info("SDK config: behavior DEFAULT reloaded")
            continue
        parent = Behavior.DEFAULT
        if spec.parent is not None and spec.parent != Behavior.DEFAULT.name:
            named = get_behavior(spec.parent)
            if named is None:
                log.warning(
                    "SDK config: behavior %r parent %r not found; using DEFAULT",
                    spec.name, spec.parent,
                )
            else:
                parent = named
        existing = get_behavior(spec.name)
        if existing is not None and existing.parent is parent:
            existing._reload_patches(spec.patches)
            log.info("SDK config: behavior %r reloaded", spec.name)
        else:
            # New name, or the parent changed: (re)register a fresh Behavior.
            Behavior(spec.name, spec.patches, parent=parent)
            log.info("SDK config: behavior %r registered", spec.name)
        _last_applied_behaviors[spec.name] = spec


def merge_settings(
    higher: Optional[SystemSettings], lower: Optional[SystemSettings],
) -> Optional[SystemSettings]:
    """Merge two settings layers per-field; *higher* wins where it has a value.

    ``None`` fields in *higher* fall through to *lower*. The nested
    :class:`TransactionSettings` and :class:`MetricsSettings` groups merge
    the same way, field by field.
    Either argument may be ``None`` (that layer absent).
    """
    if higher is None:
        return lower
    if lower is None:
        return higher
    merged: Dict[str, Any] = {}
    for f in fields(SystemSettings):
        if f.name in ("transactions", "metrics"):
            continue
        hi = getattr(higher, f.name)
        merged[f.name] = hi if hi is not None else getattr(lower, f.name)
    met: Dict[str, Any] = {}
    for f in fields(MetricsSettings):
        hi = getattr(higher.metrics, f.name)
        met[f.name] = hi if hi is not None else getattr(lower.metrics, f.name)
    merged["metrics"] = MetricsSettings(**met)
    txn: Dict[str, Any] = {}
    for f in fields(TransactionSettings):
        hi = getattr(higher.transactions, f.name)
        txn[f.name] = hi if hi is not None else getattr(lower.transactions, f.name)
    return SystemSettings(transactions=TransactionSettings(**txn), **merged)


def resolve_for_cluster(
    profiles: Mapping[str, SystemSettings], cluster_name: Optional[str],
) -> Optional[SystemSettings]:
    """Resolve the file layer for a cluster: ``<cluster-name>`` over ``DEFAULT``.

    Args:
        profiles: Per-profile map from :func:`parse_sdk_config`.
        cluster_name: The declared cluster name, or ``None`` when not set.

    Returns:
        The layered file settings, or ``None`` when no profile applies.
    """
    default = profiles.get(DEFAULT_PROFILE)
    named = profiles.get(cluster_name) if cluster_name else None
    if named is not None and cluster_name:
        log.debug("SDK config: using profile %r layered on %r", cluster_name, DEFAULT_PROFILE)
    return merge_settings(named, default)


def fill_hard_defaults(settings: Optional[SystemSettings]) -> SystemSettings:
    """Apply the bottom precedence layer (hard defaults) to resolved settings.

    Policy-mapped fields keep ``None`` (their defaults live in
    :class:`~aerospike_async.ClientPolicy`); only SDK-runtime fields with an
    SDK-defined default are filled here.
    """
    if settings is None:
        settings = SystemSettings()
    txn = settings.transactions
    fills = {}
    if txn.implicit_batch_write_transactions is None:
        fills["implicit_batch_write_transactions"] = _HARD_DEFAULT_IMPLICIT_BATCH_WRITE_TXNS
    if txn.sleep_between_attempts is None:
        fills["sleep_between_attempts"] = _HARD_DEFAULT_TXN_SLEEP_BETWEEN_ATTEMPTS
    if txn.number_of_attempts is None:
        fills["number_of_attempts"] = _HARD_DEFAULT_TXN_NUMBER_OF_ATTEMPTS
    if fills:
        settings = replace(settings, transactions=replace(txn, **fills))
    return settings


def config_path_from_env() -> Optional[str]:
    """Resolve ``AEROSPIKE_SDK_CONFIG_URL`` to a filesystem path, or ``None``.

    Accepts a ``file://`` URL or a bare path. Any other scheme is ignored
    with a warning (fail-soft) — only file sources are supported.
    """
    raw = os.environ.get(ENV_VAR, "").strip()
    if not raw:
        return None
    if raw.startswith("file://"):
        return raw[len("file://"):]
    scheme = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*)://", raw)
    if scheme is not None:
        log.warning(
            "SDK config: unsupported scheme %r in %s; only file:// or a bare path is "
            "supported — ignoring", scheme.group(1), ENV_VAR,
        )
        return None
    return raw


@dataclass(frozen=True)
class LoadedConfig:
    """One successful read + parse of the config file.

    Carries the raw bytes (for content-based change gating on reload) and
    both parsed sections from a single YAML parse.
    """

    raw: bytes
    profiles: Dict[str, SystemSettings]
    behaviors: Dict[str, BehaviorSpec]


def read_config_bytes(path: str) -> Optional[bytes]:
    """Read the config file's raw bytes, fail-soft (warn and return None)."""
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except OSError as exc:
        log.warning("SDK config: cannot read %s: %s — ignoring", path, exc)
        return None


def parse_config_bytes(raw: bytes, path: str) -> Optional[LoadedConfig]:
    """Parse raw config bytes into both sections, fail-soft.

    *path* is used only for log context.
    """
    try:
        text = raw.decode("utf-8")
        doc = _load_yaml_doc(text)
    except (ValueError, UnicodeDecodeError) as exc:
        log.warning("SDK config: %s: %s — ignoring", path, exc)
        return None
    if doc is None:
        return LoadedConfig(raw=raw, profiles={}, behaviors={})
    return LoadedConfig(
        raw=raw,
        profiles=_profiles_from_doc(doc),
        behaviors=_behaviors_from_doc(doc),
    )


def load_config(path: str) -> Optional[LoadedConfig]:
    """Read and parse the config file at *path* (both sections), fail-soft.

    Returns:
        The parsed :class:`LoadedConfig`, or ``None`` when the file is
        missing, unreadable, or structurally malformed (a warning is
        logged; the caller continues without a file layer).
    """
    raw = read_config_bytes(path)
    if raw is None:
        return None
    return parse_config_bytes(raw, path)


def load_profiles(path: str) -> Optional[Dict[str, SystemSettings]]:
    """Read and parse the config file at *path*, fail-soft.

    Returns:
        The per-profile map, or ``None`` when the file is missing,
        unreadable, or structurally malformed (a warning is logged; the
        caller continues without a file layer).
    """
    loaded = load_config(path)
    return loaded.profiles if loaded is not None else None


def load_and_resolve(
    path: str,
    cluster_name: Optional[str],
    programmatic: Optional[SystemSettings],
) -> SystemSettings:
    """Run the full precedence pipeline for one config file read.

    Layers file (``<cluster-name>`` over ``DEFAULT``) over *programmatic*,
    then fills hard defaults. Fail-soft: any file problem leaves only the
    programmatic + default layers.
    """
    profiles = load_profiles(path)
    file_layer = resolve_for_cluster(profiles, cluster_name) if profiles else None
    return fill_hard_defaults(merge_settings(file_layer, programmatic))


def resolve_from_env(
    cluster_name: Optional[str],
    programmatic: Optional[SystemSettings],
) -> tuple[SystemSettings, Optional[str]]:
    """Resolve effective settings from the environment for a connect.

    Returns:
        ``(settings, path)`` — the fully resolved :class:`SystemSettings`
        and the config file path when ``AEROSPIKE_SDK_CONFIG_URL`` supplied
        one (``None`` otherwise; the caller then skips hot-reload).
    """
    path = config_path_from_env()
    if path is None:
        return fill_hard_defaults(programmatic), None
    return load_and_resolve(path, cluster_name, programmatic), path


def load_at_connect(
    cluster_name: Optional[str],
    programmatic: Optional[SystemSettings],
    *,
    strict: bool = False,
) -> tuple[SystemSettings, Optional[str], Optional[bytes]]:
    """Full connect-time pipeline: resolve system settings, apply behaviors.

    Applies the file's ``behaviors:`` section to the behavior registry
    (create / in-place reload) and resolves the ``system:`` precedence
    stack. Fail-soft throughout, except under ``strict``.

    Args:
        cluster_name: Cluster whose ``system:`` profile takes precedence.
        programmatic: Settings supplied in code, layered under the file.
        strict: Raise if the file carried anything the loader did not
            recognize, instead of warning and continuing. Connect-time only —
            hot reload stays fail-soft, since a raise on the monitor thread
            has no caller to reach.

    Returns:
        ``(settings, path, raw)`` — the resolved settings, the config path
        (``None`` when the env var is unset; the caller then skips
        hot-reload), and the raw file bytes for content-based reload
        gating (``None`` when the file could not be read).

    Raises:
        ValueError: Under ``strict``, when any key or block was unrecognized.
    """
    path = config_path_from_env()
    if path is None:
        return fill_hard_defaults(programmatic), None, None
    with _collect_drops() as dropped:
        loaded = load_config(path)
        if loaded is None:
            return fill_hard_defaults(programmatic), path, None
        # One breadcrumb per connect confirming the file was read and applied
        # (path + profile/behavior names — never values; user-data rule).
        log.info(
            "SDK config loaded from %s (profiles: %s, behaviors: %s)",
            path, sorted(loaded.profiles), sorted(loaded.behaviors),
        )
        if loaded.behaviors:
            apply_behaviors(loaded.behaviors)
        file_layer = (
            resolve_for_cluster(loaded.profiles, cluster_name) if loaded.profiles else None
        )
        settings = fill_hard_defaults(merge_settings(file_layer, programmatic))
        if strict and dropped:
            raise ValueError(
                f"SDK config {path} has {len(dropped)} unrecognized "
                f"entr{'y' if len(dropped) == 1 else 'ies'}: {', '.join(sorted(dropped))}"
            )
    return settings, path, loaded.raw
