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

"""Seed discovery and address translation settings."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, Mapping, Optional, Union

from aerospike_native import (
    AwsEc2TagSeedCandidateProvider,
    ClientPolicy,
    DnsSeedCandidateProvider,
    PassThroughAddressTranslator,
    PortMapAddressTranslator,
    ResultCode,
    SeedCandidateProvider,
    SeedMergePolicy,
    StaticMapAddressTranslator,
    StaticSeedCandidateProvider,
    TranslationMissMode,
)

from aerospike_sdk.exceptions import AerospikeError

SeedProvider = Union[
    StaticSeedCandidateProvider,
    DnsSeedCandidateProvider,
    AwsEc2TagSeedCandidateProvider,
    SeedCandidateProvider,
]
"""A seed candidate provider: a built-in one or a :class:`SeedCandidateProvider` subclass."""

AddressTranslator = Union[
    StaticMapAddressTranslator,
    PortMapAddressTranslator,
    PassThroughAddressTranslator,
]
"""One of the built-in address translators."""


class DiscoveryExecutionMode(str, Enum):
    """Where a seed candidate provider's periodic refresh runs.

    Each provider runs in one mode in this client, so setting the mode only
    checks it: :class:`~aerospike_native.DnsSeedCandidateProvider` and
    :class:`~aerospike_native.AwsEc2TagSeedCandidateProvider` refresh on the
    cluster tend task, and a :class:`~aerospike_native.SeedCandidateProvider`
    subclass on a background thread the client starts for it. A
    :class:`~aerospike_native.StaticSeedCandidateProvider` never refreshes,
    so either mode suits it. The values are the configuration-file
    spellings; construction is case-insensitive.

    Attributes:
        THREAD: A background thread per client. A refresh may block.
        TEND: The cluster tend task, within
            ``tend_refresh_deadline``. A refresh must bound its own I/O.

    Example::

        cd = ClusterDefinition("aerospike.example.com", 3000).with_seed_discovery(
            RegistryProvider(registry), execution_mode=DiscoveryExecutionMode.THREAD
        )
    """

    THREAD = "thread"
    TEND = "tend"

    @classmethod
    def _missing_(cls, value: object) -> Optional["DiscoveryExecutionMode"]:
        if isinstance(value, str):
            lowered = value.strip().lower()
            for member in cls:
                if member.value == lowered:
                    return member
        return None

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class FileSeedProvider:
    """A seed candidate provider a configuration file names, held as data.

    Kept as its settings rather than as a provider, so that a reloaded file
    compares by value, and built at connect.

    Attributes:
        name: ``"static"`` (no provider: the seeds passed in code alone),
            ``"dns"`` or ``"aws-tags"``.
        options: Keyword arguments of the provider's constructor.
    """

    name: str
    options: Mapping[str, Any] = field(default_factory=dict)

    def build(self) -> Optional[SeedProvider]:
        """The provider, or ``None`` for ``static``.

        Raises:
            ValueError: If an option is invalid.
        """
        if self.name == "dns":
            return DnsSeedCandidateProvider(**self.options)
        if self.name == "aws-tags":
            return AwsEc2TagSeedCandidateProvider(**self.options)
        return None


@dataclass(frozen=True)
class FileAddressTranslator:
    """An address translator a configuration file names, held as data.

    Attributes:
        name: ``"static-map"``, ``"port-map"`` or ``"pass-through"``.
        mapping: The ``static_map`` or ``port_map`` entries; ``None`` for
            ``static-map`` without a map, which leaves translation to
            :meth:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.ip_map`.
    """

    name: str
    mapping: Optional[Mapping[str, str]] = None

    def build(self) -> Optional[AddressTranslator]:
        """The translator, or ``None`` for ``static-map`` without a map.

        Raises:
            ValueError: If a ``port_map`` entry is invalid.
        """
        if self.name == "port-map":
            return PortMapAddressTranslator(dict(self.mapping or {}))
        if self.name == "pass-through":
            return PassThroughAddressTranslator()
        if self.mapping is not None:
            return StaticMapAddressTranslator(dict(self.mapping))
        return None


def runs_in(
    provider: Optional[Union[SeedProvider, FileSeedProvider]],
) -> Optional[DiscoveryExecutionMode]:
    """The one mode *provider* runs in, or ``None`` when it never refreshes."""
    if isinstance(provider, SeedCandidateProvider):
        return DiscoveryExecutionMode.THREAD
    if isinstance(provider, (DnsSeedCandidateProvider, AwsEc2TagSeedCandidateProvider)):
        return DiscoveryExecutionMode.TEND
    if isinstance(provider, FileSeedProvider) and provider.name != "static":
        return DiscoveryExecutionMode.TEND
    return None


def check_execution_mode(
    provider: Optional[Union[SeedProvider, FileSeedProvider]],
    mode: Optional[DiscoveryExecutionMode],
) -> None:
    """Refuse an execution mode *provider* does not run in.

    Raises:
        AerospikeError: With ``PARAMETER_ERROR``.
    """
    mode_of_provider = runs_in(provider)
    if mode is None or mode_of_provider is None or mode is mode_of_provider:
        return
    label = (
        provider.name if isinstance(provider, FileSeedProvider) else type(provider).__name__
    )
    where = (
        "on a background thread" if mode_of_provider is DiscoveryExecutionMode.THREAD
        else "on the cluster tend task"
    )
    raise AerospikeError(
        f"Seed candidate provider {label} runs {where}, so execution_mode {mode} "
        f"does not apply to it; omit execution_mode or set {mode_of_provider}",
        result_code=ResultCode.PARAMETER_ERROR,
    )


def _millis(name: str, value: Optional[timedelta]) -> None:
    if value is not None and value <= timedelta(0):
        raise ValueError(f"{name} must be positive, got {value}")


@dataclass(frozen=True, kw_only=True)
class DiscoverySettings:
    """Seed discovery from the ``system.<cluster>.discovery`` block or code.

    Every field is ``None`` when not set, so a partial block or call only
    overrides what it names, and the rest falls through to the next layer.
    Set in code with
    :meth:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.with_seed_discovery`.

    Attributes:
        provider: The seed candidate provider, or the
            :class:`FileSeedProvider` a file names.
        merge_policy: How the provider's seeds combine with the ones passed
            to the cluster definition. Default ``MERGE``.
        execution_mode: Checked against the mode the provider runs in; see
            :class:`DiscoveryExecutionMode`.
        refresh_interval: Time between periodic refreshes. Default 30
            seconds; at least the tend interval.
        refresh_timeout: Time one refresh of a
            :class:`~aerospike_native.SeedCandidateProvider` subclass may
            take before its result is discarded, and the most that closing
            the client waits for one in progress. Default 10 seconds. The
            built-in providers are bounded by their own ``timeout``.
        tend_refresh_deadline: Time one refresh on the cluster tend task may
            take. Default 100 ms; at most half the tend interval.

    Example::

        settings = SystemSettings(
            discovery=DiscoverySettings(
                provider=DnsSeedCandidateProvider(["seeds.prod.example.com"]),
                merge_policy=SeedMergePolicy.DISCOVERY_ONLY,
            ),
        )

    Raises:
        ValueError: If a duration is not positive.
    """

    provider: Optional[Union[SeedProvider, FileSeedProvider]] = None
    merge_policy: Optional[SeedMergePolicy] = None
    execution_mode: Optional[DiscoveryExecutionMode] = None
    refresh_interval: Optional[timedelta] = None
    refresh_timeout: Optional[timedelta] = None
    tend_refresh_deadline: Optional[timedelta] = None

    def __post_init__(self) -> None:
        _millis("refresh_interval", self.refresh_interval)
        _millis("refresh_timeout", self.refresh_timeout)
        _millis("tend_refresh_deadline", self.tend_refresh_deadline)

    def apply_to(self, policy: ClientPolicy) -> None:
        """Apply the fields that are set to *policy*.

        Raises:
            AerospikeError: With ``PARAMETER_ERROR``, if
                :attr:`execution_mode` is not the mode the provider runs in.
            ValueError: If a provider named by a file has an invalid option.
        """
        if self.provider is not None:
            provider = (
                self.provider.build() if isinstance(self.provider, FileSeedProvider)
                else self.provider
            )
            check_execution_mode(provider, self.execution_mode)
            policy.seed_candidate_provider = provider
        if self.merge_policy is not None:
            policy.discovery_merge_policy = self.merge_policy
        if self.refresh_interval is not None:
            policy.discovery_refresh_interval = _to_millis(self.refresh_interval)
        if self.refresh_timeout is not None:
            policy.discovery_refresh_timeout = _to_millis(self.refresh_timeout)
        if self.tend_refresh_deadline is not None:
            policy.discovery_tend_refresh_deadline = _to_millis(self.tend_refresh_deadline)


@dataclass(frozen=True, kw_only=True)
class TranslationSettings:
    """Address translation from the ``system.<cluster>.translation`` block or code.

    Every field is ``None`` when not set. Set in code with
    :meth:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.with_address_translation`.

    Attributes:
        translator: The address translator, or the
            :class:`FileAddressTranslator` a file names. When set, it
            replaces
            :meth:`~aerospike_sdk.cluster_shared.ClusterDefinitionBase.ip_map`.
        on_miss: What happens to a peer address the translator has no
            mapping for. Default ``PASS_THROUGH``.

    Example::

        settings = SystemSettings(
            translation=TranslationSettings(
                translator=PortMapAddressTranslator({"10.0.0.1:3000": "lb.example.com:30001"}),
                on_miss=TranslationMissMode.STRICT,
            ),
        )
    """

    translator: Optional[Union[AddressTranslator, FileAddressTranslator]] = None
    on_miss: Optional[TranslationMissMode] = None

    def apply_to(self, policy: ClientPolicy) -> None:
        """Apply the fields that are set to *policy*.

        Raises:
            ValueError: If a translator named by a file has an invalid entry.
        """
        if self.translator is not None:
            policy.address_translator = (
                self.translator.build() if isinstance(self.translator, FileAddressTranslator)
                else self.translator
            )
        if self.on_miss is not None:
            policy.translation_on_miss = self.on_miss


def _to_millis(value: timedelta) -> int:
    return int(value.total_seconds() * 1000)
