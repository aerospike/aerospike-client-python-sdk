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

"""Behavior - Scope-aware configuration for operation policies."""

from __future__ import annotations

from datetime import timedelta
from typing import ClassVar, Dict, List, Optional, Tuple

from aerospike_async import CommitLevel, ReadModeSC, Replica

from aerospike_sdk.policy.behavior_registry import _register
from aerospike_sdk.policy.behavior_settings import (
    Mode,
    OpKind,
    OpShape,
    Scope,
    Settings,
    resolution_order,
)

_OpKey = Tuple[OpKind, OpShape, Mode]

_ALL_KEYS: List[_OpKey] = [
    (kind, shape, mode)
    for kind in OpKind
    for shape in OpShape
    for mode in Mode
]


class Behavior:
    """Immutable, scope-aware configuration for Aerospike operation policies.

    A Behavior holds a set of scope-keyed ``Settings`` patches that are
    resolved at operation time based on the operation's kind, shape, and
    namespace mode.  More-specific scopes override less-specific ones,
    and a child Behavior inherits from its parent.

    Resolved settings are eagerly cached at construction time so that
    ``get_settings()`` is a simple dict lookup.

    Example::

        # Use a pre-defined behavior
        session = cluster.create_session(Behavior.DEFAULT)

        # Derive a custom behavior with scope-specific overrides
        fast = Behavior.DEFAULT.derive_with_changes(
            "fast_reads",
            reads=Settings(total_timeout=timedelta(milliseconds=200)),
            reads_batch=Settings(max_concurrent_nodes=8),
        )
        session = cluster.create_session(fast)

    Attributes:
        DEFAULT: Balanced defaults (30 s total timeout, 2 retries, send key).
        READ_FAST: Low-latency reads (200 ms total, 50 ms socket, 3 retries).
        STRICTLY_CONSISTENT: SC-namespace reads with linearizable consistency.
        FAST_RACK_AWARE: Low-latency rack-preferred reads.
    """

    DEFAULT: ClassVar[Behavior]
    READ_FAST: ClassVar[Behavior]
    STRICTLY_CONSISTENT: ClassVar[Behavior]
    FAST_RACK_AWARE: ClassVar[Behavior]

    __slots__ = ("_name", "_patches", "_parent", "_children", "_resolved")

    def __init__(
        self,
        name: str,
        patches: Dict[Scope, Settings],
        parent: Optional[Behavior] = None,
    ) -> None:
        self._name = name
        self._patches: Dict[Scope, Settings] = dict(patches)
        self._parent = parent
        self._children: List[Behavior] = []
        self._resolved: Dict[_OpKey, Settings] = {}
        if parent is not None:
            parent._children.append(self)
        self._build_cache()
        _register(self)

    # -- Core API -------------------------------------------------------------

    @property
    def name(self) -> str:
        """Identifier for this behavior (for example ``'DEFAULT'``)."""
        return self._name

    @property
    def parent(self) -> Optional[Behavior]:
        """Behavior from which this one inherits, or ``None`` for root."""
        return self._parent

    @property
    def children(self) -> List[Behavior]:
        """Behaviors derived from this one via :meth:`derive_with_changes`."""
        return list(self._children)

    def get_settings(
        self,
        kind: OpKind,
        shape: OpShape,
        mode: Mode = Mode.AP,
    ) -> Settings:
        """Return the fully-resolved Settings for the given operation context.

        This is a cached O(1) dict lookup; the matrix is pre-computed at
        construction time.
        """
        return self._resolved[(kind, shape, mode)]

    def clear_cache(self) -> None:
        """Recompute the resolved settings matrix and cascade to children."""
        self._build_cache()
        for child in self._children:
            child.clear_cache()

    def derive_with_changes(
        self,
        name: str,
        *,
        # Scope-based (new API)
        all: Optional[Settings] = None,
        reads: Optional[Settings] = None,
        reads_point: Optional[Settings] = None,
        reads_batch: Optional[Settings] = None,
        reads_query: Optional[Settings] = None,
        reads_ap: Optional[Settings] = None,
        reads_sc: Optional[Settings] = None,
        writes: Optional[Settings] = None,
        writes_retryable: Optional[Settings] = None,
        writes_non_retryable: Optional[Settings] = None,
        writes_point: Optional[Settings] = None,
        writes_batch: Optional[Settings] = None,
        writes_query: Optional[Settings] = None,
        writes_ap: Optional[Settings] = None,
        writes_sc: Optional[Settings] = None,
        # Flat shortcuts (backward-compat, applied to the ALL scope)
        total_timeout: Optional[timedelta] = None,
        socket_timeout: Optional[timedelta] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[timedelta] = None,
        send_key: Optional[bool] = None,
        use_compression: Optional[bool] = None,
        compression_threshold: Optional[int] = None,
    ) -> Behavior:
        """Create a child Behavior with the specified overrides.

        Accepts either scope-keyed ``Settings`` objects or flat keyword
        arguments (which are applied to the ``ALL`` scope).  Both styles
        can be combined; flat kwargs are merged into the ``all`` Settings.

        Example::

            fast_reads = default_behavior.derive_with_changes(
                "fast_reads",
                reads=Settings(total_timeout=timedelta(milliseconds=200)),
            )
        """
        patches: Dict[Scope, Settings] = {}

        # Flat kwargs -> merge into the ALL scope
        flat = _flat_to_settings(
            total_timeout=total_timeout,
            socket_timeout=socket_timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            send_key=send_key,
            use_compression=use_compression,
            compression_threshold=compression_threshold,
        )
        if flat is not None:
            if all is not None:
                all = Settings.merge(all, flat)
            else:
                all = flat

        _set_if(patches, Scope.ALL, all)
        _set_if(patches, Scope.READS, reads)
        _set_if(patches, Scope.READS_POINT, reads_point)
        _set_if(patches, Scope.READS_BATCH, reads_batch)
        _set_if(patches, Scope.READS_QUERY, reads_query)
        _set_if(patches, Scope.READS_AP, reads_ap)
        _set_if(patches, Scope.READS_SC, reads_sc)
        _set_if(patches, Scope.WRITES, writes)
        _set_if(patches, Scope.WRITES_RETRYABLE, writes_retryable)
        _set_if(patches, Scope.WRITES_NON_RETRYABLE, writes_non_retryable)
        _set_if(patches, Scope.WRITES_POINT, writes_point)
        _set_if(patches, Scope.WRITES_BATCH, writes_batch)
        _set_if(patches, Scope.WRITES_QUERY, writes_query)
        _set_if(patches, Scope.WRITES_AP, writes_ap)
        _set_if(patches, Scope.WRITES_SC, writes_sc)

        return Behavior(name=name, patches=patches, parent=self)

    def find_behavior(self, name: str) -> Optional[Behavior]:
        """Search this behavior and its descendants for one with *name*.

        Returns:
            The matching :class:`Behavior`, or ``None`` if not found.
        """
        if self._name == name:
            return self
        for child in self._children:
            found = child.find_behavior(name)
            if found is not None:
                return found
        return None

    def explain(self) -> str:
        """Return a human-readable summary of overrides and resolved settings.

        Example::

            print(behavior.explain())

        Returns:
            Multi-line string with overrides and the full resolved settings matrix.
        """
        lines: list[str] = [f"Behavior: {self._name}"]

        lines.append("--- Patches ---")
        if not self._patches:
            lines.append("(no overrides)")
        for i, (scope, settings) in enumerate(self._patches.items(), 1):
            lines.append(f"{i:02d} {scope.value} -> {settings}")

        lines.append("--- Resolved Matrix ---")
        for key in _ALL_KEYS:
            kind, shape, mode = key
            s = self._resolved.get(key)
            if s is not None:
                lines.append(f"{kind.value}:{shape.value}:{mode.value} => {s}")

        return "\n".join(lines)

    # -- Internal -------------------------------------------------------------

    def _build_cache(self) -> None:
        """Pre-compute the resolved Settings for every OpKey."""
        self._resolved = {key: self._resolve(*key) for key in _ALL_KEYS}

    def _resolve(self, kind: OpKind, shape: OpShape, mode: Mode) -> Settings:
        """Walk the parent chain and layer patches (uncached)."""
        if self._parent is not None:
            base = self._parent._resolve(kind, shape, mode)
        else:
            base = Settings()
        for scope in resolution_order(kind, shape, mode):
            patch = self._patches.get(scope)
            if patch is not None:
                base = Settings.merge(base, patch)
        return base

    # -- Backward-compat read-only properties ---------------------------------
    # These resolve from the (READ, POINT, AP) scope for the common case.

    @property
    def total_timeout(self) -> timedelta:
        """Total timeout for point reads (from ``READ:POINT:AP`` scope)."""
        s = self.get_settings(OpKind.READ, OpShape.POINT)
        return s.total_timeout if s.total_timeout is not None else timedelta(0)

    @property
    def socket_timeout(self) -> timedelta:
        """Socket timeout for point reads (from ``READ:POINT:AP`` scope)."""
        s = self.get_settings(OpKind.READ, OpShape.POINT)
        return s.socket_timeout if s.socket_timeout is not None else timedelta(0)

    @property
    def max_retries(self) -> int:
        """Max retries for point reads (from ``READ:POINT:AP`` scope)."""
        s = self.get_settings(OpKind.READ, OpShape.POINT)
        return s.max_retries if s.max_retries is not None else 0

    @property
    def retry_delay(self) -> timedelta:
        """Retry delay for point reads (from ``READ:POINT:AP`` scope)."""
        s = self.get_settings(OpKind.READ, OpShape.POINT)
        return s.retry_delay if s.retry_delay is not None else timedelta(0)

    @property
    def send_key(self) -> bool:
        """Whether to send the user key with point reads (from ``READ:POINT:AP`` scope)."""
        s = self.get_settings(OpKind.READ, OpShape.POINT)
        return s.send_key if s.send_key is not None else False

    def __repr__(self) -> str:
        return (
            f"Behavior(name={self._name!r}, "
            f"total_timeout={self.total_timeout}, "
            f"socket_timeout={self.socket_timeout}, "
            f"max_retries={self.max_retries})"
        )


# -- Helpers ------------------------------------------------------------------

def _set_if(patches: Dict[Scope, Settings], scope: Scope, settings: Optional[Settings]) -> None:
    if settings is not None:
        patches[scope] = settings


def _flat_to_settings(
    total_timeout: Optional[timedelta] = None,
    socket_timeout: Optional[timedelta] = None,
    max_retries: Optional[int] = None,
    retry_delay: Optional[timedelta] = None,
    send_key: Optional[bool] = None,
    use_compression: Optional[bool] = None,
    compression_threshold: Optional[int] = None,
) -> Optional[Settings]:
    """Convert flat keyword arguments to a Settings, or None if all are None."""
    values = (
        total_timeout,
        socket_timeout,
        max_retries,
        retry_delay,
        send_key,
        use_compression,
        compression_threshold,
    )
    if all(v is None for v in values):
        return None
    return Settings(
        total_timeout=total_timeout,
        socket_timeout=socket_timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        send_key=send_key,
        use_compression=use_compression,
        compression_threshold=compression_threshold,
    )


# -- Pre-defined Behaviors ---------------------------------------------------

Behavior.DEFAULT = Behavior(
    name="DEFAULT",
    patches={
        Scope.ALL: Settings(
            total_timeout=timedelta(seconds=30),
            socket_timeout=timedelta(seconds=5),
            max_retries=2,
            retry_delay=timedelta(0),
            send_key=True,
            replica=Replica.SEQUENCE,
            durable_delete=False,
            max_concurrent_nodes=1,
            read_touch_ttl_percent=0,
        ),
        Scope.READS_BATCH: Settings(
            allow_inline=True,
            allow_inline_ssd=False,
        ),
        Scope.READS_QUERY: Settings(
            max_retries=5,
            record_queue_size=5000,
        ),
        Scope.WRITES_NON_RETRYABLE: Settings(
            max_retries=0,
        ),
        Scope.WRITES_AP: Settings(
            commit_level=CommitLevel.COMMIT_ALL,
        ),
        Scope.WRITES_SC: Settings(
            durable_delete=True,
        ),
        Scope.WRITES_BATCH: Settings(
            allow_inline=True,
            allow_inline_ssd=False,
        ),
    },
)

Behavior.READ_FAST = Behavior.DEFAULT.derive_with_changes(
    "READ_FAST",
    reads=Settings(
        socket_timeout=timedelta(milliseconds=50),
        total_timeout=timedelta(milliseconds=200),
        max_retries=3,
    ),
)

Behavior.STRICTLY_CONSISTENT = Behavior.DEFAULT.derive_with_changes(
    "STRICTLY_CONSISTENT",
    reads_sc=Settings(read_mode_sc=ReadModeSC.LINEARIZE),
)

Behavior.FAST_RACK_AWARE = Behavior.READ_FAST.derive_with_changes(
    "FAST_RACK_AWARE",
    reads=Settings(replica=Replica.PREFER_RACK),
    reads_sc=Settings(read_mode_sc=ReadModeSC.SESSION),
)
