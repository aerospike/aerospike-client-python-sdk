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

"""Neutral cluster-level types shared by the async + sync trees.

No asyncio anywhere. Lives at the package root so neither
:mod:`aerospike_sdk.aio.cluster_definition` nor
:mod:`aerospike_sdk.sync.cluster_definition` has to reach across tiers for
these. Both trees import the same :class:`Host` from here, so the seed-address
value type is defined once and cannot drift.
"""

from __future__ import annotations

import os
from typing import Any, Generic, List, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from aerospike_async import AuthMode, ClientPolicy

from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.system_settings import SystemSettings

# Client identifier sent to the server (user-agent), overriding the underlying
# async client's own id so PSDK usage is distinguishable on the wire.
try:
    from importlib.metadata import version as _pkg_version

    _SDK_CLIENT_ID = f"python-sdk-{_pkg_version('aerospike-sdk')}"
except Exception:
    _SDK_CLIENT_ID = "python-sdk-0.0.0"


class _TlsBuilderLike(Protocol):
    """The slice of a tree's ``TlsBuilder`` the shared definition base consumes."""

    def is_tls_enabled(self) -> bool: ...
    def build_tls_config(self) -> Optional[Any]: ...
    def get_tls_name(self) -> Optional[str]: ...


# The tree's ``TlsBuilder`` type. Each leaf binds this to its own class so the
# builder chain (``with_tls_config_of`` and friends) stays precisely typed
# without the shared base importing either tree.
_TB = TypeVar("_TB", bound=_TlsBuilderLike)

# The cluster's tree-appropriate session / transactional-session types. Each
# leaf binds these (via forward-reference strings, so the runtime never has to
# import the session modules and risk a cycle) so the shared factories return
# the runtime-appropriate type.
_S = TypeVar("_S")
_TS = TypeVar("_TS")


class Host:
    """Seed address for cluster discovery.

    Example::

        host = Host("192.168.1.10", 3000)
        # or use the convenience parser
        hosts = Host.parse_hosts("host1:3000,host2:3000", 3000)

    See Also:
        :meth:`of`: Construct a single host.
        :meth:`parse_hosts`: Parse a comma-separated seed string.
    """

    def __init__(
        self,
        name: str,
        port: int,
        tls_name: Optional[str] = None,
    ) -> None:
        """Initialize a Host.

        Args:
            name: Hostname or IP address.
            port: Port number.
            tls_name: Optional TLS name for certificate validation.
        """
        self.name = name
        self.port = port
        self.tls_name = tls_name

    @staticmethod
    def of(name: str, port: int) -> Host:
        """Create a Host instance.

        Args:
            name: Hostname or IP address.
            port: Port number.

        Returns:
            A Host with the given name and port.

        Example::

            host = Host.of("192.168.1.10", 3000)

        See Also:
            :meth:`parse_hosts`: Build many hosts from one seed string.
        """
        return Host(name, port)

    @staticmethod
    def parse_hosts(host_string: str, default_port: int) -> List[Host]:
        """Parse a host string into a list of Host objects.

        Format: ``host1:port1,host2:port2`` or ``host1,host2`` (uses
        ``default_port`` for segments without an explicit port).

        Args:
            host_string: Comma-separated seed addresses.
            default_port: Port applied to segments that omit one.

        Returns:
            One :class:`Host` per comma-separated segment.

        Raises:
            ValueError: If a port segment is present but not a valid integer.

        Example::

            hosts = Host.parse_hosts("host1:3000,host2", 3000)

        See Also:
            :meth:`of`: Construct a single host.
        """
        hosts = []
        for host_part in host_string.split(","):
            host_part = host_part.strip()
            if ":" in host_part:
                name, port_str = host_part.rsplit(":", 1)
                port = int(port_str)
            else:
                name = host_part
                port = default_port
            hosts.append(Host(name, port))
        return hosts


class ClusterDefinitionBase(Generic[_TB]):
    """Runtime-agnostic cluster-definition builder shared by both trees.

    Holds everything that is pure configuration state: seed/host handling, auth,
    rack awareness, IP mapping, system settings, TLS wiring, and the
    ``ClientPolicy`` / seeds-string assembly used at connect time. The only
    runtime-bound pieces stay on the leaves: ``connect()`` (async ``await`` vs
    blocking) and constructing the tree's ``TlsBuilder`` (behind
    :meth:`_new_tls_builder`).

    Defining these builder methods once means the two trees cannot drift on the
    accepted configuration surface or on how a ``ClientPolicy`` is assembled.
    """

    def __init__(
        self,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        hosts: Optional[Union[List[Host], tuple[Host, ...]]] = None,
    ) -> None:
        """Create a cluster definition.

        Args:
            hostname: Hostname or IP address (single-host form).
            port: Port number (single-host form).
            hosts: List of :class:`Host` objects (multi-host form).

        Raises:
            ValueError: If neither ``(hostname, port)`` nor ``hosts`` is given.

        Example::

            cd = ClusterDefinition("localhost", 3000)
            cd = ClusterDefinition(hosts=[Host.of("host1", 3000), Host.of("host2", 3000)])
        """
        if hosts is not None:
            self._hosts = list(hosts)
        elif hostname is not None and port is not None:
            self._hosts = [Host(hostname, port)]
        else:
            raise ValueError("Either (hostname, port) or hosts must be provided")

        self._auth_mode: AuthMode = AuthMode.NONE
        self._user_name: Optional[str] = None
        self._password: Optional[str] = None
        self._cluster_name: Optional[str] = None
        self._preferred_racks: Optional[List[int]] = None
        self._use_services_alternate = os.environ.get(
            "AEROSPIKE_USE_SERVICES_ALTERNATE", ""
        ).strip().lower() in ("true", "1", "yes")
        self._fail_if_not_connected = True
        self._ip_map: Optional[dict[str, str]] = None
        self._tls_builder: Optional[_TB] = None
        self._system_settings: Optional[SystemSettings] = None
        self._app_id: Optional[str] = None

    # -- Per-leaf hook --------------------------------------------------------

    def _new_tls_builder(self) -> _TB:
        """Return a new tree-appropriate ``TlsBuilder`` bound to this definition.

        Overridden per leaf so the shared base never imports either tree's
        ``TlsBuilder``. Not called on the base.
        """
        raise NotImplementedError

    # -- Builder chain (pure state mutation) ----------------------------------

    def app_id(self, app_id: str) -> Self:
        """Tag this client's traffic with an application identifier.

        The identifier is reported to the server (as the application portion of
        the client's user-agent), letting operators attribute load per calling
        application. It is distinct from the client-library identifier the SDK
        sets automatically.

        Args:
            app_id: A short label for the calling application, e.g.
                ``"billing-service"``.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).app_id("billing-service")
        """
        self._app_id = app_id
        return self

    def with_native_credentials(self, user_name: str, password: str) -> Self:
        """Set authentication credentials using Aerospike's internal authentication.

        Hashed password is stored on the server. Pass empty strings for both
        parameters to disable authentication.

        Args:
            user_name: The username for authentication.
            password: The password for authentication.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).with_native_credentials("admin", "pass123")
        """
        if not user_name:
            self._auth_mode = AuthMode.NONE
            self._user_name = None
            self._password = None
        else:
            self._auth_mode = AuthMode.INTERNAL
            self._user_name = user_name
            self._password = password
        return self

    def with_external_credentials(self, user_name: str, password: str) -> Self:
        """Set authentication credentials using external authentication (e.g. LDAP).

        External authentication is configured on the server. If TLS is
        configured, the clear password is sent on node login via TLS. Raises an
        error at connect time if TLS is not configured.

        Args:
            user_name: The username for authentication.
            password: The password for authentication.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).with_external_credentials("ldap_user", "pass")
        """
        if not user_name:
            self._auth_mode = AuthMode.NONE
            self._user_name = None
            self._password = None
        else:
            self._auth_mode = AuthMode.EXTERNAL
            self._user_name = user_name
            self._password = password
        return self

    def with_certificate_credentials(self) -> Self:
        """Configure certificate-based (PKI) authentication.

        Uses client certificates instead of username/password credentials.
        Automatically enables TLS if not already configured. Requires server
        version 5.7.0+.

        Returns:
            This ClusterDefinition for method chaining.

        Raises:
            ValueError: At connect time if any host is missing a TLS name.

        Example::

            cd = ClusterDefinition("localhost", 4333).with_certificate_credentials()
        """
        self._auth_mode = AuthMode.PKI
        self._user_name = None
        self._password = None
        if not self._tls_builder:
            self._tls_builder = self._new_tls_builder()
        return self

    @property
    def auth_mode(self) -> AuthMode:
        """The current authentication mode."""
        return self._auth_mode

    def validate_cluster_name_is(self, cluster_name: str) -> Self:
        """Validate that the cluster name matches the expected value.

        Enables cluster-name validation so the client only connects to the
        expected cluster. If the actual cluster name does not match, the
        connection fails.

        Args:
            cluster_name: The expected cluster name to validate against.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).validate_cluster_name_is("my-cluster")
        """
        self._cluster_name = cluster_name
        return self

    def preferring_racks(self, *racks: int) -> Self:
        """Set preferred racks for rack-aware operations.

        Enables rack awareness and specifies which racks to prefer for read
        operations, improving performance by reading from local racks when
        possible.

        Args:
            *racks: The rack IDs to prefer, in order of preference.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).preferring_racks(1, 2)
        """
        self._preferred_racks = list(racks) if racks else None
        return self

    def using_services_alternate(self, enabled: bool = True) -> Self:
        """Enable (or disable) alternate services for cluster discovery.

        When enabled, the client discovers peers through each node's
        ``alternate-access-address`` instead of its standard service address —
        useful in certain network configurations or service-mesh setups. Only
        enable it against a cluster that actually publishes those addresses:
        against one that does not, peer discovery comes back empty and the
        client falls back to a single node, so every key outside that node's
        partitions fails to route.

        Passing ``enabled=False`` is the only way to turn the setting back off
        once it defaults on, which it does whenever
        ``AEROSPIKE_USE_SERVICES_ALTERNATE`` is truthy in the environment.

        Args:
            enabled: Whether to use alternate service endpoints. Defaults to
                ``True`` so the no-argument call reads as an enable.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("bench-asd", 3000).using_services_alternate(False)

        See Also:
            :meth:`with_ip_map`: Client-side address translation, for when the
                cluster publishes no alternate addresses.
        """
        self._use_services_alternate = enabled
        return self

    def fail_if_not_connected(self, fail: bool) -> Self:
        """Control whether ``connect()`` raises if the cluster is unreachable.

        If ``True`` (the default), ``connect()`` raises a ``ConnectionError``
        when all seed connections fail or a seed connects but none of its peers
        are reachable. If ``False``, a partial cluster is created and the client
        connects to the remaining nodes as they become available.

        Args:
            fail: Whether to raise on connection failure.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).fail_if_not_connected(False)
        """
        self._fail_if_not_connected = fail
        return self

    def with_ip_map(self, ip_map: dict[str, str]) -> Self:
        """Set an IP address translation table for cluster node discovery.

        Used when clients from different networks need different IP addresses to
        reach the same server nodes (e.g. inside vs. outside a VPN or NAT). The
        key is the IP address returned from server info requests; the value is
        the actual IP address the client should connect to.

        Consider using :meth:`using_services_alternate` instead, which lets the
        server handle address translation without client-side configuration.

        Args:
            ip_map: Mapping of server-reported IPs to actual connection IPs.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).with_ip_map({"10.0.0.1": "192.168.1.1"})
        """
        self._ip_map = ip_map if ip_map else None
        return self

    def with_system_settings(self, settings: SystemSettings) -> Self:
        """Set cluster-wide system settings (connection pool, tend interval, etc.).

        Args:
            settings: The :class:`~aerospike_sdk.policy.system_settings.SystemSettings`
                to apply.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            from datetime import timedelta
            cd = ClusterDefinition("localhost", 3000).with_system_settings(
                SystemSettings(max_connections_per_node=200, tend_interval=timedelta(seconds=2)),
            )
        """
        self._system_settings = settings
        return self

    def with_tls_config_of(self) -> _TB:
        """Begin TLS configuration using a chainable builder.

        Returns a ``TlsBuilder`` for configuring TLS name, CA file, protocols,
        ciphers, and other TLS options. Call ``done()`` on it to return to this
        ClusterDefinition for further configuration.

        Returns:
            A ``TlsBuilder`` for configuring TLS settings.

        Example::

            cd = ClusterDefinition("localhost", 4333).with_tls_config_of().with_ca_file("/certs/ca.pem").done()
        """
        self._tls_builder = self._new_tls_builder()
        return self._tls_builder

    # -- Connect-time assembly (shared) ---------------------------------------

    def _get_policy(self, system_settings: Optional[SystemSettings] = None) -> ClientPolicy:
        """Build a ClientPolicy from the configuration.

        Args:
            system_settings: Effective settings to apply (the file layer merged
                over :meth:`with_system_settings`). Defaults to the programmatic
                settings alone.
        """
        if system_settings is None:
            system_settings = self._system_settings
        policy = ClientPolicy()

        # Override the underlying client's user-agent id with PSDK's own.
        policy.custom_client_id = _SDK_CLIENT_ID
        if self._app_id is not None:
            policy.application_id = self._app_id

        policy.use_services_alternate = self._use_services_alternate
        policy.fail_if_not_connected = self._fail_if_not_connected

        # Authentication
        policy.set_auth_mode(self._auth_mode, self._user_name, self._password)

        # Rack awareness (setting rack_ids automatically enables rack awareness)
        if self._preferred_racks:
            policy.rack_ids = self._preferred_racks

        # Cluster name validation (setting cluster_name enables validation)
        if self._cluster_name:
            policy.cluster_name = self._cluster_name

        # IP address translation
        if self._ip_map:
            policy.ip_map = self._ip_map

        # TLS configuration
        if self._tls_builder and self._tls_builder.is_tls_enabled():
            tls_config = self._tls_builder.build_tls_config()
            if tls_config is not None:
                policy.tls_config = tls_config

        # System settings (connection pool, tend interval, etc.)
        if system_settings is not None:
            system_settings.apply_to(policy)

        return policy

    def _get_effective_hosts(self) -> List[Host]:
        """Return hosts, adding TLS names when TLS is enabled and they are unset."""
        if not self._tls_builder or not self._tls_builder.is_tls_enabled():
            return self._hosts

        tls_name = self._tls_builder.get_tls_name()
        if not tls_name:
            return self._hosts

        new_hosts = []
        for host in self._hosts:
            if host.tls_name is None:
                new_hosts.append(Host(host.name, host.port, tls_name))
            else:
                new_hosts.append(host)
        return new_hosts

    def _build_seeds_string(self) -> str:
        """Build a seeds string from the hosts list.

        Format is ``host:port`` or ``host:tls_name:port`` when a TLS name is set.
        """
        effective_hosts = self._get_effective_hosts()
        parts = []
        for host in effective_hosts:
            if host.tls_name:
                parts.append(f"{host.name}:{host.tls_name}:{host.port}")
            else:
                parts.append(f"{host.name}:{host.port}")
        return ",".join(parts)

    def _validate(self) -> None:
        """Validate the configuration before connecting."""
        if self._auth_mode == AuthMode.PKI:
            effective = self._get_effective_hosts()
            missing = [h.name for h in effective if not h.tls_name]
            if missing:
                raise ValueError(
                    f"PKI authentication requires TLS names on all hosts. "
                    f"Missing TLS name for: {', '.join(missing)}"
                )


class ClusterBase(Generic[_S, _TS]):
    """Runtime-agnostic cluster behavior shared by the async and sync clusters.

    Holds the session / transaction factories, which are pure delegation to the
    owned SDK client and therefore identical across trees. Everything that
    touches the event loop — connect/close, the context-manager protocol, and
    the UDF / index terminals — stays per-leaf (async ``await`` vs blocking).

    Defining the factories once means the two trees cannot drift on how a
    session or transaction is opened from a cluster.
    """

    # Narrowed to ``Client`` / ``SyncClient`` by each leaf's ``__init__``;
    # loose here so the shared factories can delegate without the type-checker
    # flagging the per-tree client's methods. The base only ever delegates to
    # it, so ``Any`` costs no precision on the surfaces users touch.
    _sdk_client: Any

    def create_session(self, behavior: Optional[Behavior] = None) -> _S:
        """Open a session on this cluster with optional behavior.

        A session is a logical connection carrying behavior settings (timeouts,
        retry policies, consistency levels, ...) that govern how operations run.

        Args:
            behavior: Policy bundle for the session. Defaults to
                :attr:`~aerospike_sdk.policy.behavior.Behavior.DEFAULT`.

        Returns:
            A session bound to this cluster's SDK client.

        Example::

            session = cluster.create_session(Behavior.DEFAULT)

        See Also:
            :meth:`transaction`: Open a multi-record transaction instead.
        """
        return self._sdk_client.create_session(behavior)

    def transaction(self, behavior: Optional[Behavior] = None) -> _TS:
        """Open a transactional session for a multi-record transaction (MRT).

        Operations run inside the returned context manager use *behavior* (or
        :attr:`~aerospike_sdk.policy.behavior.Behavior.DEFAULT` when omitted) and
        auto-participate in a fresh :class:`~aerospike_async.Txn`, committed on
        clean exit and aborted if an exception propagates. Requires a
        strong-consistency (SC) namespace.

        Args:
            behavior: Policy bundle for operations inside the transaction.
                Defaults to :attr:`Behavior.DEFAULT`.

        Returns:
            The tree's transactional session, bound to this cluster's client.

        Example::

            with cluster.transaction() as tx:
                tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()

        See Also:
            :meth:`create_session`: Non-transactional session.
        """
        return self._sdk_client.transaction(behavior)

    def is_connected(self) -> bool:
        """Return whether the cluster connection is currently active.

        Returns:
            ``True`` if the underlying client reports a live connection.

        Example::

            assert cluster.is_connected()
        """
        return self._sdk_client.is_connected
