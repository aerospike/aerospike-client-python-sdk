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
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, List, Optional, TypeVar, Union

from typing import Self

from aerospike_async import AuthMode, ClientPolicy, TlsConfig, Version

from aerospike_sdk import capabilities
from aerospike_sdk.exceptions import PacAerospikeError, _convert_pac_exception
from aerospike_sdk.metrics import MetricsPolicy, MetricsSnapshot
from aerospike_sdk.metrics.snapshot import ProcessSampler
from aerospike_sdk.metrics.usage import COMMAND_COUNT
from aerospike_sdk.node_shared import NodeBase
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.system_settings import SystemSettings

# Client identifier sent to the server (user-agent), overriding the underlying
# async client's own id so PSDK usage is distinguishable on the wire.
try:
    from importlib.metadata import version as _pkg_version

    _SDK_CLIENT_ID = f"python-sdk-{_pkg_version('aerospike-sdk')}"
except Exception:
    _SDK_CLIENT_ID = "python-sdk-0.0.0"


@dataclass(frozen=True, kw_only=True)
class _TlsSettings:
    """TLS options held until connect, when they become a ``TlsConfig``.

    Building the ``TlsConfig`` opens the CA and client files, so it waits for
    ``connect()`` rather than failing at definition time.
    """

    tls_name: Optional[str] = None
    ca_file: Optional[str] = None
    client_cert_file: Optional[str] = None
    client_key_file: Optional[str] = None
    protocols: Optional[list[str]] = None
    ciphers: Optional[list[str]] = None
    for_login_only: bool = False

    def build_tls_config(self) -> TlsConfig:
        """Build the PAC ``TlsConfig``.

        A config with no CA file still builds: the server is then verified
        against the system trust store, which is what a ``tls_name``-only setup
        needs.

        Raises:
            ValueError: An unrecognized protocol or cipher-suite name.
            aerospike_async.exceptions.IoError: A CA or client file that
                cannot be read.
        """
        if self.client_cert_file:
            return TlsConfig.with_client_auth(
                self.ca_file,
                self.client_cert_file,
                self.client_key_file,
                protocols=self.protocols,
                ciphers=self.ciphers,
                for_login_only=self.for_login_only,
            )
        return TlsConfig(
            self.ca_file,
            protocols=self.protocols,
            ciphers=self.ciphers,
            for_login_only=self.for_login_only,
        )


def _name_list(field: str, names: Optional[Sequence[str]]) -> Optional[list[str]]:
    # A str is itself a Sequence, so a bare "TLSv1.3" would split into characters.
    if isinstance(names, str):
        raise TypeError(f"{field} takes a list of names, not a single string: [{names!r}]")
    if names is None:
        return None
    if not names:
        # Empty would read as "no restriction" and allow every default.
        raise ValueError(f"{field} must name at least one entry; omit it to allow the defaults")
    return list(names)


# The cluster's tree-appropriate session / transactional-session types. Each
# leaf binds these (via forward-reference strings, so the runtime never has to
# import the session modules and risk a cycle) so the shared factories return
# the runtime-appropriate type.
_S = TypeVar("_S")
_TS = TypeVar("_TS")
# The tree's node view: the two differ only in how ``info`` dispatches.
_N = TypeVar("_N", bound=NodeBase)


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


class ClusterDefinitionBase:
    """Runtime-agnostic cluster-definition builder shared by both trees.

    Holds everything that is pure configuration state: seed/host handling, auth,
    rack awareness, IP mapping, system settings, TLS, and the ``ClientPolicy`` /
    seeds-string assembly used at connect time. The only runtime-bound piece
    stays on the leaves: ``connect()`` (async ``await`` vs blocking).

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
        self._seed_only_cluster = False
        self._strict_config = False
        self._ip_map: Optional[dict[str, str]] = None
        self._tls: Optional[_TlsSettings] = None
        self._system_settings: Optional[SystemSettings] = None
        self._app_id: Optional[str] = None

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

    def with_external_insecure_credentials(self, user_name: str, password: str) -> Self:
        """Set external (e.g. LDAP) credentials that may travel without TLS.

        The same server-side external authentication as
        :meth:`with_external_credentials`, but the clear password is sent on
        node login even when TLS is not configured. Use it only on a network
        you trust; prefer :meth:`with_external_credentials` with TLS.

        Args:
            user_name: The username for authentication. An empty name clears
                the credentials.
            password: The password for authentication.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).with_external_insecure_credentials(
                "ldap_user", "pass"
            )
        """
        if not user_name:
            self._auth_mode = AuthMode.NONE
            self._user_name = None
            self._password = None
        else:
            self._auth_mode = AuthMode.EXTERNAL_INSECURE
            self._user_name = user_name
            self._password = password
        return self

    def with_certificate_credentials(self) -> Self:
        """Configure certificate-based (PKI) authentication.

        The server identifies the client by its TLS client certificate instead
        of a user name and password, so the certificate and key must be set
        with :meth:`with_tls_config`.

        Returns:
            This ClusterDefinition for method chaining.

        Raises:
            ValueError: At connect time if no client certificate is configured
                or any host is missing a TLS name.

        Example::

            cd = (
                ClusterDefinition("db.example.com", 4333)
                .with_tls_config(
                    tls_name="db.example.com",
                    ca_file="/certs/ca.pem",
                    client_cert_file="/certs/app.pem",
                    client_key_file="/certs/app.key",
                )
                .with_certificate_credentials()
            )

        See Also:
            :meth:`with_tls_config`: Supplies the client certificate.
        """
        self._auth_mode = AuthMode.PKI
        self._user_name = None
        self._password = None
        return self

    @property
    def auth_mode(self) -> AuthMode:
        """The current authentication mode."""
        return self._auth_mode

    def cluster_name(self, cluster_name: str) -> Self:
        """Declare the name of the cluster this definition connects to.

        The client validates it against each node: nodes reporting a different
        cluster name are rejected, so the connection fails if none match. The
        name also selects the matching ``system.<name>`` profile in a dynamic
        SDK config file before the connection is built.

        Args:
            cluster_name: The expected cluster name.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).cluster_name("prod-east")

        See Also:
            :attr:`ClusterBase.cluster_name`: The name the connected servers report.
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
            :meth:`ip_map`: Client-side address translation, for when the
                cluster publishes no alternate addresses.
        """
        self._use_services_alternate = enabled
        return self

    def force_single_node(self, enabled: bool = True) -> Self:
        """Talk to the first seed only, bypassing peer discovery. **Testing only.**

        Every other client names this ``force_single_node`` / ``forceSingleNode``
        and documents it for testing, and so does the underlying flag's own
        origin. Do not enable it in production.

        With this set, peer discovery is skipped and the seed addresses become
        the whole cluster: peers reported by other nodes are ignored, seeds are
        retained across connection failures rather than dropped by the tend
        loop, and load-balancer detection is skipped so a seed address is
        treated as the canonical service endpoint. No tend task is spawned, so
        node restarts, master failover and rebalances are invisible.

        .. warning::

            On a multi-node cluster this yields **partial availability**, not a
            restricted-but-working view. The partition map still names the real
            owners, including nodes that discovery was told to ignore, so every
            partition the seed does not own becomes unroutable and its
            operations raise ``Cannot get appropriate node for namespace ...
            partition N``. Measured against a 3-node cluster: 6 of 30 writes
            succeeded and 24 raised. Other clients avoid this by rewriting the
            partition map to point every partition at the seed; this client
            does not.

            Use it only against a single-node cluster, or where a test or
            benchmark needs a node set that cannot shift under it.

        Args:
            enabled: Whether to talk to the first seed only. Defaults to
                ``True`` so the no-argument call reads as an enable.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            # Benchmark against a single-node cluster, with no tend noise.
            cd = ClusterDefinition("localhost", 3000).force_single_node()

        See Also:
            :meth:`using_services_alternate`: The production remedy when
                advertised addresses are not routable — discovers peers, but
                addresses them by their alternate address.
            :meth:`ip_map`: Client-side translation of discovered
                addresses.
        """
        self._seed_only_cluster = enabled
        return self

    def with_strict_config(self, strict: bool = True) -> Self:
        """Fail the connect if the SDK config file carries anything unrecognized.

        By default an unrecognized key is warned about and skipped, so a config
        written for a different client or carrying a typo still connects — it
        simply does not apply the settings it names. That is the right default
        for a file shared across SDKs, and the wrong one if you would rather
        find out at deploy time than from behavior in production.

        Strict mode raises on what the loader does not *recognize*, not on
        what it honors at a different scope: a key this SDK accepts elsewhere
        (for example ``wait_for_connection_to_complete``, honored client-wide
        under ``system.<cluster>.connections``) warns with a pointer to the
        right spelling and still connects.

        Applies to the connect-time read only. Hot reload stays fail-soft
        whatever this is set to: the monitor runs in the background with no
        caller to raise to, so it warns and keeps the previous configuration.

        Args:
            strict: Whether to raise on unrecognized config entries. Defaults
                to ``True`` so the no-argument call reads as an enable.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            # Deployment-time gate: a stale key fails the rollout, not a request.
            cd = ClusterDefinition("localhost", 3000).with_strict_config()

        See Also:
            :meth:`with_system_settings`: Settings supplied in code, which the
                file layers over.
        """
        self._strict_config = strict
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

    def ip_map(self, ip_map: dict[str, str]) -> Self:
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

            cd = ClusterDefinition("localhost", 3000).ip_map({"10.0.0.1": "192.168.1.1"})
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

    def with_tls_config(
        self,
        *,
        tls_name: Optional[str] = None,
        ca_file: Optional[str] = None,
        client_cert_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        protocols: Optional[Sequence[str]] = None,
        ciphers: Optional[Sequence[str]] = None,
        for_login_only: bool = False,
    ) -> Self:
        """Connect over TLS with these settings.

        Each call replaces any earlier TLS settings on this definition. The
        settings are built into a TLS configuration at ``connect()``, so that
        is where a missing file, an unknown protocol or cipher name, or a
        protocol and cipher restriction that leaves no usable suite is
        reported.

        Args:
            tls_name: Name the server certificate must carry, also sent as SNI.
                It is applied to every :class:`Host` that has no ``tls_name`` of
                its own.
            ca_file: PEM file of the certificate authorities that sign the
                server certificates. Without one, the system trust store is
                used.
            client_cert_file: PEM client certificate for mutual TLS. Requires
                ``client_key_file``.
            client_key_file: PEM private key for ``client_cert_file``.
            protocols: TLS versions to allow, for example ``["TLSv1.3"]``.
                Defaults to the client's supported set.
            ciphers: Cipher-suite names to allow. Defaults to the client's
                supported set. They must suit an allowed protocol: TLS 1.3
                suites are separate from TLS 1.2 ones.
            for_login_only: Encrypt only the login exchange. The credential
                exchange runs over TLS, that connection closes once the session
                token is held, and every later connection to the node opens in
                cleartext at its cleartext service address; no socket is
                downgraded. Trades data-plane encryption for throughput on
                trusted networks, and requires credentials on this definition.

        Returns:
            This ClusterDefinition for method chaining.

        Raises:
            ValueError: If only one of ``client_cert_file`` and
                ``client_key_file`` is given.
            ValueError: If ``protocols`` or ``ciphers`` is an empty list.
            TypeError: If ``protocols`` or ``ciphers`` is a single string
                rather than a list of names.

        Example::

            cd = (
                ClusterDefinition("db.example.com", 4333)
                .with_tls_config(tls_name="db.example.com", ca_file="/certs/ca.pem")
                .with_native_credentials("app", "secret")
            )

        See Also:
            :meth:`with_certificate_credentials`: Authenticate with the client
            certificate instead of a password.
        """
        if bool(client_cert_file) != bool(client_key_file):
            raise ValueError(
                "mutual TLS needs both client_cert_file and client_key_file",
            )
        self._tls = _TlsSettings(
            tls_name=tls_name,
            ca_file=ca_file,
            client_cert_file=client_cert_file,
            client_key_file=client_key_file,
            protocols=_name_list("protocols", protocols),
            ciphers=_name_list("ciphers", ciphers),
            for_login_only=for_login_only,
        )
        return self

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
        policy.seed_only_cluster = self._seed_only_cluster

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

        if self._tls is not None:
            try:
                policy.tls_config = self._tls.build_tls_config()
            except PacAerospikeError as e:
                raise _convert_pac_exception(e) from e

        # System settings (connection pool, tend interval, etc.)
        if system_settings is not None:
            system_settings.apply_to(policy)

        return policy

    def _get_effective_hosts(self) -> List[Host]:
        """Return hosts, adding TLS names when TLS is enabled and they are unset."""
        tls_name = self._tls.tls_name if self._tls is not None else None
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
            if self._tls is None or not self._tls.client_cert_file:
                raise ValueError(
                    "PKI authentication requires a client certificate: pass "
                    "client_cert_file and client_key_file to with_tls_config()"
                )
            effective = self._get_effective_hosts()
            missing = [h.name for h in effective if not h.tls_name]
            if missing:
                raise ValueError(
                    f"PKI authentication requires TLS names on all hosts. "
                    f"Missing TLS name for: {', '.join(missing)}"
                )


class ClusterBase(Generic[_S, _TS, _N]):
    """Runtime-agnostic cluster behavior shared by the async and sync clusters.

    Holds the session / transaction factories and the cluster-membership reads,
    which are pure delegation to the owned SDK client (or reads of its tended
    node list) and therefore identical across trees. Everything that touches
    the event loop — connect/close, the context-manager protocol, and the
    UDF / index terminals — stays per-leaf (async ``await`` vs blocking).

    Defining these once means the two trees cannot drift on how a session or
    transaction is opened from a cluster, or on what membership it reports.
    """

    # Narrowed to ``Client`` / ``SyncClient`` by each leaf's ``__init__``;
    # loose here so the shared factories can delegate without the type-checker
    # flagging the per-tree client's methods. The base only ever delegates to
    # it, so ``Any`` costs no precision on the surfaces users touch.
    _sdk_client: Any
    _node_cls: type[_N]
    # Set by each leaf's ``__init__`` / ``enable_metrics``; read by :meth:`metrics`.
    _metrics_policy: Optional[MetricsPolicy]
    _process_sampler: ProcessSampler

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

    # -- Cluster membership ----------------------------------------------------
    # Read from the client's tended node list: no server round trip, so plain
    # methods even on the async surface. They track membership as of the last
    # tend.

    @property
    def cluster_name(self) -> Optional[str]:
        """The cluster name the servers report.

        When the connection was built with ``ClusterDefinition.cluster_name()``, nodes
        reporting any other name were rejected, so this equals the validated
        name.

        Example::

            print(f"connected to {cluster.cluster_name}")   # connected to prod-east

        Returns:
            The server's ``cluster-name``, or ``None`` when the servers are
            configured without one.

        See Also:
            :meth:`nodes`: The nodes making up this cluster.
        """
        return self._sdk_client.underlying_client.server_cluster_name

    def nodes(self) -> list[_N]:
        """The nodes currently in the cluster.

        Example::

            versions = {node.name: str(node.version) for node in cluster.nodes()}
            if len(set(versions.values())) > 1:
                print(f"rolling upgrade in progress: {versions}")

        Returns:
            One node view per active node.

        Raises:
            RuntimeError: If not connected.

        See Also:
            :meth:`get_node`: Look up one node by name.
            :meth:`node_names`: Just the names.
        """
        node_cls = self._node_cls
        return [node_cls(pac) for pac in self._sdk_client.underlying_client.nodes()]

    def get_node(self, name: str) -> _N:
        """Look up one node by its name.

        Example::

            node = cluster.get_node("BB9D4EB574A8DA6")
            print(node.version, node.address)

        Args:
            name: The node name, as reported by a node's ``name`` or the
                ``*_per_node`` info helpers.

        Returns:
            The matching node view.

        Raises:
            InvalidNodeError: If no active node has that name.
            RuntimeError: If not connected.

        See Also:
            :meth:`nodes`: Every node at once.
        """
        try:
            return self._node_cls(self._sdk_client.underlying_client.get_node(name))
        except PacAerospikeError as e:
            raise _convert_pac_exception(e) from e

    def node_names(self) -> list[str]:
        """The names of the nodes currently in the cluster.

        Example::

            names = cluster.node_names()
            print(f"{len(names)} nodes: {', '.join(names)}")

        Returns:
            One name per active node.

        Raises:
            RuntimeError: If not connected.

        See Also:
            :meth:`nodes`: The nodes themselves.
        """
        return self._sdk_client.underlying_client.node_names()

    # -- Server-capability probes ---------------------------------------------
    # Guard feature use against the cluster's least-capable node before
    # calling a feature that a mixed-version cluster may not fully support.
    # Each folds the per-node version, so a single lagging node reports the
    # feature unsupported. Read live from the tended node list, so the answer
    # tracks current membership without a round trip.

    def server_version(self) -> Optional[Version]:
        """The minimum server version across connected nodes.

        Returns:
            The least-capable node's :class:`~aerospike_async.Version`, or
            ``None`` when the cluster reports no nodes. Guarding against the
            *minimum* is what makes a feature check safe on a mixed-version
            or mid-upgrade cluster.

        Example::

            v = cluster.server_version()
            if v is not None and (v.major, v.minor, v.patch) >= (8, 2, 0):
                ...

        See Also:
            :meth:`nodes`: Each node's own version.
        """
        return capabilities.min_version(self._sdk_client._cluster_versions())

    def supports_ael(self) -> bool:
        """Whether every node parses server-compiled AEL (filters, exp reads/writes)."""
        return capabilities.supports_ael(self._sdk_client._cluster_versions())

    def supports_query_operations(self) -> bool:
        """Whether every node supports read operations inside an index query."""
        return capabilities.supports_query_operations(
            self._sdk_client._cluster_versions())

    def supports_string_operations(self) -> bool:
        """Whether every node supports the server-side string operations.

        Example::

            if cluster.supports_string_operations():
                await session.upsert(key).bin("s").str_append("!").execute()
        """
        return capabilities.supports_string_operations(
            self._sdk_client._cluster_versions())

    def supports_query_selection(self) -> bool:
        """Whether every node supports server-led index selection (>= 8.2.0)."""
        return capabilities.supports_query_selection(
            self._sdk_client._cluster_versions())

    # -- Metrics snapshot -------------------------------------------------------
    # The core assembles the snapshot from in-memory per-node state: no socket
    # is touched, so this is a plain call on both surfaces. PAC releases the
    # GIL for the copy.

    def metrics(self) -> MetricsSnapshot:
        """Snapshot the accumulated cluster metrics.

        Values are cumulative since metrics were enabled (connection gauges
        are point-in-time). Snapshotting drains and aggregates per-node
        state, so poll at an export interval rather than per operation.

        Returns:
            A :class:`~aerospike_sdk.metrics.MetricsSnapshot`; empty (zeroed) if
            metrics were never enabled.

        Example::

            snapshot = cluster.metrics()
            reads = snapshot.latency(LatencyType.READ)
            print(f"{reads.count} reads, avg {reads.average:.1f}")
        """
        client = self._sdk_client
        pac = client.underlying_client
        client_policy = client._policy
        return MetricsSnapshot(
            pac.metrics(),
            policy=self._metrics_policy,
            nodes=pac.nodes(),
            usage=client._usage_counters.totals(),
            command_count=client._command_counts.totals().get(COMMAND_COUNT, 0),
            # With no application identity of its own, the snapshot reports the
            # authenticated user -- the identity the server already knows this
            # connection by, and so the one that joins the two views.
            app_id=client_policy.application_id or client_policy.user,
            process=self._process_sampler.sample(),
        )

    @property
    def is_connected(self) -> bool:
        """Whether the cluster connection is currently active.

        Returns:
            ``True`` if the underlying client reports a live connection.

        Example::

            assert cluster.is_connected

        See Also:
            :meth:`close`: Release the connection.
        """
        return self._sdk_client.is_connected
