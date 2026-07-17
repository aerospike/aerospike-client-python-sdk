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

"""ClusterDefinition - Builder for configuring Aerospike cluster connections."""

from __future__ import annotations

import os
from typing import List, Optional, Union

from aerospike_async import AuthMode, ClientPolicy

from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.aio.tls_builder import TlsBuilder
from aerospike_sdk.policy.sdk_config_loader import load_at_connect
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource

# Client identifier sent to the server (user-agent), overriding the underlying
# async client's own id so PSDK usage is distinguishable on the wire.
try:
    from importlib.metadata import version as _pkg_version

    _SDK_CLIENT_ID = f"python-sdk-{_pkg_version('aerospike-sdk')}"
except Exception:
    _SDK_CLIENT_ID = "python-sdk-0.0.0"


class Host:
    """Seed address for cluster discovery.

    Example::

        host = Host("192.168.1.10", 3000)
        # or use the convenience parser
        hosts = Host.parse_hosts("host1:3000,host2:3000", 3000)
    """
    
    def __init__(
        self,
        name: str,
        port: int,
        tls_name: Optional[str] = None,
    ) -> None:
        """
        Initialize a Host.
        
        Args:
            name: Hostname or IP address
            port: Port number
            tls_name: Optional TLS name for certificate validation
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
        """
        return Host(name, port)
    
    @staticmethod
    def parse_hosts(host_string: str, default_port: int) -> List[Host]:
        """Parse a host string into a list of Host objects.

        Format: ``host1:port1,host2:port2`` or ``host1,host2`` (uses ``default_port``).

        Raises:
            ValueError: If a port segment is present but not a valid integer.
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


class ClusterDefinition:
    """Configure seeds, auth, TLS, rack awareness, and validation before :meth:`connect`.

    Call :meth:`connect` to obtain a live :class:`~aerospike_sdk.aio.cluster.Cluster`.
    The sync counterpart lives under ``aerospike_sdk.sync.cluster_definition``.

    Example::

            cluster = await (
                ClusterDefinition("localhost", 3100)
                .with_native_credentials("user", "secret")
                .using_services_alternate()
                .preferring_racks(1, 2)
                .validate_cluster_name_is("my-cluster")
                .connect()
            )

    See Also:
        :class:`~aerospike_sdk.aio.cluster.Cluster`
    """
    
    def __init__(
        self,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        hosts: Optional[Union[List[Host], tuple[Host, ...]]] = None,
    ) -> None:
        """
        Create a cluster definition.
        
        Args:
            hostname: Hostname or IP address (if single host)
            port: Port number (if single host)
            hosts: List of Host objects (if multiple hosts)
        
        Examples:
            ClusterDefinition("localhost", 3000)
            ClusterDefinition(hosts=[Host.of("host1", 3000), Host.of("host2", 3000)])
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
        self._tls_builder: Optional[TlsBuilder] = None
        self._system_settings: Optional[SystemSettings] = None
        self._app_id: Optional[str] = None
        self._index_refresh_interval: float = 5.0

    def with_index_refresh_interval(
        self, seconds: float
    ) -> ClusterDefinition:
        """Set how often the secondary-index metadata cache refreshes.

        The client polls ``sindex-list`` / ``sindex-stat`` on this interval to
        keep the per-namespace index cache current, which the AEL query planner
        consults to pick secondary-index plans. Lower values pick up new indexes
        faster at the cost of more background info traffic; the default is 5
        seconds.

        Args:
            seconds: Refresh interval in seconds.

        Returns:
            This ClusterDefinition for method chaining.

        Example::

            cd = ClusterDefinition("localhost", 3000).with_index_refresh_interval(2.0)
        """
        self._index_refresh_interval = seconds
        return self

    def app_id(self, app_id: str) -> ClusterDefinition:
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

    def with_native_credentials(
        self,
        user_name: str,
        password: str,
    ) -> ClusterDefinition:
        """
        Sets authentication credentials using Aerospike's internal authentication.

        Hashed password is stored on the server. Pass empty strings for both
        parameters to disable authentication.

        Args:
            user_name: The username for authentication
            password: The password for authentication

        Returns:
            This ClusterDefinition for method chaining

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
    
    def with_external_credentials(
        self,
        user_name: str,
        password: str,
    ) -> ClusterDefinition:
        """
        Sets authentication credentials using external authentication (e.g. LDAP).

        External authentication is configured on the server. If TLS is configured,
        the clear password is sent on node login via TLS. Raises an error at
        connect time if TLS is not configured.

        Args:
            user_name: The username for authentication
            password: The password for authentication

        Returns:
            This ClusterDefinition for method chaining
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
    
    def with_certificate_credentials(self) -> ClusterDefinition:
        """
        Configures certificate-based (PKI) authentication.

        Uses client certificates instead of username/password credentials.
        Automatically enables TLS if not already configured. Requires
        server version 5.7.0+.

        Returns:
            This ClusterDefinition for method chaining

        Raises:
            ValueError: If any host is missing a TLS name
        """
        self._auth_mode = AuthMode.PKI
        self._user_name = None
        self._password = None
        if not self._tls_builder:
            self._tls_builder = TlsBuilder(self)
        return self
    
    @property
    def auth_mode(self) -> AuthMode:
        """The current authentication mode."""
        return self._auth_mode

    def validate_cluster_name_is(self, cluster_name: str) -> ClusterDefinition:
        """
        Validates that the cluster name matches the expected value.
        
        This enables cluster name validation to ensure the client connects to
        the expected cluster. If the actual cluster name doesn't match, the connection
        will fail.
        
        Args:
            cluster_name: The expected cluster name to validate against
        
        Returns:
            This ClusterDefinition for method chaining

        Example::

            cd = ClusterDefinition("localhost", 3000).validate_cluster_name_is("my-cluster")
        """
        self._cluster_name = cluster_name
        return self
    
    def preferring_racks(self, *racks: int) -> ClusterDefinition:
        """
        Sets preferred racks for rack-aware operations.
        
        This enables rack awareness and specifies which racks should be preferred
        for read operations. Rack awareness helps improve performance by reading from
        local racks when possible.
        
        Args:
            *racks: The rack IDs to prefer, in order of preference

        Returns:
            This builder for chaining.
        """
        self._preferred_racks = list(racks) if racks else None
        return self
    
    def using_services_alternate(self) -> ClusterDefinition:
        """
        Enables the use of alternate services for cluster discovery.
        
        When enabled, the client will use alternate service endpoints for
        cluster discovery, which can be useful in certain network configurations
        or when using service mesh solutions.

        Returns:
            This builder for chaining.
        """
        self._use_services_alternate = True
        return self

    def fail_if_not_connected(self, fail: bool) -> ClusterDefinition:
        """
        Controls whether ``connect()`` raises if the cluster is unreachable.

        If ``True`` (the default), ``connect()`` raises a ``ConnectionError``
        when all seed connections fail or a seed connects but none of its
        peers are reachable.

        If ``False``, a partial cluster is created and the client will
        automatically connect to the remaining nodes when they become
        available.

        Args:
            fail: Whether to raise on connection failure.

        Returns:
            This builder for chaining.
        """
        self._fail_if_not_connected = fail
        return self

    def with_ip_map(self, ip_map: dict[str, str]) -> ClusterDefinition:
        """
        Sets an IP address translation table for cluster node discovery.

        Used when clients from different networks need different IP addresses
        to reach the same server nodes (e.g. inside vs. outside a VPN or NAT).
        The key is the IP address returned from server info requests; the value
        is the actual IP address the client should connect to.

        Consider using ``using_services_alternate()`` instead, which lets the
        server handle address translation without client-side configuration.

        Args:
            ip_map: Mapping of server-reported IPs to actual connection IPs

        Returns:
            This builder for chaining.
        """
        self._ip_map = ip_map if ip_map else None
        return self

    def with_system_settings(self, settings: SystemSettings) -> ClusterDefinition:
        """
        Set cluster-wide system settings (connection pool, tend interval, etc.).

        Args:
            settings: The SystemSettings to apply.

        Returns:
            This builder for chaining.

        Example::

            cluster = await ClusterDefinition("localhost", 3000) \\
                .with_system_settings(SystemSettings(
                    max_connections_per_node=200,
                    tend_interval=timedelta(seconds=2),
                )) \\
                .connect()
        """
        self._system_settings = settings
        return self

    def with_tls_config_of(self) -> TlsBuilder:
        """
        Begins TLS configuration using a chainable builder pattern.
        
        This method returns a TlsBuilder that allows you to configure various
        TLS settings such as TLS name, CA file, protocols, ciphers, and other
        TLS-specific options. Call done() on the TlsBuilder to return
        to this ClusterDefinition for further configuration.

        Returns:
            A TlsBuilder for configuring TLS settings.
        """
        self._tls_builder = TlsBuilder(self)
        return self._tls_builder
    
    def _get_policy(self, system_settings: Optional[SystemSettings] = None) -> ClientPolicy:
        """Build a ClientPolicy from the configuration.

        Args:
            system_settings: Effective settings to apply (the file layer
                merged over :meth:`with_system_settings`). Defaults to the
                programmatic settings alone.
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
        """
        Gets the effective hosts array, potentially creating new Host instances with TLS names
        if TLS is configured and the existing hosts don't have TLS names set.
        """
        if not self._tls_builder or not self._tls_builder.is_tls_enabled():
            return self._hosts
        
        tls_name = self._tls_builder.get_tls_name()
        if not tls_name:
            return self._hosts
        
        # Create new hosts with TLS names if they don't have them
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

    async def connect(self) -> Cluster:
        """
        Establishes a connection to the Aerospike cluster.

        This method creates and returns a Cluster instance using the configured
        parameters. The returned Cluster should be closed when no longer needed
        to properly release resources.

        Example with async context manager::

                async with await ClusterDefinition("localhost", 3100).connect() as cluster:
                    session = cluster.create_session(Behavior.DEFAULT)
                    # Use the session...

        Returns:
            A connected Cluster instance

        Raises:
            ValueError: If PKI auth is configured but hosts are missing TLS names
            ConnectionError: If ``fail_if_not_connected`` is True (default) and
                the cluster is unreachable
        """
        self._validate()
        # SDK config file (AEROSPIKE_SDK_CONFIG_URL): applies the behaviors
        # section, layers system settings over programmatic ones per-field,
        # and arms hot-reload on the client when a path is configured.
        settings, config_path, raw = load_at_connect(self._cluster_name, self._system_settings)
        config_source = (
            SdkConfigSource(config_path, self._cluster_name, self._system_settings, raw)
            if config_path is not None
            else None
        )
        policy = self._get_policy(settings)
        seeds = self._build_seeds_string()
        return await Cluster._create(
            policy,
            seeds,
            index_refresh_interval=self._index_refresh_interval,
            sdk_settings=settings,
            sdk_config_source=config_source,
        )

