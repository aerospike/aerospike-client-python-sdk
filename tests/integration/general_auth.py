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

"""General-leg auth helpers for tests that build raw ClusterDefinitions.

Most integration tests reach the cluster through the auth-aware conftest
fixtures (``make_cluster_definition`` / ``client_policy``). Tests whose
*subject* is the ClusterDefinition construction idiom itself (entry-idiom,
parity-example, and config suites) build definitions inline instead — and
would silently fail to connect on the Mode-axis SC leg, whose seed requires
authentication. :func:`apply_general_auth` is the one-line wrap for those
sites: a pass-through that applies ``AEROSPIKE_AUTH_*`` only when the
``AEROSPIKE_GENERAL_AUTH`` opt-in is set, keeping the default AP leg no-auth.

A plain module (not a conftest fixture) for the same reason as
:mod:`tests.integration.namespace`: an imported function needs no fixture
parameter threaded through every test signature, and a name that is only
ever called cannot collide with local variables.

See Also:
    ``conftest._apply_auth_to_definition`` / ``conftest._general_auth_enabled``
    delegate here so the env contract has a single source.
"""

import os

_TRUTHY = ("1", "true", "yes", "on")
_KNOWN_MODES = ("INTERNAL", "EXTERNAL", "PKI")


def general_auth_enabled() -> bool:
    """Whether the general suites should authenticate — opt-in only.

    Controlled by ``AEROSPIKE_GENERAL_AUTH`` (the ``make test-sc`` leg sets
    it). The default AP fast path stays no-auth: sending credentials to a
    cluster that does not require them costs ~1s per client on some configs.
    """
    return os.environ.get("AEROSPIKE_GENERAL_AUTH", "").strip().lower() in _TRUTHY


def apply_auth_to_definition(cluster_def):
    """Apply ``AEROSPIKE_AUTH_*`` env vars to *cluster_def*, if any are set.

    Works on both the async and sync ClusterDefinition (same credential
    methods). Returns *cluster_def* for chaining.
    """
    mode = os.environ.get("AEROSPIKE_AUTH_MODE", "").strip().upper()
    if not mode or mode not in _KNOWN_MODES:
        return cluster_def
    user = os.environ.get("AEROSPIKE_AUTH_USER", "")
    password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "")
    if mode == "INTERNAL":
        cluster_def.with_native_credentials(user, password)
    elif mode == "EXTERNAL":
        cluster_def.with_external_credentials(user, password)
    else:  # PKI
        cluster_def.with_certificate_credentials()
    return cluster_def


def general_seed() -> str:
    """The seed the general suites should target — mirrors ``aerospike_host``.

    On the Mode-axis SC leg (:func:`general_auth_enabled`) the general suites
    target ``AEROSPIKE_HOST_SC`` when set, falling back to ``AEROSPIKE_HOST``.
    For module-level constants and helpers that cannot take the
    ``aerospike_host`` fixture; reading ``AEROSPIKE_HOST`` directly would
    silently keep a test on the AP seed while :func:`~tests.integration.namespace.general_namespace`
    names an SC namespace the AP cluster does not serve.
    """
    if general_auth_enabled():
        sc_seed = os.environ.get("AEROSPIKE_HOST_SC", "").strip()
        if sc_seed:
            return sc_seed
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


def apply_general_auth(cluster_def):
    """Pass-through auth wrap for raw ClusterDefinition test sites.

    No-op unless :func:`general_auth_enabled`; otherwise applies
    ``AEROSPIKE_AUTH_*``. Always returns *cluster_def*, so call sites stay
    chainable: ``apply_general_auth(ClusterDefinition(host, port)).connect()``.
    """
    if general_auth_enabled():
        apply_auth_to_definition(cluster_def)
    return cluster_def
