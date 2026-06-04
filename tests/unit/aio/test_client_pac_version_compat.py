# Copyright 2025-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.

"""PAC / :class:`Version` API compatibility for server-compiled filter detection."""

from aerospike_sdk.aio.client import _pac_version_supports_server_compiled_filter


class _VersionWithoutMethod:
    """Mimics older PAC ``Version`` bindings (no server-compiled helper)."""


class _VersionSupportsTrue:
    def supports_server_compiled_filter_expression(self) -> bool:
        return True


class _VersionSupportsFalse:
    def supports_server_compiled_filter_expression(self) -> bool:
        return False


def test_missing_method_means_not_supported() -> None:
    assert _pac_version_supports_server_compiled_filter(_VersionWithoutMethod()) is False


def test_callable_true() -> None:
    assert _pac_version_supports_server_compiled_filter(_VersionSupportsTrue()) is True


def test_callable_false() -> None:
    assert _pac_version_supports_server_compiled_filter(_VersionSupportsFalse()) is False
