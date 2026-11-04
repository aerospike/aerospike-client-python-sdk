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

"""Seed discovery against a live cluster: the provider's seeds are the ones used."""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from aerospike_native import Host as PncHost

from aerospike_sdk import (
    ClusterDefinition,
    DiscoveryExecutionMode,
    SeedCandidateProvider,
    SeedDiscoveryError,
    SeedMergePolicy,
)

# A static seed nothing listens on, so connecting proves the provider's seeds
# were the ones used.
_UNREACHABLE = "127.0.0.1:1"


def _seed(aerospike_host: str) -> PncHost:
    name, port = aerospike_host.rsplit(":", 1)
    return PncHost(name, int(port))


class SeedRegistry(SeedCandidateProvider):
    """Returns a fixed seed, recording the context of each refresh."""

    def __init__(self, seed):
        self.seed = seed
        self.contexts = []

    def refresh_seed_candidates(self, ctx):
        self.contexts.append(ctx)
        return [self.seed]


class RegistryDown(SeedCandidateProvider):
    def refresh_seed_candidates(self, ctx):
        raise RuntimeError("registry down")


class TestSeedCandidateProvider:
    """A provider passed in code supplies the seeds the cluster connects through."""

    async def test_python_provider_in_thread_mode_supplies_the_seeds(
        self, aerospike_host, make_cluster_definition,
    ):
        registry = SeedRegistry(_seed(aerospike_host))
        cd = make_cluster_definition(_UNREACHABLE).with_seed_discovery(
            registry,
            merge_policy=SeedMergePolicy.DISCOVERY_ONLY,
            execution_mode=DiscoveryExecutionMode.THREAD,
        )
        async with cd.connect() as cluster:
            assert cluster.node_names()
        assert registry.contexts[0].tend_iteration == 0
        assert [str(h) for h in registry.contexts[0].configured_seeds] == [_UNREACHABLE]

    async def test_a_failed_provider_with_no_seeds_left_is_a_seed_discovery_error(self):
        cd = ClusterDefinition(hosts=[]).with_seed_discovery(RegistryDown())
        with pytest.raises(SeedDiscoveryError, match="registry down"):
            await cd.connect()


@pytest.fixture
def ec2_endpoint(aerospike_host):
    """A local EC2 endpoint answering DescribeInstances with the test seed's address.

    Yields the endpoint URL and the form bodies of the requests it received.
    """
    body = (
        '<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">'
        "<requestId>r-1</requestId><reservationSet><item><instancesSet><item>"
        f"<instanceId>i-0123</instanceId><privateIpAddress>{_seed(aerospike_host).name}"
        "</privateIpAddress></item></instancesSet></item></reservationSet>"
        "</DescribeInstancesResponse>"
    ).encode()
    forms = []

    class DescribeInstances(BaseHTTPRequestHandler):
        def do_POST(self):
            forms.append(self.rfile.read(int(self.headers["Content-Length"])).decode())
            self.send_response(200)
            self.send_header("Content-Type", "text/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), DescribeInstances)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", forms
    finally:
        server.shutdown()
        server.server_close()


class TestDiscoveryConfigBlock:
    """A provider named in the SDK config file is built and used at connect."""

    async def test_aws_tags_block_supplies_the_seeds(
        self, aerospike_host, make_cluster_definition, ec2_endpoint, tmp_path, monkeypatch,
    ):
        endpoint, forms = ec2_endpoint
        config = tmp_path / "sdk.yaml"
        config.write_text(
            "system:\n  DEFAULT:\n    discovery:\n"
            "      provider: aws-tags\n      merge_policy: discovery_only\n"
            "      aws:\n        region: us-west-2\n"
            "        filters:\n          - {name: \"tag:role\", values: [aerospike]}\n"
            f"        port: {_seed(aerospike_host).port}\n"
            f"        endpoint: {endpoint}\n"
        )
        monkeypatch.setenv("AEROSPIKE_SDK_CONFIG_URL", str(config))
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "example-secret")
        cd = make_cluster_definition(_UNREACHABLE).with_strict_config()
        async with cd.connect() as cluster:
            assert cluster.node_names()
        assert "Filter.1.Name=tag%3Arole" in forms[0]
