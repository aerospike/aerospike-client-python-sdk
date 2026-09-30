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

"""Node — one server node of a connected cluster (sync)."""

from __future__ import annotations

from typing import Dict

from aerospike_sdk.exceptions import PacAerospikeError, _convert_pac_exception
from aerospike_sdk.node_shared import NodeBase


class Node(NodeBase):
    """One server node of a connected cluster (synchronous).

    Obtained from :meth:`Cluster.nodes <aerospike_sdk.sync.cluster.Cluster.nodes>`
    or :meth:`Cluster.get_node <aerospike_sdk.sync.cluster.Cluster.get_node>`.
    Properties and :meth:`aliases` read state the client already holds;
    :meth:`info` sends a command to this node specifically, which a
    cluster-wide info call cannot do.

    Example::

        for node in cluster.nodes():
            build = node.info("build")["build"]
            print(f"{node.name} {node.address}: {build}")

    See Also:
        :class:`aerospike_sdk.aio.node.Node`: The async counterpart.
        :class:`~aerospike_sdk.sync.info.InfoCommands`: Parsed cluster-wide info.
    """

    __slots__ = ()

    def info(self, command: str) -> Dict[str, str]:
        """Run a raw info command on this node only.

        Example::

            node = cluster.get_node("BB9D4EB574A8DA6")
            roster = node.info("roster:namespace=test")
            print(roster["roster:namespace=test"])

        Args:
            command: The info command, e.g. ``"build"`` or ``"namespace/test"``.

        Returns:
            The response keyed by command. A command the server rejects comes
            back as an ``ERROR:...`` value, not as an exception.

        Raises:
            AerospikeError: If the node cannot be reached.

        See Also:
            :meth:`InfoCommands.info_on_all_nodes
            <aerospike_sdk.sync.info.InfoCommands.info_on_all_nodes>`: The same
            command on every node at once.
        """
        try:
            return self._pac.info_blocking(command)
        except PacAerospikeError as e:
            raise _convert_pac_exception(e) from e
