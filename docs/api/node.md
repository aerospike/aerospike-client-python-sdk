# Node

Returned by {meth}`~aerospike_sdk.aio.cluster.Cluster.nodes` and
{meth}`~aerospike_sdk.aio.cluster.Cluster.get_node`, one per server node:

```python
for node in cluster.nodes():
    build = (await node.info("build"))["build"]
    print(f"{node.name} {node.address} v{node.version} build={build}")
```

The properties are the client's own view of the node as of the last cluster tend, so
reading them costs no round trip. {meth}`~aerospike_sdk.aio.node.Node.info` sends the
command to this node only, unlike the cluster-wide {class}`~aerospike_sdk.aio.info.InfoCommands`.

```{eval-rst}
.. autoclass:: aerospike_sdk.aio.node.Node
   :members:
   :inherited-members:
   :show-inheritance:
```
