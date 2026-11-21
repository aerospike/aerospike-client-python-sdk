# Policy Enums

The values of the enum-typed fields of
{class}`~aerospike_sdk.policy.behavior_settings.Settings`. Import them from
`aerospike_sdk.policy` (or `aerospike_sdk`) and pass a member:

```python
from aerospike_sdk import Behavior
from aerospike_sdk.policy import ReadModeAP, Replica, Settings

distributed_reads = Behavior.DEFAULT.derive_with_changes(
    "distributed-reads",
    reads=Settings(replica=Replica.MASTER_PROLES),
    reads_ap=Settings(read_mode_ap=ReadModeAP.ALL),
)
```

| Enum | `Settings` field | Applies to |
|------|------------------|------------|
| `Replica` | `replica` | which replica an operation is sent to |
| `ReadModeAP` | `read_mode_ap` | reads on AP (availability) namespaces |
| `ReadModeSC` | `read_mode_sc` | reads on SC (strong-consistency) namespaces |
| `CommitLevel` | `commit_level` | writes on AP namespaces |

A string name is not accepted in code: `Settings(replica="MASTER")` raises
`TypeError`. Names are accepted only in the
[SDK config file](../guide/dynamic-sdk-config.md).

```{eval-rst}
.. py:class:: aerospike_sdk.policy.Replica

   Which node an operation is sent to. The value of ``Settings.replica``.

   .. py:attribute:: SEQUENCE

      Try the node holding the master partition first. If the connection
      fails, try the nodes holding replicas. On a socket timeout a read also
      moves to a replica; a write stays on the master. The default.

   .. py:attribute:: MASTER

      Use the node holding the key's master partition.

   .. py:attribute:: MASTER_PROLES

      Spread reads round-robin across the nodes holding the key's master and
      replica partitions. Writes always go to the master.

   .. py:attribute:: PREFER_RACK

      Try a node on the client's own rack first; on a timeout, or when the
      rack has no node, behave as ``SEQUENCE``. Needs the client's rack ids
      and the server's rack configuration set.

   .. py:attribute:: RANDOM

      Spread reads round-robin across every node in the cluster. Useful when
      the replication factor equals the node count. Writes always go to the
      master.

.. py:class:: aerospike_sdk.policy.ReadModeAP

   How many replicas a read consults on an AP (availability) namespace. The
   value of ``Settings.read_mode_ap``.

   .. py:attribute:: ONE

      Involve a single node in the read. The default.

   .. py:attribute:: ALL

      Consult every duplicate of the record before answering.

.. py:class:: aerospike_sdk.policy.ReadModeSC

   The consistency a read asks for on an SC (strong-consistency) namespace.
   The value of ``Settings.read_mode_sc``.

   .. py:attribute:: SESSION

      This client sees only an increasing sequence of record versions. Reads
      go to the master. The default.

   .. py:attribute:: LINEARIZE

      Every client sees only an increasing sequence of record versions. Reads
      go to the master.

   .. py:attribute:: ALLOW_REPLICA

      Read from the master or any full (non-migrating) replica. An increasing
      sequence of record versions is not guaranteed.

   .. py:attribute:: ALLOW_UNAVAILABLE

      As ``ALLOW_REPLICA``, and also read from unavailable partitions. An
      increasing sequence of record versions is not guaranteed.

.. py:class:: aerospike_sdk.policy.CommitLevel

   How many replicas must commit a write before the server answers, on an AP
   namespace. The value of ``Settings.commit_level``.

   .. py:attribute:: COMMIT_ALL

      Wait until the master and every replica have committed. The default.

   .. py:attribute:: COMMIT_MASTER

      Wait until the master alone has committed.
```
