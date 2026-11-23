# PreparedAel

An AEL template with named placeholders. Passed where an AEL string is
accepted, followed by a mapping of placeholder name to value, it binds each
value as an AEL literal of the value's own type, quoted and escaped.

```python
from aerospike_sdk import PreparedAel

by_name_and_age = PreparedAel("$.name == ?name and $.age > ?min_age")

stream = await (
    session.query(users)
    .where(by_name_and_age, {"name": name, "min_age": min_age})
    .execute()
)
```

See [Binding Values](../guide/expression-ael.md) in the AEL guide for the
literal each Python type binds as.

```{eval-rst}
.. autoclass:: aerospike_sdk.PreparedAel
   :members:
```
