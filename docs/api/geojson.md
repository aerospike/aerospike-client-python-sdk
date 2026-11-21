# GeoJSON

A GeoJSON value. A geospatial bin reads back as a `GeoJSON`, and a `GeoJSON`
is how you place a geospatial value where a whole-bin setter does not reach:
inside a list or a map, or in the dict passed to `.put({...})`. To write one
bin from a GeoJSON string, `set_to_geo_json(...)` is shorter.

```python
from aerospike_sdk import GeoJSON

await session.upsert(places.id("space_needle")).put({
    "name": "Space Needle",
    "loc": GeoJSON('{"type":"Point","coordinates":[-122.349,47.620]}'),
}).execute()
```

See "GeoJSON Bins" in the [writes guide](../guide/writes.md).

```{eval-rst}
.. py:class:: aerospike_sdk.GeoJSON(value)

   A geospatial value, stored with the server's GEOJSON type and so eligible
   for a geospatial index.

   :param value: The GeoJSON text, for example
      ``'{"type":"Point","coordinates":[-122.349,47.620]}'``.

   Two ``GeoJSON`` values are equal when their text is equal, and a
   ``GeoJSON`` also compares equal to that text as a ``str``.

   .. py:attribute:: value
      :type: str

      The GeoJSON text.

   .. py:method:: as_string() -> str

      The value's ``repr``-style form, ``GeoJSON('...')``. ``str(value)``
      gives the bare text.
```
