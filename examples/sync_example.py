#!/usr/bin/env python3
"""Example demonstrating the synchronous SDK API.

Covers: sync ClusterDefinition connection, put, get, exists, delete — no async/await.
"""

import _env
from aerospike_sdk import Behavior, DataSet


def main() -> None:
    with _env.sync_connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")
        key = users.id("user123")

        # --- 1) Write a record ---
        session.upsert(key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # --- 2) Read the whole record ---
        stream = session.query(key).execute()
        first = stream.first_or_raise()
        print(f"Got record: {first.record.bins}")

        # --- 3) Read selected bins ---
        stream = session.query(key).bins("name").execute()
        first = stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")

        # --- 4) Check that the record exists ---
        stream = session.exists(key).execute()
        first = stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # --- 5) Delete the record ---
        session.delete(key).execute()
        print("Deleted record")



if __name__ == "__main__":
    main()
