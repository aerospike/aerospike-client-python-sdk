#!/usr/bin/env python3
"""Example demonstrating the synchronous SDK API.

Covers: sync ClusterDefinition connection, put, get, exists, delete — no async/await.
"""

import _env
from aerospike_sdk import Behavior, DataSet


class SyncExample:
    def __init__(self):
        

    def run(self):
        # PUT
        session.upsert(key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # GET
        stream = session.query(key).execute()
        first = stream.first_or_raise()
        print(f"Got record: {first.record.bins}")

        # GET with selected bins
        stream = session.query(key).bins(["name"]).execute()
        first = stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")

        # EXISTS
        stream = session.exists(key).execute()
        first = stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # DELETE
        session.delete(key).execute()
        print("Deleted record")

        print("\nAll operations completed successfully!")
    finally:
        cluster.close()


if __name__ == "__main__":
    main()
