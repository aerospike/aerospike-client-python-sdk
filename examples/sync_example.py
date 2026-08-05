#!/usr/bin/env python3
"""Example demonstrating the synchronous SDK API.

Covers: sync ClusterDefinition connection, put, get, exists, delete — no async/await.
"""

from _env import SyncExample


class SyncBasicExample(SyncExample):
    def run(self):
        # PUT
        self.session.upsert(self.key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # GET
        stream = self.session.query(self.key).execute()
        first = stream.first_or_raise()
        print(f"Got record: {first.record.bins}")

        # GET with selected bins
        stream = self.session.query(self.key).bins(["name"]).execute()
        first = stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")

        # EXISTS
        stream = self.session.exists(self.key).execute()
        first = stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # DELETE
        self.session.delete(self.key).execute()
        print("Deleted record")

        print("\nAll operations completed successfully!")
