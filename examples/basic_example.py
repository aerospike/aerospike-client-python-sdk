#!/usr/bin/env python3
"""Basic example demonstrating key-value operations via the SDK API.

Covers: connect, put, get, get with bin selection, exists, delete.
"""

from _env import Example


class BasicExample(Example):
    async def run(self):
        # PUT
        await self.session.upsert(self.key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # GET
        stream = await self.session.query(self.key).execute()
        first = await stream.first_or_raise()
        print(f"Got record: {first.record.bins}")

        # GET with selected bins
        stream = await self.session.query(self.key).bins(["name"]).execute()
        first = await stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")

        # EXISTS
        stream = await self.session.exists(self.key).execute()
        first = await stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # DELETE
        await self.session.delete(self.key).execute()
        print("Deleted record")

        print("\nAll operations completed successfully!")
