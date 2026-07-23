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

"""Tests for the DataSet class."""

import pytest
from aerospike_sdk import Key

from aerospike_sdk import DataSet


class TestDataSet:
    """Test DataSet creation and basic properties."""

    def test_create_with_of(self):
        """Test creating a DataSet using the of() factory method."""
        dataset = DataSet.of("test", "users")
        assert dataset.namespace == "test"
        assert dataset.set_name == "users"

    def test_create_with_constructor(self):
        """Test creating a DataSet using the constructor."""
        dataset = DataSet("test", "users")
        assert dataset.namespace == "test"
        assert dataset.set_name == "users"

    def test_empty_namespace_raises_error(self):
        """Test that empty namespace raises ValueError."""
        with pytest.raises(ValueError, match="namespace cannot be empty"):
            DataSet.of("", "users")

    def test_empty_set_name_raises_error(self):
        """Test that empty set_name raises ValueError."""
        with pytest.raises(ValueError, match="set_name cannot be empty"):
            DataSet.of("test", "")

    def test_equality(self):
        """Test DataSet equality comparison."""
        dataset1 = DataSet.of("test", "users")
        dataset2 = DataSet.of("test", "users")
        dataset3 = DataSet.of("test", "customers")

        assert dataset1 == dataset2
        assert dataset1 != dataset3

    def test_hash(self):
        """Test DataSet hashing."""
        dataset1 = DataSet.of("test", "users")
        dataset2 = DataSet.of("test", "users")
        dataset3 = DataSet.of("test", "customers")

        assert hash(dataset1) == hash(dataset2)
        assert hash(dataset1) != hash(dataset3)

    def test_repr(self):
        """Test DataSet string representation."""
        dataset = DataSet.of("test", "users")
        repr_str = repr(dataset)
        assert "DataSet" in repr_str
        assert "test" in repr_str
        assert "users" in repr_str


class TestDataSetId:
    """Test DataSet.id() method."""

    def test_id_with_string(self):
        """Test creating a key with a string identifier."""
        dataset = DataSet.of("test", "users")
        key = dataset.id("user123")

        assert isinstance(key, Key)
        assert key.namespace == "test"
        assert key.set_name == "users"
        assert key.value == "user123"

    def test_id_with_integer(self):
        """Test creating a key with an integer identifier."""
        dataset = DataSet.of("test", "users")
        key = dataset.id(12345)

        assert isinstance(key, Key)
        assert key.namespace == "test"
        assert key.set_name == "users"
        # Integer keys are preserved as integers (core client supports integer keys)
        assert key.value == 12345
        assert isinstance(key.value, int)

    def test_id_with_bytes(self):
        """Test creating a key with a bytes identifier."""
        dataset = DataSet.of("test", "users")
        key = dataset.id(b"bytes_key")

        assert isinstance(key, Key)
        assert key.namespace == "test"
        assert key.set_name == "users"
        assert key.value == b"bytes_key"


class TestDataSetIdForObject:
    """Test DataSet.id_for_object() method."""

    def test_id_for_object_string(self):
        """Test creating a key from a string object."""
        dataset = DataSet.of("test", "users")
        key = dataset.id_for_object("user123")

        assert isinstance(key, Key)
        assert key.value == "user123"

    def test_id_for_object_integer(self):
        """Test creating a key from an integer object."""
        dataset = DataSet.of("test", "users")
        key = dataset.id_for_object(12345)

        assert isinstance(key, Key)
        # Integer keys are preserved as integers (core client supports integer keys)
        assert key.value == 12345
        assert isinstance(key.value, int)

    def test_id_for_object_float_whole_number(self):
        """Test creating a key from a float that's a whole number."""
        dataset = DataSet.of("test", "users")
        key = dataset.id_for_object(123.0)

        assert isinstance(key, Key)
        # Integer keys are preserved as integers (core client supports integer keys)
        assert key.value == 123
        assert isinstance(key.value, int)

    def test_id_for_object_float_raises_error(self):
        """Test that non-whole float raises TypeError."""
        dataset = DataSet.of("test", "users")
        with pytest.raises(TypeError, match="Cannot use float"):
            dataset.id_for_object(123.5)

    def test_id_for_object_bytes(self):
        """Test creating a key from a bytes object."""
        dataset = DataSet.of("test", "users")
        key = dataset.id_for_object(b"bytes_key")

        assert isinstance(key, Key)
        assert key.value == b"bytes_key"

    def test_id_for_object_bytearray(self):
        """Test creating a key from a bytearray object."""
        dataset = DataSet.of("test", "users")
        key = dataset.id_for_object(bytearray(b"bytes_key"))

        assert isinstance(key, Key)
        assert key.value == b"bytes_key"

    def test_id_for_object_unsupported_type(self):
        """Test that unsupported types raise TypeError."""
        dataset = DataSet.of("test", "users")
        with pytest.raises(TypeError, match="Cannot construct a key"):
            dataset.id_for_object(["list", "not", "supported"])


class TestDataSetIds:
    """Test DataSet.ids() method for batch key creation."""

    def test_ids_with_strings(self):
        """Test creating multiple keys from string identifiers."""
        dataset = DataSet.of("test", "users")
        keys = dataset.ids("user1", "user2", "user3")

        assert len(keys) == 3
        assert all(isinstance(key, Key) for key in keys)
        assert keys[0].value == "user1"
        assert keys[1].value == "user2"
        assert keys[2].value == "user3"

    def test_ids_with_integers(self):
        """Test creating multiple keys from integer identifiers."""
        dataset = DataSet.of("test", "users")
        keys = dataset.ids(1, 2, 3, 4, 5)

        assert len(keys) == 5
        assert all(isinstance(key, Key) for key in keys)
        # Integer keys are preserved as integers (core client supports integer keys)
        assert keys[0].value == 1
        assert isinstance(keys[0].value, int)
        assert keys[4].value == 5
        assert isinstance(keys[4].value, int)

    def test_ids_with_bytes(self):
        """Test creating multiple keys from bytes identifiers."""
        dataset = DataSet.of("test", "users")
        keys = dataset.ids(b"key1", b"key2", b"key3")

        assert len(keys) == 3
        assert all(isinstance(key, Key) for key in keys)
        assert keys[0].value == b"key1"

    def test_ids_with_list(self):
        """Test creating multiple keys from a list of identifiers."""
        dataset = DataSet.of("test", "users")
        keys = dataset.ids(["user1", "user2", "user3"])

        assert len(keys) == 3
        assert all(isinstance(key, Key) for key in keys)
        assert keys[0].value == "user1"

    def test_ids_with_mixed_list(self):
        """Test creating multiple keys from a mixed list."""
        dataset = DataSet.of("test", "users")
        keys = dataset.ids(["user1", 2, b"key3"])

        assert len(keys) == 3
        assert keys[0].value == "user1"
        # Integer keys are preserved as integers (core client supports integer keys)
        assert keys[1].value == 2
        assert isinstance(keys[1].value, int)
        assert keys[2].value == b"key3"


class TestDataSetIdFromDigest:
    """Test DataSet.id_from_digest() method."""

    def test_id_from_digest_hex_string(self):
        """Test creating a key from a hex string digest."""
        # First create a key to get its digest
        dataset = DataSet.of("test", "users")
        original_key = dataset.id("user123")
        digest_hex = original_key.digest

        # Create a new key from the digest
        key_from_digest = dataset.id_from_digest(digest_hex)

        assert isinstance(key_from_digest, Key)
        assert key_from_digest == original_key
        assert key_from_digest.namespace == "test"
        assert key_from_digest.set_name == "users"

    def test_id_from_digest_bytes(self):
        """Test creating a key from a bytes digest."""
        # First create a key to get its digest
        dataset = DataSet.of("test", "users")
        original_key = dataset.id("user123")
        digest_hex = original_key.digest
        digest_bytes = bytes.fromhex(digest_hex)

        # Create a new key from the digest
        key_from_digest = dataset.id_from_digest(digest_bytes)

        assert isinstance(key_from_digest, Key)
        assert key_from_digest == original_key


class TestDataSetIdsFromDigests:
    """Test DataSet.ids_from_digests() method."""

    def test_ids_from_digests(self):
        """Test creating multiple keys from digests."""
        dataset = DataSet.of("test", "users")

        # Create original keys to get their digests
        key1 = dataset.id("user1")
        key2 = dataset.id("user2")
        digest1 = key1.digest
        digest2 = key2.digest

        # Create keys from digests
        keys = dataset.ids_from_digests(digest1, digest2)

        assert len(keys) == 2
        assert keys[0] == key1
        assert keys[1] == key2


class TestDataSetUsageExamples:
    """Test common DataSet usage patterns."""

    def test_basic_usage(self):
        """Test basic DataSet usage pattern."""
        users = DataSet.of("test", "users")
        user_key = users.id("user123")
        user_keys = users.ids("user1", "user2", "user3")

        assert isinstance(user_key, Key)
        assert len(user_keys) == 3

    def test_numeric_keys(self):
        """Test creating numeric keys."""
        users = DataSet.of("test", "users")
        key = users.id(123)
        keys = users.ids(1, 2, 3, 4, 5)

        # Integer keys are preserved as integers (core client supports integer keys)
        assert key.value == 123
        assert isinstance(key.value, int)
        assert len(keys) == 5

    def test_object_keys(self):
        """Test creating keys from objects."""
        users = DataSet.of("test", "users")
        key1 = users.id_for_object("user123")
        key2 = users.id_for_object(123)
        key3 = users.id_for_object(12345)
        key4 = users.id_for_object(b"bytes")

        assert key1.value == "user123"
        # Integer keys are preserved as integers (core client supports integer keys)
        assert key2.value == 123
        assert isinstance(key2.value, int)
        assert key3.value == 12345
        assert isinstance(key3.value, int)
        assert key4.value == b"bytes"
