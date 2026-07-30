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

"""DataSet - A SDK API for creating Aerospike keys for a namespace and set."""

from __future__ import annotations

from typing import List, Union, overload

from aerospike_async import Key


class DataSet:
    """Pair a namespace and set name for building :class:`~aerospike_async.Key` values.

    Use :meth:`of` as the factory, then :meth:`id` for one key or :meth:`ids` for
    many. Pass keys to :meth:`~aerospike_sdk.aio.session.Session.query`,
    :meth:`~aerospike_sdk.aio.session.Session.upsert`, and other session APIs.

    Example::

        users = DataSet.of("test", "users")
        k = users.id("user-123")
        ks = users.ids("a", "b", "c")

    See Also:
        :class:`~aerospike_sdk.aio.session.Session`: Operations on keys.
    """

    def __init__(self, namespace: str, set_name: str) -> None:
        """
        Create a DataSet instance.

        Args:
            namespace: The Aerospike namespace
            set_name: The set name within the namespace

        Raises:
            ValueError: If namespace or set_name is empty
        """
        if not namespace:
            raise ValueError("namespace cannot be empty")
        if not set_name:
            raise ValueError("set_name cannot be empty")

        self._namespace = namespace
        self._set_name = set_name

    @staticmethod
    def of(namespace: str, set_name: str) -> DataSet:
        """Construct a dataset handle for ``namespace`` and ``set_name``.

        Args:
            namespace: Non-empty Aerospike namespace name.
            set_name: Non-empty set name within that namespace.

        Returns:
            A :class:`DataSet` sharing the same equality/hash for the pair.

        Raises:
            ValueError: If either string is empty.

        Example::

            orders = DataSet.of("prod", "orders")

        See Also:
            :meth:`id`: Build a single user key.
            :meth:`ids`: Build several keys at once.
        """
        return DataSet(namespace, set_name)

    @property
    def namespace(self) -> str:
        """Get the namespace of this dataset."""
        return self._namespace

    @property
    def set_name(self) -> str:
        """Get the set name of this dataset."""
        return self._set_name

    def id(self, identifier: Union[str, int, bytes]) -> Key:
        """Build one :class:`~aerospike_async.Key` for this namespace and set.

        Args:
            identifier: User key value (string, integer, or bytes).

        Returns:
            A :class:`~aerospike_async.Key` bound to this dataset's namespace/set.

        Example::

            users = DataSet.of("test", "users")
            key_str = users.id("user123")
            key_int = users.id(12345)
            key_bin = users.id(b"pk")

        See Also:
            :meth:`ids`: Multiple keys in one call.
        """
        return Key(self._namespace, self._set_name, identifier)

    def id_from_digest(self, digest: Union[str, bytes]) -> Key:
        """
        Create an Aerospike key from a digest.

        Args:
            digest: The digest as a hex string or bytes

        Returns:
            A new Key instance created from the digest

        Example::

                users = DataSet.of("test", "users")
                # From hex string
                key = users.id_from_digest("a1b2c3d4...")
                # From bytes
                key = users.id_from_digest(b"\\xa1\\xb2\\xc3...")
        """
        return Key.key_with_digest(self._namespace, self._set_name, digest)

    def id_for_object(self, obj: object) -> Key:
        """
        Create an Aerospike key from an object identifier.
        Supports String, Integer, Long, and byte array types.

        Args:
            obj: The object to use as the key identifier

        Returns:
            A new Key instance

        Raises:
            TypeError: If the object type is not supported

        Example::

                users = DataSet.of("test", "users")
                key1 = users.id_for_object("user123")     # String key
                key2 = users.id_for_object(123)           # Integer key
                key3 = users.id_for_object(12345)         # Integer key
                key4 = users.id_for_object(b"bytes")      # Bytes key
        """
        if isinstance(obj, str):
            return self.id(obj)
        elif isinstance(obj, (int, float)):
            # Convert float to int if it's a whole number
            if isinstance(obj, float) and obj.is_integer():
                return self.id(int(obj))
            elif isinstance(obj, int):
                return self.id(obj)
            else:
                raise TypeError(f"Cannot use float {obj} as key identifier")
        elif isinstance(obj, bytes):
            return self.id(obj)
        elif isinstance(obj, bytearray):
            return self.id(bytes(obj))
        else:
            raise TypeError(
                f"Cannot construct a key for object of type {type(obj).__name__}. "
                "Only str, int, and bytes are supported."
            )

    @overload
    def ids(self, *identifiers: str) -> List[Key]:
        """Create multiple keys from string identifiers."""
        ...

    @overload
    def ids(self, *identifiers: int) -> List[Key]:
        """Create multiple keys from integer identifiers."""
        ...

    @overload
    def ids(self, *identifiers: bytes) -> List[Key]:
        """Create multiple keys from bytes identifiers."""
        ...

    @overload
    def ids(self, identifiers: List[Union[str, int, bytes]]) -> List[Key]:
        """Create multiple keys from a list of identifiers."""
        ...

    def ids(
        self,
        *identifiers: Union[str, int, bytes],
    ) -> List[Key]:
        """Build many keys sharing this dataset's namespace and set.

        Call either with several positional identifiers
        (``ids("a", "b", "c")``) or with a single list
        (``ids(["a", "b", "c"])``). Mixed-type lists use :meth:`id_for_object`
        per element when the single-list form is used.

        Args:
            *identifiers: One or more identifiers, or exactly one list of
                identifiers.

        Returns:
            A list of :class:`~aerospike_async.Key` instances in input order.

        Raises:
            TypeError: If list elements are not supported key types.

        Example::

            users = DataSet.of("test", "users")
            keys1 = users.ids("u1", "u2", "u3")
            keys2 = users.ids(["u1", "u2"])
            keys3 = users.ids(1, 2, 3)

        See Also:
            :meth:`id`: Single-key helper.
        """
        # Handle case where a single list is passed
        if len(identifiers) == 1 and isinstance(identifiers[0], list):
            return [self.id_for_object(id_obj) for id_obj in identifiers[0]]

        # Handle multiple positional arguments
        return [self.id(id_obj) for id_obj in identifiers]

    def ids_from_digests(self, *digests: Union[str, bytes]) -> List[Key]:
        """
        Create multiple Aerospike keys from digests.

        Args:
            *digests: Variable number of digests (hex strings or bytes)

        Returns:
            A list of Key instances created from the digests

        Example::

                users = DataSet.of("test", "users")
                keys = users.ids_from_digests("digest1", "digest2", b"digest3")
        """
        return [self.id_from_digest(digest) for digest in digests]

    def __repr__(self) -> str:
        """Return a string representation of this DataSet."""
        return f"DataSet(namespace={self._namespace!r}, set_name={self._set_name!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality with another DataSet."""
        if not isinstance(other, DataSet):
            return False
        return self._namespace == other._namespace and self._set_name == other._set_name

    def __hash__(self) -> int:
        """Return hash of this DataSet."""
        return hash((self._namespace, self._set_name))

