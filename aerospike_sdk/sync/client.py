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

"""Synchronous façade over :class:`~aerospike_sdk.aio.client.Client`.

Each public call runs the corresponding async API on a private event loop for
the current thread. Prefer :class:`~aerospike_sdk.aio.client.Client` in
async code for lower overhead and clearer concurrency.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import types
from typing import TYPE_CHECKING, Any, List, Optional, Union, overload

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.sync.operations.index import SyncIndexBuilder
    from aerospike_sdk.sync.operations.query import SyncQueryBuilder
    from aerospike_sdk.sync.session import SyncSession
    from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

from aerospike_async import (
    AdminPolicy,
    ClientPolicy,
    Key,
    RegisterTask,
    UDFLang,
    UdfRemoveTask,
)
from aerospike_async.exceptions import ConnectionError as AerospikeConnectionError

from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior

# Set up debug logging
log = logging.getLogger(__name__)
# Enable debug output via environment variable or set to True directly
DEBUG_SYNC_CLIENT = os.environ.get("DEBUG_SYNC_CLIENT", "false").lower() == "true"
if DEBUG_SYNC_CLIENT:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Thread-local storage for event loop managers
_thread_local = threading.local()


class _EventLoopManager:
    """Manages an event loop for sync operations in a thread-safe way."""

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = threading.Lock()
        self._thread_id: Optional[int] = None
        self._refcount = 0  # Track how many clients are using this manager
    
    def __del__(self) -> None:
        """Ensure event loop is closed when manager is destroyed."""
        try:
            self.close()
        except Exception:
            pass
    
    def acquire(self) -> None:
        """Acquire a reference to this manager."""
        with self._lock:
            self._refcount += 1
    
    def release(self) -> None:
        """Release a reference to this manager. Closes the loop if no more references to free file descriptors."""
        loop_to_close = None
        with self._lock:
            self._refcount -= 1
            if self._refcount <= 0:
                # Close the loop when no more clients are using it to free file descriptors
                # This is critical to prevent "too many open files" errors
                if self._loop is not None:
                    loop_to_close = self._loop
                    self._loop = None
                    self._thread_id = None
                    if DEBUG_SYNC_CLIENT:
                        log.debug(f"[release] Thread {threading.get_ident()}: Closing loop (refcount=0) to free file descriptors")
            else:
                if DEBUG_SYNC_CLIENT:
                    log.debug(f"[release] Thread {threading.get_ident()}: Released reference (refcount={self._refcount})")
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            log.warning(f"[release] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                sel = getattr(loop_to_close, "_selector", None)
                                if sel is not None:
                                    sel.close()  # type: ignore[attr-defined]
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    log.debug(f"[release] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    log.warning(f"[release] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                log.warning(f"[release] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    log.warning(f"[release] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")
    
    def _reset_loop(self) -> None:
        """Reset the loop if it's in a bad state. Creates a new loop for the next use."""
        loop_to_close = None
        with self._lock:
            if self._loop is not None:
                loop_to_close = self._loop
                self._loop = None
                self._thread_id = None
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            log.warning(f"[_reset_loop] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                sel = getattr(loop_to_close, "_selector", None)
                                if sel is not None:
                                    sel.close()  # type: ignore[attr-defined]
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    log.debug(f"[_reset_loop] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    log.warning(f"[_reset_loop] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                log.warning(f"[_reset_loop] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    log.warning(f"[_reset_loop] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create an event loop for the current thread."""
        current_thread_id = threading.get_ident()
        if DEBUG_SYNC_CLIENT:
            log.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Starting, stored_thread_id={self._thread_id}, has_loop={self._loop is not None}")

        with self._lock:
            # If we're in the same thread and have a loop, reuse it
            if self._thread_id == current_thread_id and self._loop is not None:
                try:
                    # Check if loop is still valid and can be used
                    if self._loop.is_closed():
                        # Loop is closed, create a new one
                        if DEBUG_SYNC_CLIENT:
                            log.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop is closed, creating new one")
                        self._loop = None
                    else:
                        # Try to verify the loop is actually usable
                        # Check if it's running (can't use a running loop)
                        if self._loop.is_running():
                            if DEBUG_SYNC_CLIENT:
                                log.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop is running, creating new one")
                            self._loop = None
                        else:
                            # Try to verify the loop is actually usable
                            # by checking if we can get its default executor
                            try:
                                _ = self._loop._default_executor  # type: ignore[attr-defined]
                                if DEBUG_SYNC_CLIENT:
                                    log.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Reusing existing loop")
                                return self._loop
                            except (RuntimeError, AttributeError) as e:
                                # Loop is in a bad state, create a new one
                                if DEBUG_SYNC_CLIENT:
                                    log.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop validation failed: {e}, creating new one")
                                self._loop = None
                except (RuntimeError, AttributeError) as e:
                    # Loop is invalid, create a new one
                    if DEBUG_SYNC_CLIENT:
                        log.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop check exception: {e}, creating new one")
                    self._loop = None

            # Create a new loop
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Creating new loop")
            self._loop = self._create_new_loop()
            self._thread_id = current_thread_id
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Created new loop: {self._loop}, closed: {self._loop.is_closed()}")
            return self._loop

    @staticmethod
    def _create_new_loop() -> asyncio.AbstractEventLoop:
        """Create a new event loop."""
        if DEBUG_SYNC_CLIENT:
            log.debug(f"[_create_new_loop] Thread {threading.get_ident()}: Starting")
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, we can't use the sync client
            # The sync client manages its own loop and cannot coexist with a running loop
            if DEBUG_SYNC_CLIENT:
                log.error(f"[_create_new_loop] Thread {threading.get_ident()}: Found running loop: {loop}")
            raise RuntimeError(
                "Cannot use SyncClient from within an async context. "
                "Use Client (async) instead, or ensure you're not in an async context."
            )
        except RuntimeError as e:
            # Check if this is our error or a "no running loop" error
            if "Cannot use SyncClient" in str(e):
                raise
            # No running loop, create a new one
            # Don't use get_event_loop() as it may create a loop we don't manage
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[_create_new_loop] Thread {threading.get_ident()}: No running loop ({e}), creating new one")
            loop = asyncio.new_event_loop()
            # Don't set it as the default loop - we'll manage it ourselves
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[_create_new_loop] Thread {threading.get_ident()}: Created new loop: {loop}")
            return loop

    def run_async(self, coro) -> Any:
        """Run an async coroutine synchronously.

        Fast path: when the loop is already established for the current
        thread, skip all defensive checks and go straight to
        ``run_until_complete``.  The slow path handles first-call setup,
        thread migration, and error recovery.
        """
        # ---- fast path: reuse an established loop ----
        loop = self._loop
        if loop is not None and self._thread_id == threading.get_ident():
            try:
                return loop.run_until_complete(coro)
            except StopAsyncIteration:
                raise
            except RuntimeError as runtime_err:
                error_msg = str(runtime_err).lower()
                if "no running event loop" not in error_msg and "this event loop is already running" not in error_msg:
                    raise
                # Fall through to slow path for recovery.
            except AerospikeConnectionError:
                self._reset_loop()
                raise
            except Exception:
                if loop.is_closed():
                    with self._lock:
                        if self._loop is loop:
                            self._loop = None
                            self._thread_id = None
                raise

        # ---- slow path: first call or recovery ----
        return self._run_async_slow(coro)

    def _run_async_slow(self, coro) -> Any:
        """Slow path for :meth:`run_async` — setup and error recovery."""
        if DEBUG_SYNC_CLIENT:
            log.debug(f"[run_async] Thread {threading.get_ident()}: Slow path")
        loop_was_reused = self._loop is not None and self._thread_id == threading.get_ident()
        loop = self._get_or_create_loop()
        loop_is_fresh = not loop_was_reused
        try:
            if loop.is_running():
                self._reset_loop()
                loop = self._get_or_create_loop()
                loop_is_fresh = True

            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(coro)
            return result
        except RuntimeError as runtime_err:
            error_msg = str(runtime_err).lower()
            if "no running event loop" in error_msg or "this event loop is already running" in error_msg:
                self._reset_loop()
                loop = self._get_or_create_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(coro)
            raise
        except StopAsyncIteration:
            raise
        except AerospikeConnectionError:
            if not loop_is_fresh:
                self._reset_loop()
            else:
                with self._lock:
                    self._loop = None
                    self._thread_id = None
            raise
        except Exception:
            try:
                if loop.is_closed():
                    with self._lock:
                        if self._loop is loop:
                            self._loop = None
                            self._thread_id = None
            except (RuntimeError, AttributeError):
                with self._lock:
                    if self._loop is loop:
                        self._loop = None
                        self._thread_id = None
            raise

    def close(self) -> None:
        """Close the event loop if we own it."""
        loop_to_close = None
        with self._lock:
            if self._loop is not None:
                loop_to_close = self._loop
                self._loop = None
                self._thread_id = None
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            log.warning(f"[close] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                sel = getattr(loop_to_close, "_selector", None)
                                if sel is not None:
                                    sel.close()  # type: ignore[attr-defined]
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    log.debug(f"[close] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    log.warning(f"[close] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                log.warning(f"[close] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    log.warning(f"[close] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")


class SyncClient:
    """Connect to Aerospike and run the SDK API without ``async``/``await``.

    Method shapes match :class:`~aerospike_sdk.aio.client.Client`; return
    types are synchronous counterparts (for example
    :class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder`,
    :class:`~aerospike_sdk.sync.session.SyncSession`).

    Example::

            with SyncClient("localhost:3000") as client:
                for row in client.query(
                    namespace="test",
                    set_name="users"
                ).execute():
                    if row.record:
                        print(row.record.bins)

    Raises:
        RuntimeError: If constructed or used while an asyncio event loop is
            already running in the thread (use :class:`~aerospike_sdk.aio.client.Client` instead).

    See Also:
        :class:`~aerospike_sdk.aio.client.Client`: Native async client.
        :meth:`create_session`: Session-scoped :class:`~aerospike_sdk.policy.behavior.Behavior`.
    """

    def __init__(
        self,
        seeds: str,
        policy: Optional[ClientPolicy] = None,
    ) -> None:
        """
        Initialize a SyncClient.

        Args:
            seeds: Aerospike cluster seed addresses (e.g., "localhost:3000")
            policy: Optional client policy. If None, a default policy is used.
        """
        self._seeds = seeds
        self._policy = policy
        self._async_client: Optional[Client] = None
        # Reuse event loop manager per thread to avoid creating too many loops
        if not hasattr(_thread_local, 'loop_manager'):
            _thread_local.loop_manager = _EventLoopManager()
        self._loop_manager = _thread_local.loop_manager
        self._loop_manager.acquire()
        self._connected = False

    def connect(self) -> None:
        """Connect to the Aerospike cluster synchronously."""
        if DEBUG_SYNC_CLIENT:
            log.debug(f"[connect] Thread {threading.get_ident()}: Starting, connected={self._connected}")
        if self._connected and self._async_client is not None:
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[connect] Thread {threading.get_ident()}: Already connected, returning")
            return

        async def _connect():
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[_connect] Thread {threading.get_ident()}: Creating Client for {self._seeds}")
            client = Client(self._seeds, self._policy)
            try:
                await client.connect()
                if DEBUG_SYNC_CLIENT:
                    log.debug(f"[_connect] Thread {threading.get_ident()}: Client connected")
                return client
            except Exception:
                # If connection fails, ensure the client is closed to prevent resource leaks
                try:
                    await client.close()
                except Exception:
                    pass
                raise

        try:
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[connect] Thread {threading.get_ident()}: Calling run_async to connect")
            self._async_client = self._loop_manager.run_async(_connect())
            self._connected = True
            if DEBUG_SYNC_CLIENT:
                log.debug(f"[connect] Thread {threading.get_ident()}: Connection successful")
        except Exception as e:
            # If connection fails, ensure we're in a clean state
            self._connected = False
            if DEBUG_SYNC_CLIENT:
                log.error(f"[connect] Thread {threading.get_ident()}: Connection failed: {type(e).__name__}: {e}")
            # Don't reset the loop on connection errors - they can happen for many reasons
            # The loop reset in run_async() will handle bad loop states
            raise

    def close(self) -> None:
        """Close the connection to the Aerospike cluster."""
        if self._async_client is not None:
            async def _close():
                await self._async_client.close()

            self._loop_manager.run_async(_close())
            self._async_client = None
            self._connected = False

        self._loop_manager.close()

    def __enter__(self) -> SyncClient:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Context manager exit."""
        try:
            self.close()
        except Exception:
            # Ensure we always release the reference, even if close fails
            if hasattr(self, '_loop_manager'):
                self._loop_manager.release()
            raise

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    @property
    def supports_server_compiled_ael(self) -> bool:
        """Same as :attr:`aerospike_sdk.aio.client.Client.supports_server_compiled_ael`.

        Value is the connect-time snapshot on the underlying async client (PAC
        ``Version.supports_server_compiled_ael`` aggregate plus PAC API checks).
        """
        if not self._connected or self._async_client is None:
            return False
        return self._async_client.supports_server_compiled_ael

    def _ensure_connected(self) -> Client:
        """Ensure the client is connected and return the async client."""
        if not self._connected or self._async_client is None:
            self.connect()
        assert self._async_client is not None
        return self._async_client

    @overload
    def query(
        self,
        dataset: DataSet,
        *,
        behavior: Optional[Behavior] = None,
    ):
        """Create a query builder from a DataSet."""
        ...

    @overload
    def query(
        self,
        key: Key,
        *,
        behavior: Optional[Behavior] = None,
    ):
        """Create a query builder for a single Key (point read)."""
        ...

    @overload
    def query(
        self,
        keys: List[Key],
        *,
        behavior: Optional[Behavior] = None,
    ):
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @overload
    def query(
        self,
        *keys: Key,
        behavior: Optional[Behavior] = None,
    ):
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @overload
    def query(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ):
        """Create a query builder with explicit namespace/set."""
        ...

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[str] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
        behavior: Optional[Behavior] = None,
    ) -> SyncQueryBuilder:
        """
        Create a query builder (synchronous).

        Supports the same calling styles as :meth:`Client.query
        <aerospike_sdk.aio.client.Client.query>`, including multi-key
        varargs. Returns a wrapper that executes queries synchronously.

        Args:
            arg1: Optional first positional (DataSet, key, list of keys, or
                namespace string when paired with ``arg2`` as set name).
            arg2: When ``arg1`` is a namespace, the set name; otherwise may be
                a second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional argument is a key.
            namespace: Keyword namespace (with ``set_name``).
            set_name: Keyword set name (with ``namespace``).
            dataset: Keyword :class:`~aerospike_sdk.dataset.DataSet`.
            key: Keyword single key.
            keys_list: Keyword list of keys for batch read.
            behavior: Optional :class:`~aerospike_sdk.policy.behavior.Behavior`
                for this builder (same semantics as ``Client.query``).

        Returns:
            SyncQueryBuilder: Configured for the requested namespace, set,
                and/or keys.
        """
        from aerospike_sdk.sync.operations.query import SyncQueryBuilder

        async_client = self._ensure_connected()
        b = behavior

        def _wrap(builder):
            return SyncQueryBuilder(
                async_client=async_client,
                namespace=builder._namespace,
                set_name=builder._set_name,
                loop_manager=self._loop_manager,
                query_builder=builder,
            )

        if arg1 is not None:
            if isinstance(arg1, DataSet):
                return _wrap(async_client.query(dataset=arg1, behavior=b))
            if isinstance(arg1, Key):
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                else:
                    return _wrap(async_client.query(key=arg1, behavior=b))
                return _wrap(async_client.query(keys=all_keys, behavior=b))
            if isinstance(arg1, list):
                if len(arg1) == 0:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(
                        f"Expected List[Key], but first element is {type(arg1[0])}"
                    )
                return _wrap(async_client.query(keys=arg1, behavior=b))
            if isinstance(arg1, str) and arg2 is not None:
                return _wrap(
                    async_client.query(namespace=arg1, set_name=arg2, behavior=b)
                )

        if keys:
            keys_coll = list(keys)
            if arg1 is not None and isinstance(arg1, Key):
                keys_coll.insert(0, arg1)
            if arg2 is not None and isinstance(arg2, Key):
                keys_coll.insert(
                    1 if arg1 is not None and isinstance(arg1, Key) else 0, arg2
                )
            return _wrap(async_client.query(keys=keys_coll, behavior=b))

        return _wrap(
            async_client.query(  # type: ignore[call-overload]
                namespace=namespace,
                set_name=set_name,
                dataset=dataset,
                key=key,
                keys=keys_list,
                behavior=b,
            )
        )

    @overload
    def index(
        self,
        *,
        dataset: DataSet,
        behavior: Optional[Behavior] = None,
    ):
        """Create an index builder from a DataSet."""
        ...

    @overload
    def index(
        self,
        namespace: str,
        set_name: str,
        *,
        behavior: Optional[Behavior] = None,
    ):
        """Create an index builder with explicit namespace/set."""
        ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
        behavior: Optional[Behavior] = None,
    ) -> SyncIndexBuilder:
        """
        Create a secondary-index builder (synchronous).

        Same arguments as :meth:`~aerospike_sdk.aio.client.Client.index`.
        ``create()`` / ``drop()`` run on the client's event loop.

        Returns:
            SyncIndexBuilder: Configured for the requested namespace and set.

        See Also:
            :class:`~aerospike_sdk.aio.operations.index.IndexBuilder`: Async
            implementation.
        """
        from aerospike_sdk.sync.operations.index import SyncIndexBuilder

        # Delegate to async client to handle argument parsing
        async_client = self._ensure_connected()
        builder = async_client.index(  # type: ignore[call-overload]
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
            behavior=behavior,
        )

        return SyncIndexBuilder(
            async_client=async_client,
            namespace=builder._namespace,
            set_name=builder._set_name,
            loop_manager=self._loop_manager,
        )

    def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set (synchronous).

        This method deletes all records in the specified set.
        This operation cannot be undone.

        Args:
            dataset: The DataSet to truncate.
            before_nanos: Optional timestamp in nanoseconds. Only records with
                         last update time (LUT) less than this value will be
                         truncated. If None, all records in the set are truncated.

        Example::

                users = DataSet.of("test", "users")
                client.truncate(users)

                # Truncate only records older than a specific time
                import time
                cutoff_time = time.time_ns() - (24 * 60 * 60 * 10**9)  # 24 hours ago
                client.truncate(users, before_nanos=cutoff_time)
        """
        async_client = self._ensure_connected()

        # Access the underlying async client and call its truncate method
        if async_client._client is None:
            raise RuntimeError("Client is not connected")

        async def _truncate():
            await async_client._client.truncate(
                dataset.namespace,
                dataset.set_name,
                before_nanos
            )

        self._loop_manager.run_async(_truncate())

    def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF module from bytes (synchronous)."""
        async_client = self._ensure_connected()

        async def _reg():
            return await async_client.register_udf(
                body, server_path, language, policy=policy)

        return self._loop_manager.run_async(_reg())

    def register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> RegisterTask:
        """Register a UDF module from a local file (synchronous)."""
        async_client = self._ensure_connected()

        async def _reg():
            return await async_client.register_udf_from_file(
                client_path, server_path, language, policy=policy)

        return self._loop_manager.run_async(_reg())

    def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional[AdminPolicy] = None,
    ) -> UdfRemoveTask:
        """Remove a UDF module from the cluster (synchronous)."""
        async_client = self._ensure_connected()

        async def _rm():
            return await async_client.remove_udf(server_path, policy=policy)

        return self._loop_manager.run_async(_rm())

    def create_session(self, behavior: Optional[Behavior] = None) -> "SyncSession":
        """
        Create a session with the specified behavior (synchronous).

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: The behavior configuration for the session.
                     If None, uses Behavior.DEFAULT.

        Returns:
            A :class:`~aerospike_sdk.sync.session.SyncSession` sharing this
            client's event-loop manager and applying ``behavior`` to operations.

        Example::

                session = client.create_session()
                from datetime import timedelta
                fast = Behavior.DEFAULT.derive_with_changes(
                    name="fast",
                    total_timeout=timedelta(seconds=5),
                )
                session = client.create_session(fast)

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.create_session`:
                Async equivalent.
        """
        from aerospike_sdk.sync.session import SyncSession

        async_client = self._ensure_connected()
        async_session = async_client.create_session(behavior)
        return SyncSession(async_session, self._loop_manager)

    def create_transactional_session(
        self, behavior: Optional[Behavior] = None,
    ) -> "SyncTransactionalSession":
        """Create a synchronous multi-record transaction (MRT) session.

        Allocates a fresh :class:`~aerospike_async.Txn` on entry. Operations
        chained off the returned session (``tx.upsert(...)``, ``tx.query(...)``,
        ``tx.batch()``, ...) auto-participate in the transaction — every
        builder stamps ``policy.txn = tx.txn`` under the hood. On clean exit
        the transaction is committed; if an exception propagates out of the
        ``with`` block it is aborted.

        Multi-record transactions require an Aerospike server running in
        strong-consistency (SC) mode on the target namespace.

        Args:
            behavior: Optional :class:`~aerospike_sdk.policy.behavior.Behavior`
                for operations inside the transaction. Defaults to
                :attr:`Behavior.DEFAULT` when omitted.

        Returns:
            A :class:`~aerospike_sdk.sync.transactional_session.SyncTransactionalSession`
            bound to this client and behavior.

        Example::

            with client.create_transactional_session() as tx:
                tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
                tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

        See Also:
            :meth:`~aerospike_sdk.aio.client.Client.transaction_session`:
                Async equivalent.
        """
        from aerospike_sdk.sync.transactional_session import SyncTransactionalSession

        async_client = self._ensure_connected()
        async_txn_session = async_client.transaction_session(behavior)
        return SyncTransactionalSession(async_txn_session, self._loop_manager)

