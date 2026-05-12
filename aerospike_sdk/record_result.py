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

"""RecordResult — per-record outcome for batch and query operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aerospike_async import Key, Record
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.exceptions import _result_code_to_exception
from aerospike_sdk.hll_config import HllConfig

if TYPE_CHECKING:  # Not unused — needed for forward-reference type annotations and Sphinx autodoc.
    from aerospike_async import BatchRecord
    from aerospike_sdk.exceptions import AerospikeError


@dataclass(frozen=True, slots=True)
class RecordResult:
    """One row from a batch, query stream, or single-key SDK call.

    Inspect :attr:`is_ok` and :attr:`result_code` for outcome; use
    :meth:`or_raise` or :meth:`record_or_raise` when failures should throw.
    Foreground UDF success values appear in :attr:`udf_result` when returned
    by the server.

    Attributes:
        key: Target :class:`~aerospike_async.Key` for this row.
        record: :class:`~aerospike_async.Record` payload, or ``None`` if not
            returned (errors, not found, or UDF error rows).
        result_code: Server :class:`~aerospike_async.exceptions.ResultCode`.
        in_doubt: ``True`` when a write may have completed despite an error.
        index: Batch position, or ``0`` / ``-1`` depending on origin.
        exception: Embedded :class:`~aerospike_sdk.exceptions.AerospikeError`
            when the client placed an error in-stream instead of raising.
        udf_result: Lua return value for successful foreground UDF calls.

    Example:
        Inspect a row from a stream::

            row = await stream.first()
            if row and row.is_ok:
                bins = row.record.bins if row.record else {}
            elif row:
                row.or_raise()

    See Also:
        :class:`~aerospike_sdk.record_stream.RecordStream`: Async iteration
        of results.
    """

    key: Key
    record: Record | None
    result_code: ResultCode
    in_doubt: bool = False
    index: int = -1
    exception: AerospikeError | None = None
    udf_result: Any | None = None

    @property
    def is_ok(self) -> bool:
        """Whether :attr:`result_code` is ``ResultCode.OK``.

        Returns:
            ``True`` on success; ``False`` for any other result code.

        Example::

            row = await stream.first()
            if row is not None and row.is_ok and row.record:
                bins = row.record.bins
        """
        return self.result_code == ResultCode.OK

    def or_raise(self) -> RecordResult:
        """Return ``self`` if successful, else raise from :attr:`exception` or result code.

        Returns:
            This instance when :attr:`is_ok` is true.

        Raises:
            AerospikeError: If :attr:`exception` is set (embedded client error).
            AerospikeError: Otherwise, from :attr:`result_code` via
                :func:`~aerospike_sdk.exceptions._result_code_to_exception`
                (usually a specific subclass; unmapped codes use the base type).

        Example::

            row = await stream.first()
            if row is not None:
                row.or_raise()
        """
        if not self.is_ok:
            if self.exception is not None:
                raise self.exception
            raise _result_code_to_exception(
                self.result_code, str(self.result_code), self.in_doubt
            )
        return self

    def record_or_raise(self) -> Record:
        """Return :attr:`record`, raising if the result is not OK.

        Returns:
            The non-``None`` :class:`~aerospike_async.Record`.

        Raises:
            Same as :meth:`or_raise`, plus ``ValueError`` if the result is OK
            but :attr:`record` is ``None`` (unexpected empty payload).

        Example:
            rec = (await stream.first_or_raise()).record_or_raise()
        """
        self.or_raise()
        if self.record is None:
            raise ValueError("Record is None despite ResultCode.OK")
        return self.record

    def get_hll_config(self, bin_name: str) -> HllConfig | None:
        """Return the HLL bin's :class:`~aerospike_sdk.HllConfig` from a ``hll_describe()`` result.

        Wraps the two-element ``[index_bit_count, min_hash_bit_count]`` list
        that ``hll_describe()`` writes back into the bin and returns it as an
        :class:`HllConfig`. Returns ``None`` if the bin is absent from the
        record (or the record itself is ``None``).

        Args:
            bin_name: Name of the bin holding a ``hll_describe()`` result.

        Returns:
            An :class:`HllConfig`, or ``None`` if the bin is absent.

        Raises:
            TypeError: If the bin value is not a 2-element list of ints.

        Example::

            result = await (
                session.update(key).bin("h").hll_describe().execute()
            ).first_or_raise()
            cfg = result.get_hll_config("h")
            assert cfg.index_bit_count == 14
        """
        if self.record is None:
            return None
        value = self.record.bins.get(bin_name)
        if value is None:
            return None
        if not isinstance(value, list) or len(value) != 2:
            raise TypeError(
                f"Bin {bin_name!r} is not a 2-element list "
                f"(got {type(value).__name__})",
            )
        return HllConfig(int(value[0]), int(value[1]))

    def as_bool(self) -> bool:
        """Interpret the row as an existence check (for example after :meth:`Session.exists`).

        Returns:
            ``True`` if the key exists (OK result).
            ``False`` if the result is key-not-found.

        Raises:
            AerospikeError: For any other non-OK code (via :meth:`or_raise`).

        Example:
            found = (await exists_stream.first_or_raise()).as_bool()
        """
        if self.is_ok:
            return True
        if self.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            return False
        self.or_raise()
        return False  # unreachable


def batch_records_to_results(
    batch_records: list[BatchRecord] | tuple[BatchRecord, ...],
) -> list[RecordResult]:
    """Convert ``BatchRecord`` entries to :class:`RecordResult` (library internal).

    Args:
        batch_records: Sequence of :class:`~aerospike_async.BatchRecord` from
            the async client.

    Returns:
        Parallel list with :attr:`~RecordResult.index` set to each row's
        position in ``batch_records``.
    """
    return [
        RecordResult(
            key=br.key,
            record=br.record,
            result_code=br.result_code if br.result_code is not None else ResultCode.OK,
            in_doubt=br.in_doubt,
            index=i,
        )
        for i, br in enumerate(batch_records)
    ]
