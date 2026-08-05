#!/usr/bin/env python3
"""Run every illustrative example class in sequence.

Each class is instantiated and its :meth:`run` is called directly, mirroring
``aerospike-client-python/examples/string_ops/run_all_examples.py``::

    for cls in example_classes:
        example = cls()
        example.run()

Async examples connect in :meth:`Example.__init__` and disconnect in
:meth:`cleanup`. Sections that share a session receive the host in ``__init__``.
"""

import asyncio
from typing import Any, Literal

from _env import Example

from basic_example import BasicExample
from batch_example import BatchExample
from behavior_hierarchical_example import BehaviorHierarchicalExample
from behavior_yaml_example import BehaviorYamlExample
from cdt_path_expression_example import CdtPathExpressionExample
from common_example import CommonExample
from complete_yaml_config_example import CompleteYamlConfigExample
from dataset_example import DatasetExample
from map_remove_by_key_range import MapRemoveExample
from multi_record_transaction_example import MultiRecordTransactionExample
from query_examples import (
    DemonstrateBackgroundQuery,
    DemonstrateBasicWritesAndErrors,
    DemonstrateBatchReads,
    DemonstrateBitOperations,
    DemonstrateClusterInfo,
    DemonstrateComplexCdt,
    DemonstrateConditionalUpdates,
    DemonstrateFilteredUpdates,
    DemonstrateGenerationCheck,
    DemonstrateHeterogeneousBatch,
    DemonstrateMultiOperationBatches,
    DemonstratePointAndHeaderReads,
    DemonstrateQueryHints,
    DemonstrateReadWriteExpressions,
    DemonstrateRecordsPerSecondAndChunking,
    DemonstrateReusableFilter,
    DemonstrateSortingAndPagination,
    DemonstrateTtl,
    QueryExample,
    SeedData,
)
from roster_example import RosterExample
from sdk_config_example import HotReload, NamedBehaviors, SdkConfigExample
from session_example import SessionExample
from string_operations_example import StringOperationsExample
from student_scores_example import StudentScoresExample
from sync_example import SyncBasicExample
from yaml_config_connection_example import YamlConfigConnectionExample
from yaml_config_example import YamlConfigExample

RunEntry = type | Literal["query_examples", "sdk_config_examples"]


async def _create(cls: type, *args: Any, **kwargs: Any):
    """Construct an example with an async ``__init__``."""
    instance = cls.__new__(cls)
    await instance.__init__(*args, **kwargs)
    return instance


query_example_classes: list[type[QueryExample]] = [
    DemonstrateClusterInfo,
    DemonstrateBasicWritesAndErrors,
    SeedData,
    DemonstrateConditionalUpdates,
    DemonstrateBatchReads,
    DemonstrateFilteredUpdates,
    DemonstratePointAndHeaderReads,
    DemonstrateRecordsPerSecondAndChunking,
    DemonstrateSortingAndPagination,
    DemonstrateReusableFilter,
    DemonstrateTtl,
    DemonstrateReadWriteExpressions,
    DemonstrateQueryHints,
    DemonstrateBackgroundQuery,
    DemonstrateMultiOperationBatches,
    DemonstrateGenerationCheck,
    DemonstrateComplexCdt,
    DemonstrateBitOperations,
    DemonstrateHeterogeneousBatch,
]

sdk_config_example_classes: list[type[SdkConfigExample]] = [
    NamedBehaviors,
    HotReload,
]

run_order: list[RunEntry] = [
    BasicExample,
    BatchExample,
    BehaviorHierarchicalExample,
    BehaviorYamlExample,
    CdtPathExpressionExample,
    CommonExample,
    CompleteYamlConfigExample,
    DatasetExample,
    MapRemoveExample,
    MultiRecordTransactionExample,
    "query_examples",
    RosterExample,
    "sdk_config_examples",
    SessionExample,
    StringOperationsExample,
    StudentScoresExample,
    YamlConfigConnectionExample,
    YamlConfigExample,
]


async def _run_async(cls: type[Example]) -> None:
    example = await _create(cls)
    try:
        await example.run()
    finally:
        await example.cleanup()



async def _run_shared_sections(
    host_cls: type[Example],
    section_classes: list[type[Example]],
) -> None:
    host = await _create(host_cls)
    try:
        for cls in section_classes:
            print(f"=== {cls.__name__} ===")
            section = await _create(cls, host)
            await section.run()
            print()
    finally:
        await host.cleanup()


async def run_all() -> None:
    for entry in run_order:
        if entry == "query_examples":
            await _run_shared_sections(QueryExample, query_example_classes)
            continue
        if entry == "sdk_config_examples":
            await _run_shared_sections(SdkConfigExample, sdk_config_example_classes)
            continue

        print(f"=== {entry.__name__} ===")
        await _run_async(entry)
        print()


if __name__ == "__main__":
    asyncio.run(run_all())
    SyncBasicExample().run()
