# Examples — contributor guide

These scripts are **documentation source**. They are farmed onto aerospike.com as
side-by-side Java/Python tabs (e.g. `/docs/develop/client/sdk/usage/…`), snippet by
snippet, paired with the equivalent example from the Java SDK. A docs author finds
the Python counterpart of a Java snippet by **filename and section comment**, so the
value of this directory is only as good as that alignment stays. If you add or change
an example, keep the contract below.

> Running the examples (env vars, `make examples`, the `_env` helper) is covered in
> the repository [README](../README.md#examples). This file is about *writing* them.

## The contract

1. **One file per Java example, matching name.** The filename is the `snake_case` of the
   Java example class: `StringOperationsExample.java` ↔ `string_operations_example.py`.
   - Exception: never use the `*_test.py` suffix — pytest would collect the file. Drop a
     trailing `Test` from the Java name (`MapRemoveByKeyRangeTest` → `map_remove_by_key_range.py`).

2. **Section and comment alignment.** Mirror the Java example's sections in the same order,
   with aligned header comments, so paired snippets tell the same story. Section headers are
   **comments** (`# --- 1) … ---`), not `print()` calls — the comment is part of the farmed
   snippet, and this is the convention across the whole tree. (Consequence: the two languages'
   *output transcripts* won't line up line-for-line — that's expected; the farmed artifact is the
   code, not the transcript.)

3. **Pythonic above all.** "Near line-for-line" means *conceptual/section* parity rendered as
   natural async Python — context managers, comprehensions, f-strings, `snake_case`. Never
   transliterate Java. Idioms win over literal line matching.

4. **Illustrative, not a test harness.** No pass/fail counters and no comments narrating client
   or server defects. (The one exception is `operation_differences.py` / `ael_test_spec_runner.py`,
   which mirror Java examples that are themselves difference-runners.)

5. **Output must read as documentation.** A docs page shows an example's output next to its code,
   so print the *payload*: `record.bins` or a formatted line — never a bare `RecordResult`
   (its repr is a full record, not a caption).

6. **Open the connection the standard way.** Every example uses the async context-manager
   convention `async with await _env.connect().connect() as cluster:` (sync:
   `with _env.sync_connect().connect() as cluster:`), so the cluster always closes cleanly.

7. **Gate on capability, skip cleanly.** Examples that need more than a default AP cluster degrade
   to a clear skip rather than an error:
   - strong consistency → `_env.connect_sc()` (reads `AEROSPIKE_HOST_SC` + auth, SC namespace via
     `_env.sc_namespace()`);
   - a server version → `if not await _env.server_at_least(session, (8, 1, 3)): …return`.

8. **`_env` is examples-only infrastructure**, not part of the published package — the mirror of
   `benchmarks/_env.py`. It resolves connection settings from the environment so the scripts run
   with no edits. A real application constructs `ClusterDefinition` directly (as the repo README's
   Quick start does) and does **not** import `_env`.

## Blocked / not yet portable

Some Java examples have no faithful Python counterpart until a feature ships. Do **not** fake them:

- **Object mapping** — `TypedMappingExamples`, `EcommerceExample`, and the object-mapping sections
  of `QueryExamples` (typed data sets, `toObjectList`, async object mapping). PSDK reads records as
  `dict` bins.

(`CdtPathExpressionExample` is **not** blocked — `cdt_path_expression_example.py` ships it via the
low-level `CdtOperation.select_by_path`/`modify_by_path`/`remove` + `CTX.all_children[_with_filter]`
factories. PSDK only lacks the *fluent* `.on_each_child()` ergonomics.)

See `.cursor/plans/examples-parity.md` for the full status map and the live JSDK-vs-PSDK
output comparison.
