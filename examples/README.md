# Examples — contributor guide

These scripts are **documentation source**. They are farmed onto aerospike.com
(e.g. `/docs/develop/client/sdk/usage/…`) snippet by snippet, and a docs author locates a
snippet by **filename and section comment**. Both are therefore load-bearing: renaming a
file or dropping a section comment silently breaks a published page. If you add or change
an example, keep the contract below.

> Running the examples (env vars, `make examples`, the `_env` helper) is covered in
> the repository [README](../README.md#examples). This file is about *writing* them.

## The contract

1. **One file per topic, `snake_case`, and stable.** Docs pages point at these filenames, so
   renaming one is a breaking change — do it deliberately, not incidentally.
   - Exception: never use the `*_test.py` suffix — pytest would collect the file. That is why
     `map_remove_by_key_range.py` carries no `_test` despite being a focused probe.

2. **Every demonstration step gets a section comment.** Use the `# --- 1) … ---` form and keep
   the steps in a deliberate order, so a page can quote one step without dragging in its
   neighbors. The comment is part of the farmed snippet — it is how a docs author finds the
   step at all, so it describes what the step demonstrates, not how the code works.
   - Many examples also `print()` their section header so the *output* transcript reads as
     documentation too (see rule 5). Either is fine; the comment anchor is the part that is
     mandatory.

3. **Pythonic above all.** Sections carry *concepts*, rendered as natural async Python —
   context managers, comprehensions, f-strings, `snake_case`. Never transliterate another
   language's API shape into Python. Idioms win over literal line matching.

4. **Illustrative, not a test harness.** No pass/fail counters and no comments narrating client
   or server defects. (The one exception is `operation_differences.py` / `ael_test_spec_runner.py`,
   which are deliberately difference-runners.)

5. **Output must read as documentation.** A docs page shows an example's output next to its code,
   so print the *payload*: `record.bins` or a formatted line — never a bare `RecordResult`
   (its repr is a full record, not a caption).

6. **Open the connection the standard way.** Every example uses the async context-manager
   convention `async with _env.connect().connect() as cluster:` (sync:
   `with _env.sync_connect().connect() as cluster:`), so the cluster always closes cleanly.

7. **Gate on capability, skip cleanly.** Examples that need more than a default AP cluster degrade
   to a clear skip rather than an error:
   - strong consistency → `_env.connect_sc()` (reads `AEROSPIKE_HOST_SC` + auth, SC namespace via
     `_env.sc_namespace()`);
   - a server version → `if not await _env.server_at_least(session, (8, 2, 0)): …return`.

8. **`_env` is examples-only infrastructure**, not part of the published package — the mirror of
   `benchmarks/_env.py`. It resolves connection settings from the environment so the scripts run
   with no edits. A real application constructs `ClusterDefinition` directly (as the repo README's
   Quick start does) and does **not** import `_env`.

## Blocked / not yet portable

Some Java examples have no faithful Python counterpart until a feature ships. Do **not** fake them:

- **Object mapping** — `TypedMappingExamples`, `EcommerceExample`, `TransactionProcessingExample`,
  and the object-mapping sections of `QueryExamples` (typed data sets, `toObjectList`, async object
  mapping). PSDK reads records as `dict` bins.

(`CdtPathExpressionExample` is **not** blocked — `cdt_path_expression_example.py` ships it via the
low-level `CdtOperation.select_by_path`/`modify_by_path`/`remove` + `CTX.all_children[_with_filter]`
factories. PSDK only lacks the *fluent* `.on_each_child()` ergonomics.)
