# Testing

Structure requires layered testing because correctness spans source DSL semantics, execution, generated code,
runtime behavior, and performance guardrails.

## Test Layers

1. DSL unit tests.
2. Schema model tests.
3. Configuration validation tests.
4. Discovery tests.
5. Symbolic execution tests.
6. IR tests.
7. Compileability checker tests.
8. Negative compiler and diagnostic tests.
9. Execution tests.
10. Generated-code snapshot tests.
11. Syntax/import tests.
12. PySpark execution tests.
13. Execution/generated-code parity tests.
14. Performance guardrail tests.
15. Compile-time performance benchmarks.
16. Golden generated-output tests.
17. Differential tests against independently written references.
18. Metamorphic and property-based tests.
19. Public API snapshot tests.
20. Invariant tests.

## Example Apps

Public example apps live under `examples/` and are treated as both documentation and test input. Example source files live
under `examples/<package>/`; their import package is `examples.<package>`. Checked-in generated output for each
example package lives under `examples/structure_generated/<package>/` and the imports are
`examples.structure_generated.<package>`.

Layout:

```text
examples/
  orders/
    schemas.py
    transforms.py
  fixtures/
    orders/
      orders.csv
  structure_generated/
    orders/
      pyspark/
      runtime/
      traceability/
```

Use the repository root as the import base so public
examples do not create top-level packages such as `orders` that can shadow temporary projects or internal fixtures.

## Generated-Code Correctness

Generated code should be tested by:

- snapshot comparison
- `ast.parse`
- import execution
- small Spark DataFrame input/output tests
- schema validation failure tests
- compiler provenance and static dataflow traceability tests

## Golden Generated Output

Golden generated-output tests live under `tests/golden`. They render generated files into an isolated test location
such as `tmp_path` or an in-memory path-to-text map, then compare the result to checked-in files under
`examples/structure_generated/<package>/`.

Golden tests must:

- fail with a readable unified diff;
- never rewrite checked-in golden files during ordinary test runs;
- compare file additions, removals, and content changes;
- treat generated source stability as a reviewability contract.

Golden generated-output tests prove that generated source is stable and reviewable. They do not by themselves prove
runtime behavior. Runtime behavior is proved by execution/generated-code parity, differential tests, and integration tests.

## Differential Tests

Differential tests live under `tests/differential`. They compare Structure behavior or behavioral artifacts against an
independently written reference. The reference should be small, direct, and intentionally not implemented through the
same compiler path as Structure.

Use example source under `examples/<package>/` and small fixtures under `examples/fixtures/<package>/` when a scenario
needs data. When PySpark is available, differential tests should compare online Structure, generated Structure, and a
hand-written PySpark reference over the same rows. Spark-free differential tests may compare compiled/generated
behavioral contracts against hand-written reference fragments, but live DataFrame equality belongs in integration or a
PySpark-available differential suite.

## Metamorphic and Property-Based Tests

Metamorphic tests live under `tests/metamorphic`. They assert relationships that must hold across repeated or
equivalent operations, such as:

- rendering the same project twice produces byte-identical output;
- generated file order is deterministic;
- equivalent public source forms produce equivalent behavior;
- diagnostic documentation links remain stable.

Property-based tests also live under `tests/metamorphic` unless a narrower specification directory owns the behavior.
Use Hypothesis for true property-based tests. Good first targets are identifier handling, aliases, field-order
preservation, scalar type and nullability combinations, and generated path normalization.

## Public API Snapshots

Public API snapshot tests guard user-facing library shape. The root Structure API snapshot lives at
`res/testing/snapshots/api/public_structure.v1.json` and is tested from `tests/app/test_public_api_snapshot.py`.

Snapshot changes require review. A failing API snapshot means either the public API drifted accidentally or the snapshot
must be intentionally regenerated as part of a compatibility change.

## Invariant Tests

Invariant tests prove internal phase-boundary truths that should hold after Structure has accepted user input. See
[Invariants.md](specifications/Invariants.spec.md). Use invariants for impossible internal states, not user-correctable
problems. User-correctable problems must still produce structured diagnostics with documentation links.

## Execution Correctness

Execution should be tested by:

- config defaults and invalid execution-mode diagnostics
- transform invocation input binding
- deferred construction without Spark work
- `StructureSession.run(...)` delegation
- PySpark execution against small Spark DataFrames
- parity with generated PySpark output for every supported operation

## Execution/Generated-Code Parity

Every supported compiled operation must have at least one parity test before the operation is considered complete.
Parity tests run the same transform online through `StructureSession` and through the generated PySpark class, then
compare output column order, row contents, schema shape where Spark exposes it reliably, and expected validation
placement.

Each admitted PySpark feature family must also have one public black-box scenario in `tests/concepts/live_pyspark`. These
integration-marked tests are the compact concept-level release proof; tests in `tests/specifications`,
`tests/user_stories`, and `tests/integration` continue to own exhaustive semantic edges and infrastructure behavior.

Generated-code snapshots are still required for reviewability, but snapshots are secondary. The semantic authority is
runtime parity through the shared contract in [ExecutionSemanticContract.md](specifications/ExecutionSemanticContract.spec.md).

Live PySpark parity tests are opt-in because they require the optional `pyspark` package and a usable local Spark
runtime. Run them before release with:

    poetry run pytest --run-integration tests/integration/pyspark -q

If pytest reports that `pyspark` cannot be imported, install the project integration environment or run the same command
in the repository's Spark integration lane. A skipped local live test is not release evidence.

For release verification, use the Compose matrix rather than a locally installed PySpark:

    make integration BACKEND=pyspark35
    make integration BACKEND=pyspark40
    make integration BACKEND=pyspark41

The initial `pyspark41` lane selects the runtime-version check and V11 integration tests only. Run
`spark-connect35` and `spark-connect40` only for a feature family whose capability profile claims Spark Connect
support. Record the exact commands, runtime versions, and passed/skipped totals in the hardening plan's
`Outcomes & Retrospective`; this concise evidence record replaces a formal sign-off or scorecard.

## Concept Tests

Concept tests live under `tests/concepts`. They are end-to-end, black-box tests for the project vocabulary in
[Concepts.testing.md](testing/Concepts.testing.md). Their job is to prove that a named concept works through public user-facing surfaces such as
the DSL, CLI, `StructureSession`, generated packages, runtime diagnostics, and execution/generated-code parity.

Concept tests are also the concept coverage map. One test may cover several concept leaves, but the covered concept
should be visible from the test module, test name, docstring, or a nearby coverage table. Concept tests should exercise
small representative scenarios instead of duplicating every unit, specification, or integration test.

Keep concept tests focused on observable behavior:

- prefer public API, CLI, runtime output, generated package behavior, and diagnostics
- prefer execution/generated-code parity when a concept has runtime semantics
- avoid asserting compiler internals, renderer implementation details, or exact IR shape unless the concept itself is
  that public artifact
- avoid re-testing every edge already owned by `tests/specifications/...`; include one black-box representative and
  leave narrow semantic edges to the specification suite

## Negative Compiler and Diagnostic Tests

Each supported DSL feature needs at least one intentionally broken transform test when it has a meaningful failure mode.
These tests should assert the diagnostic code, location, problem summary, and suggested fix, not merely that compilation
failed.

Required negative cases:

- missing fields
- wrong types
- nullable-to-non-nullable assignment
- invalid hook signatures
- ambiguous public methods
- bad source order
- unsupported Python methods
- `lookup_join(...)` without uniqueness warning
- duplicate output fields
- non-boolean filters
- `@special(type="expr")` returning non-expression values

## Performance Guardrails

Compiled generated paths must not contain:

- `udf`
- `pandas_udf`
- `rdd`
- `collect`
- `toPandas`
- Python row maps

Hooks may use arbitrary PySpark, but strict performance mode should lint hooks and report risky operations.

## Compile-Time Performance Tests

Add benchmark fixtures for:

- 10 transforms
- 100 transforms
- 1,000 transforms
- N-step serial joins
- many schema files
- many expression helpers

Test cold compile in current releases. Add separate cold and warm incremental-compile tests when future production
incremental compile is implemented.

Warm incremental compile should avoid symbolic execution and regeneration for unchanged transforms once a future cache
is enabled.

Compiler tests must prove the no-Spark compile contract: `structure check`, `structure compile`, and
`structure compile --fail-on-diff` run without PySpark, Java, a SparkSession, Spark startup, or a Spark cluster. Keep
execution, generated-code import, and PySpark execution tests in separate suites because those may legitimately
require PySpark and a local Spark runtime.

## Testing Helpers

Reusable testing code has two homes.

Use `src/structure/lib/testing` for general reusable testing helpers that are fixture-agnostic and suitable for testing
Structure projects through stable public behavior. This package is a free-form testing library with topic-based
subpackages, not a logic module or app. Good candidates include row and schema comparison helpers, generated package
import cleanup, deterministic generated-project writing, parity runner utilities, diagnostic assertions, and filesystem
result comparisons.

Use `tests/helpers` for repository-local test helpers that know about checked-in fixtures, specific model projects, CSV
data, pytest fixtures, or scenario construction. Good candidates include `res/testing/model/...` loaders, orders or join
scenario builders, Spark-session fixture helpers, and data conversion code for one fixture family.

The dependency direction is:

```text
tests/concepts -> tests/helpers -> structure.lib.testing -> structure production code
```

`structure.lib.testing` must not import from `tests`, `res/testing/model`, or fixture-specific modules. Keep pytest-only
helpers in `tests/helpers` unless the project explicitly chooses to expose them as part of the reusable testing library.

## Test Placement

Use these directories consistently:

- `tests/app/[app]/[subapp]/...`: tests for app implementation code. Keep nesting aligned with the app and subapp
  package path.
- `tests/concepts/[concept]/...`: end-to-end black-box tests for concepts from [Concepts.testing.md](testing/Concepts.testing.md).
- `tests/golden/...`: generated-output golden comparisons for public examples.
- `tests/differential/...`: comparisons against independently written reference behavior.
- `tests/metamorphic/...`: relationship-based and property-based behavior tests.
- `tests/helpers/...`: repo-local helpers for fixture-backed or pytest-specific test scenarios.
- `tests/user_stories/[section-or-story]/...`: tests backing user stories from [UserStories.md](specifications/UserStories.spec.md).
- `tests/specifications/[specification-doc-slug]/...`: tests backing individual documents under `docs/dev/specifications/`
  when we need to prove the behavior described by a specification document directly.

Examples:

- CLI command behavior: `tests/app/cli/...`
- Target capability app behavior: `tests/app/target/capabilities/...`
- PySpark target behavior: `tests/app/target/pyspark/...`
- Join concept coverage: `tests/concepts/join/...`
- Example generated-output drift: `tests/golden/...`
- Independent reference comparison: `tests/differential/...`
- Repeated-generation stability: `tests/metamorphic/...`
- Fixture-specific scenario helpers: `tests/helpers/scenarios/...`
- User stories completed from [UserStories.md](specifications/UserStories.spec.md): `tests/user_stories/...`
- Execution semantic contract checks: `tests/specifications/execution-semantic-contract/...`
- PySpark code generation contract checks: `tests/specifications/pyspark-code-generation/...`

## CI

Recommended CI pipeline:

```text
1. ruff check
2. structure check
3. structure compile --fail-on-diff
4. pytest compiler tests
5. pytest negative compiler and diagnostic tests
6. pytest execution tests
7. pytest generated-code tests
8. pytest PySpark execution tests
9. pytest execution/generated-code parity tests
10. pytest golden tests
11. pytest differential tests that do not require live infrastructure
12. pytest metamorphic and property-based tests
13. pytest public API snapshot tests
14. pytest compatibility consistency tests
15. compile-time benchmark smoke test
16. package build
```

## Make Targets

Use these focused targets from the repository root:

```text
make golden
make differential
make metamorphic
make concepts
make rigidity
```

`make rigidity` runs the behavior-rigidity suites that do not require live Spark infrastructure. Keep Docker Compose,
live PySpark sessions, and backend matrix execution under `make integration`.

## Integration Tests

Live backend integration tests live under `tests/integration`. They are opt-in because they start or contact Docker
Compose infrastructure, import PySpark, and create live Spark sessions. Ordinary `poetry run pytest`, `make test`, and
`make build` remain Spark-free.

Run the full local backend matrix:

```text
make integration
```

Run one backend's test selection:

```text
make integration BACKEND=pyspark35
make integration BACKEND=pyspark40
make integration BACKEND=pyspark41
make integration BACKEND=spark-connect35
make integration BACKEND=spark-connect40
```

The ordinary PySpark 4.1 lane currently runs `tests/integration/pyspark/backend/test_runtime_versions.py` and
`tests/integration/pyspark/v11` only. Other integration tests continue to run on their existing backends; Connect 4.1
is a later, separate lane.

The Delta mutation suite is `tests/integration/pyspark/v11/test_delta_transform_live.py`. It uses disposable native
tables and checks preflight validation, native CHECK enforcement, delete/update/merge/append, explicit merge and append
schema evolution, and online/generated parity. Its isolated evidence pair is ordinary `pyspark==4.1.0` with
`delta-spark==4.1.0` in a Delta-enabled Spark session. After provisioning that runtime, run
`poetry run pytest --run-integration -q tests/integration/pyspark/v11/test_delta_transform_live.py`. The full V11
profile and Compose matrix remains an admission gate; see the
[Delta specification](specifications/V11DeltaSchemaBoundMutations.spec.md).

Integration runs retain the selected local Spark master/worker services and the versioned Spark Connect dependency
caches; only the disposable test runner is removed. This makes repeated focused runs fast without sharing test process
state. Run `make integration-rebuild` after changing the integration image, or `make integration-down` to stop the
services while preserving those caches.

Run integration tests after the ordinary build:

```text
make build INTEGRATION=1
```

The Compose stack is defined in `infra/compose/docker-compose.yaml`. Local values are stored in
`infra/compose/.env`, created automatically from the tracked `infra/compose/.env_example` when missing. The full stack
starts the selected PySpark backend version on distinct services and ports.

The current matrix covers ordinary PySpark 3.5/4.0 and Spark Connect over PySpark 3.5/4.0. Spark Connect lanes start
the Connect gateway inside the runner container instead of adding separate Compose services.

Pytest integration tests must use `pytest.mark.integration` and must not import PySpark at module import time. Import
PySpark inside fixtures or test functions so the default suite can collect tests without Spark installed.

Integration pytest options live in `tests/integration/pytest_plugin.py`, loaded from `pyproject.toml`. Keep
integration-specific pytest machinery under `tests/integration` so the directory conveys the test context.

Within `tests/integration/pyspark`, group live tests by backend family, fixture version, and scenario. Backend-wide
smoke tests belong under `tests/integration/pyspark/backend`, shared live-backend helpers belong under
`tests/integration/pyspark/support`, and scenario tests belong under versioned directories such as
`tests/integration/pyspark/v2/analytics`. Keep fixture-specific builders in matching support modules such as
`tests/integration/pyspark/v2/support/analytics.py`; do not create a second version hierarchy under the top-level
`support` directory.

Versioned integration fixture data belongs under `res/testing/data`. For example, the v1 orders integration scenario
uses CSV files from `res/testing/data/v1/orders`.

### Reusing preparation without retaining growing plans

The shared `generated_sources` fixture caches source maps per module and complete explicit compilation request.
Use it with `generated_project` so each test still isolates generated imports. The `snapshots` fixture performs an
eager Parquet write/read at a test-chosen preparation boundary; `snapshots.outputs(result, label="indexing")` returns
a new result containing canonical public outputs. It does not retain stage outputs or aliases. Values and column
types are preserved, but Parquet can widen nullability; do not use it for nullability-contract assertions.

Snapshots and classic checkpoint directories are unique per test under `.pytest-workspace-tmp/integration` on the
Compose shared volume and are removed by their owning fixtures. Connect 4.0 uses a runner-owned checkpoint directory
configured at server startup and removed after the server exits. Applications still own their production storage.

Use `support.timing.phase` for construction/collection timings. An eager checkpoint is charged to construction,
not to the later collection. `rows(frame, *order)` reuses an ordered collection for subsequent unordered assertions;
a different explicit order still runs in Spark. Search tests disable unneeded stage-output exposure in both runtime
and generated compilation; stage-access tests retain their own coverage.

The Search integration comparison switch is `STRUCTURE_SEARCH_STAGE_OUTPUTS=0|1`. It defaults to `0` and is resolved
once during test collection, so the same value must be used for online sessions, generated compilation, and the
shared source cache. Use `1` only for a controlled benchmark; it does not change library defaults. Invalid values
fail before a live Spark case starts. Phase messages include source setup/cleanup and snapshot cleanup, while
snapshot writes and Search construction/collection retain their own labels. Do not add actions merely to measure a
phase, and do not sum nested snapshot timings into their enclosing preparation phase.

PySpark integration modules can share compiler artifacts without sharing Spark sessions or DataFrame results. The
module-scoped `compiled_artifacts` owner supplies the pool to `GeneratedSources` and the function-scoped
`runtime_sessions` factory. Reuse is enabled by default; set `STRUCTURE_COMPILED_ARTIFACT_REUSE=off` to give every
rendering compilation and runtime session a fresh pool while retaining the same compiler options. Set
`STRUCTURE_PROFILE_COMPILATION=1` to print each complete cache request, including key construction, and a module total.
These timings include cache lookup work and are not pure compiler-execution timings. The owner is integration-only and
must not be used to retain Spark frames, results, or sessions.

Use `STRUCTURE_PLAN_BOUNDARIES=off|auto|strict` to compare the compiler's temporary-view policy on any PySpark
backend. The value configures both online execution and generated compilation and participates in source-cache keys.
The default is `auto` for batch transforms. Keep stage exposure, driver heap, fixtures, and checkpoints fixed when
comparing policies. Named views do not truncate Spark logical lineage; retain explicit checkpoints. Actual streaming
frames bypass compiler boundaries. Replace the former `STRUCTURE_CONNECT_PLAN_BOUNDARIES` override with this name.

For a separate diagnostic run, set `STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1` to time the existing DataFrame
checkpoint calls. The timer wraps the actual runtime DataFrame class for online/generated parity and adds no Spark
action. These times are nested within construction; do not add them again to the total. Keep this switch fixed
within a comparison pair.

For deeper diagnosis, set `STRUCTURE_PROFILE_QUERY_PLANS=timing` or `explain`. Unset, `0`, and `off` disable the
profiler; `1` remains an alias for `explain`. Both active modes print guard-construction timings, cache misses, and
estimated expanded input references around joins, assertions, and checkpoints. These are structural estimates, not
row counts or exact Spark optimizer node counts; expression subqueries and other complex operations are not modeled.
A view reference does not reset the estimate, while a reliable checkpoint does.

`explain` mode calls public `explain(mode="simple")` immediately before each checkpoint, reporting explanation time
and the subsequent checkpoint duration separately. `timing` mode wraps only the original checkpoint call with a timer;
it does not call `explain`, collect rows, inspect schema, or fetch a query plan. Explain requests planning, not row
collection, and checkpoint elapsed time can still include planning, serialization, scheduling, and execution: do not
label it pure executor time or file-writing time.
The plan text is captured rather than printed and remains subject to Spark's diagnostic string limit. Spark's
truncation warnings can report the full physical-plan string length. Profiling changes warm-up and timing; compare
only equally profiled runs and keep unprofiled regression evidence separate. Monkeypatches are test-scoped.

Shared sessions use two input partitions for tiny fixtures and cap diagnostic plan strings at 8192 characters.
The runner bounds pytest with `STRUCTURE_INTEGRATION_TIMEOUT` (seconds, default 3600, then a 15-second kill grace).
For a focused run against the checked-in runner without rebuilding the image:

```sh
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
  -e 'INTEGRATION_PYTEST_ARGS=-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s --durations=20' \
  -e STRUCTURE_INTEGRATION_TIMEOUT=600 structure-integration-pyspark35 \
  bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35
```

Replace both backend names to select another lane. Run memory-sensitive lanes sequentially when the Docker VM
cannot accommodate their combined driver heaps. A timeout is a failure, not a skip or evidence of support.
