# Testing

Structure requires layered testing because correctness spans source DSL semantics, online execution, generated code,
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
9. Online execution tests.
10. Generated-code snapshot tests.
11. Syntax/import tests.
12. PySpark execution tests.
13. Online/generated parity tests.
14. Performance guardrail tests.
15. Compile-time performance benchmarks.
16. Golden generated-output tests.
17. Differential tests against independently written references.
18. Metamorphic and property-based tests.
19. Public API snapshot tests.
20. Invariant tests.

## Public Examples

Public examples live under `examples/` and are treated as both documentation and test input. Example source files live
under `examples/<package>/`, but their import package is `examples.<package>`. Checked-in generated output for each
example package lives under `examples/structure_generated/<package>/` and imports as
`examples.structure_generated.<package>`.

Example layout:

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

Keep `res/testing/model/v*` focused on internal model fixtures. Do not put every public example domain under
`res/testing/model/v*`; use `examples/` for user-readable examples and their golden generated output.

Do not add `examples` as an import root for tests or IDE setup. Use the repository root as the import base so public
examples do not create top-level packages such as `orders` that can shadow temporary projects or internal fixtures.
`make lint` and `make type` include hand-written example source under `examples/`, while excluding fixtures and
checked-in generated golden output.

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
runtime behavior. Runtime behavior is proved by online/generated parity, differential tests, and integration tests.

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
[Invariants.md](../specifications/Invariants.md). Use invariants for impossible internal states, not user-correctable
problems. User-correctable problems must still produce structured diagnostics with documentation links.

## Online Execution Correctness

Online execution should be tested by:

- config defaults and invalid execution-mode diagnostics
- transform invocation input binding
- deferred construction without Spark work
- `StructureSession.run(...)` delegation
- online PySpark execution against small Spark DataFrames
- parity with generated PySpark output for every supported v1 operation

## Online/Generated Parity

Every supported compiled operation must have at least one parity test before the operation is considered complete.
Parity tests run the same transform online through `StructureSession` and through the generated PySpark class, then
compare output column order, row contents, schema shape where Spark exposes it reliably, and expected validation
placement.

Generated-code snapshots are still required for reviewability, but snapshots are secondary. The semantic authority is
runtime parity through the shared contract in [ExecutionSemanticContract.md](../specifications/ExecutionSemanticContract.md).

## Concept Tests

Concept tests live under `tests/concepts`. They are end-to-end, black-box tests for the project vocabulary in
[Concepts.md](Concepts.md). Their job is to prove that a named concept works through public user-facing surfaces such as
the DSL, CLI, `StructureSession`, generated packages, runtime diagnostics, and online/generated parity.

Concept tests are also the concept coverage map. One test may cover several concept leaves, but the covered concept
should be visible from the test module, test name, docstring, or a nearby coverage table. Concept tests should exercise
small representative scenarios instead of duplicating every unit, specification, or integration test.

Keep concept tests focused on observable behavior:

- prefer public API, CLI, runtime output, generated package behavior, and diagnostics
- prefer online/generated parity when a concept has runtime semantics
- avoid asserting compiler internals, renderer implementation details, or exact IR shape unless the concept itself is
  that public artifact
- avoid re-testing every edge already owned by `tests/specifications/...`; include one black-box representative and
  leave narrow semantic edges to the specification suite

## Negative Compiler and Diagnostic Tests

Each supported DSL feature needs at least one intentionally broken transform test when it has a meaningful failure mode.
These tests should assert the diagnostic code, location, problem summary, and suggested fix, not merely that compilation
failed.

Required v1 negative cases:

- missing fields
- wrong types
- nullable-to-non-nullable assignment
- invalid hook signatures
- ambiguous public methods
- bad source order
- unsupported Python methods
- `join_one(...)` without uniqueness warning
- duplicate output fields
- non-boolean filters
- `@expr_fn` returning non-expression values

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

Test cold compile in v1. Add separate cold and warm incremental-compile tests when v2 production incremental compile is
implemented.

Warm incremental compile should avoid symbolic execution and regeneration for unchanged transforms once the v2 cache is
enabled.

Compiler tests must prove the no-Spark compile contract: `structure check`, `structure compile`, and
`structure compile --fail-on-diff` run without PySpark, Java, a SparkSession, Spark startup, or a Spark cluster. Keep
online execution, generated-code import, and PySpark execution tests in separate suites because those may legitimately
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
- `tests/concepts/[concept]/...`: end-to-end black-box tests for concepts from [Concepts.md](Concepts.md).
- `tests/golden/...`: generated-output golden comparisons for public examples.
- `tests/differential/...`: comparisons against independently written reference behavior.
- `tests/metamorphic/...`: relationship-based and property-based behavior tests.
- `tests/helpers/...`: repo-local helpers for fixture-backed or pytest-specific test scenarios.
- `tests/user_stories/[section-or-story]/...`: tests backing user stories from [UserStories.md](../specifications/UserStories.md).
- `tests/specifications/[specification-doc-slug]/...`: tests backing individual documents under `docs/specifications/`
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
- User stories completed from [UserStories.md](../specifications/UserStories.md): `tests/user_stories/...`
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
6. pytest online execution tests
7. pytest generated-code tests
8. pytest PySpark execution tests
9. pytest online/generated parity tests
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

Run one backend's test selection against the all-version stack:

```text
make integration BACKEND=pyspark35
make integration BACKEND=pyspark40
```

Run integration tests after the ordinary build:

```text
make build INTEGRATION=1
```

The Compose stack is defined in `infra/compose/docker-compose.yaml`. Local values are stored in
`infra/compose/.env`, created automatically from the tracked `infra/compose/.env_example` when missing. The full stack
starts the currently claimed PySpark backend versions at the same time on distinct services and ports.

Pytest integration tests must use `pytest.mark.integration` and must not import PySpark at module import time. Import
PySpark inside fixtures or test functions so the default suite can collect tests without Spark installed.

Integration pytest options live in `tests/integration/pytest_plugin.py`, loaded from `pyproject.toml`. Keep
integration-specific pytest machinery under `tests/integration` so the directory conveys the test context.

Versioned integration fixture data belongs under `res/testing/data`. For example, the v1 orders integration scenario
uses CSV files from `res/testing/data/v1/orders`.
