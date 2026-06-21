# Challenges Before Coding Starts

This document captures the pre-implementation challenges identified for the **Structure** project. They are labeled **C1–C20** for reference in planning, backlog, risk tracking, and sprint discussions.

## C1. Package and Import Layout Is Not Fully Resolved

The default project paths need to avoid confusion with the open-source package name `structure`.

Earlier candidate layout:

```text
structure/src
structure/generated
```

Risk: if the package itself is named `structure`, and user projects also have a top-level `structure/` directory, imports may become confusing or fragile.

Recommended direction:

```text
src/my_package/...
generated/structure_generated/my_package/...
```

Structure should use the project's real Python source roots as input roots instead of inventing a special
source package directory. Transform discovery can identify Structure classes by `@transform`, so the default
source layout should not force users to move code under a Structure-specific source package.

Source-root resolution should be:

1. If `pyproject.toml`, `structure.toml`, or CLI flags explicitly configure Structure, use that configuration.
2. Else if `./src` exists and contains importable packages or modules, use `source_roots = ["src"]`.
3. Else use `source_roots = ["."]`.

Generated code should live under a distinct generated namespace and mirror the source import path below that
namespace. For example:

```text
src/my_package/transforms/order.py
  -> generated/structure_generated/my_package/pyspark/transforms/order.py

my_package/transforms/order.py
  -> generated/structure_generated/my_package/pyspark/transforms/order.py
```

This avoids shadowing the installed `structure` library while respecting both common Python layouts: `src/`
projects and smaller root-package projects.

## C2. Schema Syntax Needs to Be Finalized

Resolved by `docs/specifications/SchemaDeclarationSyntax.md` and decision
`docs/dev/design/decisions/D06172602.Schema-declaration-syntax.md`.

Deprecated examples used:

```python
class OrderRaw(Structure):
    id = field(string, nullable=False)
```

Alternative styles considered included annotation-based or dataclass/Pydantic-inspired forms.

Recommended v1 canonical form:

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)
```

Use explicit type objects such as:

```python
String()
Decimal(12, 2)
Float()
Double()
Array(String())
Map(String(), String())
Struct(Address)
Timestamp()
Date()
Boolean()
```

This scales better for nested schemas, Spark `StructType` generation, IDE behavior, and future adapters.

## C3. Nullability and Type Coercion Rules Are Missing

Resolved by `docs/specifications/NullabilityAndTypeCoercion.md` and planned by
`docs/dev/planning/P06172601.Nullability-and-type-coercion-rules.plan.md`.

Structure uses Spark SQL assumptions configured under `[tool.structure]` with Spark-native dotted key names:

```toml
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Recommended v1 rules:

- A nullable expression cannot feed a non-nullable field unless the developer narrows or repairs nullability explicitly.
- Spark-ANSI-compatible widening and typed literals are accepted when they do not hide business intent.
- Semantic parsing conversions such as string-to-decimal and string-to-timestamp require explicit helpers.
- Structure source may use Python literals in expression contexts; generated PySpark may lower them to `F.lit(...)`.

Example:

```python
total=coalesce(to_decimal(order.total, precision=12, scale=2), 0)
```

## C4. Python Decorator Mechanics Need a Spike

The preferred hook syntax is:

```python
@after(normalize)
def remove_bad_orders(self, *, df, spark, ctx):
    ...
```

This should work inside a class body because `normalize` exists in the class namespace before `remove_bad_orders` is defined.

However, this needs an early implementation spike to prove:

- hook attachment works reliably;
- source order is preserved;
- function identity is stable;
- source locations can be captured;
- generated code can map hooks back to methods.

## C5. Class-Local `@expr_fn` Without `self` Needs a Spike

Desired syntax:

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))


def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(
        customer_id=self.clean_id(order.customer_id),
    )
```

The helper should not take `self`, but should still be callable through `self`.

This likely requires descriptor behavior similar to `staticmethod`.

This should be proven before building the full symbolic execution engine.

## C6. Source Import Safety Is Underspecified

Discovery currently implies importing user source modules. This is simple, but imports execute top-level Python code.

Structure should document and enforce a rule:

> Structure source modules must be import-safe.

Source modules should avoid top-level side effects such as:

- creating Spark sessions;
- reading files;
- opening database connections;
- running expensive computations;
- calling external services.

The compiler may later use AST/LibCST for lower-risk discovery, but v1 can use imports if the rule is explicit.

## C7. Generated Code Ownership Rules Need to Be Explicit

Resolved by decision `docs/dev/design/decisions/D06182601.Generated-code-ownership.md`.

Structure treats generated PySpark as committed, reviewable build output owned by the compiler. Developers review it,
import it, test it, and regenerate it, but do not hand-edit it.

Ownership rules:

- Generated code is committed to git.
- Generated code is never edited manually; change Structure source, configuration, or the generator instead.
- Generated code is formatted deterministically.
- CI runs `structure compile --fail-on-diff`.
- Generated-code diffs are reviewed like other build artifacts.

These rules are part of the Definition of Done.

## C8. Hook Access to Original Inputs May Need an Escape Hatch

The simplified hook signature is:

```python
def hook(self, *, df, spark, ctx):
    ...
```

This is clean, but some hooks may need original input DataFrames for custom validation, reference lookups, or specialized joins.

Recommended optional escape hatch:

```python
@after(normalize, pass_inputs=True)
def custom_check(self, *, df, inputs, spark, ctx):
    orders = inputs.orders
    ...
```

Default hooks should remain minimal, but advanced hooks should have an opt-in path.

## C9. Join Semantics Need Sharper Definitions

`join_one(...)` needs precise semantics before implementation.

Define support for:

- composite keys;
- null-safe equality;
- case-normalized keys;
- duplicate right-side rows;
- right-side column projection;
- alias naming;
- field name collisions;
- join order;
- broadcast hints;
- v2 `join_many(...)` row multiplication.

Composite joins should be supported early:

```python
customer = self.customers.join_one(
    on=(self.customers.country == order.country)
       & (self.customers.id == order.customer_id),
    how=Join.LEFT,
)
```

Consider explicit null-safe equality support:

```python
self.customers.id.null_safe_eq(order.customer_id)
```

## C10. Intermediate Validation May Be Expensive

Intermediate schema validation is enabled by default, but the implementation must avoid unnecessary data scans.

Default validation should be schema-only:

- column names;
- data types;
- nullable flags where reliable;
- nested struct shape;
- missing or extra columns.

It should not validate every row unless explicitly requested.

Project configuration should expose both an off switch and a validation-depth mode:

```toml
validate_intermediate = true
intermediate_validation_mode = "schema_only"
```

Fuller validation mode:

```toml
intermediate_validation_mode = "schema_and_constraints"
```

Intermediate schema validation can be disabled project-wide:

```toml
validate_intermediate = false
```

This resolves the performance concern without weakening compile-time field and type checks.

## C11. Streaming Compatibility Needs a Precise v1 Definition

Resolved by `docs/specifications/StreamingCompatibility.md` and decision
`docs/dev/design/decisions/D06182604.Streaming-compatibility-v1.md`.

Structure v1 streaming compatibility means generated DataFrame transforms can run inside a caller-owned Spark
Structured Streaming query when the current pipeline DataFrame is streaming, side lookup inputs are static, and every
generated operation is compatible with that runtime shape.

Included in v1:

- row-local projection;
- row-local filtering;
- expression-based derived columns;
- schema-only validation;
- stream-static `Join.LEFT` and `Join.INNER` lookup joins;
- hooks explicitly marked `streaming_safe=True`.

Deferred or rejected in v1:

- streaming source and sink orchestration;
- stream-stream joins;
- watermarks and output modes;
- global `orderBy`;
- aggregations and windowed aggregations;
- deduplication, limits, Spark actions, RDD conversion, Python UDFs, and Pandas UDFs;
- hooks without an explicit streaming-safety promise.

## C12. Compiler Lineage Schema Needs Versioning

Resolved by `docs/dev/planning/P06182601.Compiler-provenance-static-dataflow-lineage.plan.md`.

Lineage is split into three topics:

1. compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
2. static dataflow lineage, which records transform, table, and column dependencies inferred from IR;
3. runtime LDJSON lineage, which is optional transform-run telemetry deferred beyond v4.

The v1 lineage schema should version compiler provenance and static dataflow metadata. Runtime LDJSON can define its
own record format later if the nice-to-have becomes scheduled work.

## C13. Compile-Time Performance Needs Concrete Targets

Compiler speed should be a first-class metric.

Suggested initial targets:

```text
Small project:
  10 transforms, 50 schemas
  structure check under 2 seconds warm / 5 seconds cold

Medium project:
  100 transforms, 300 schemas
  structure check under 10 seconds warm / 30 seconds cold

Single-file edit:
  v2 incremental compile under 2 seconds for affected transform
```

These targets should influence architecture decisions such as caching, v2 incremental compilation, and avoiding Spark
startup during compile.

## C14. Incremental Compile and Cache Are Missing

Fast compilers need caching and change detection.

Possible cache layout:

```text
.structure/cache/
  source_hashes.json
  discovered_models.json
  ir/
  generated_hashes.json
```

Potential commands:

```bash
structure compile --changed-only
structure clean
```

At minimum, design the compiler so v2 production incremental compilation can be added without major rework.

## C15. Need a “No Spark Dependency During Compile” Rule

Resolved by decision `docs/dev/design/decisions/D06182606.No-spark-compile-dependency.md`.

`structure check` and `structure compile` must not require a SparkSession, Spark cluster, Java runtime, or PySpark
import. The compiler operates on Structure DSL objects, source metadata, backend-neutral IR, and emitter capability
metadata. Generated-code execution tests can require Spark, but compiler checks must not.

Reasons:

- fast compile;
- easy CI;
- fewer local setup issues;
- no Spark startup just to check DSL code;
- easier adoption in Python project builds.

This rule affects discovery, schema extraction, symbolic execution, compileability checks, IR construction, code
generation, compiler provenance, static dataflow lineage, and `structure compile --fail-on-diff`.

## C16. Generated PySpark Examples Should Include Code-Size Comparison

Docs should highlight Structure’s strength in requiring less developer-maintained code.

Show comparisons such as:

```text
Hand-written PySpark:
  schema assertions + filters + joins + aliases + projections
  50+ lines maintained by developer

Structure source:
  typed schema construction and symbolic joins
  20–30 lines maintained by developer

Generated PySpark:
  visible, reviewable, deterministic, but not hand-maintained
```

This clarifies that Structure does not hide Spark; it reduces manually maintained Spark boilerplate while preserving generated Spark visibility.

## C17. Testing Should Include Mutation and Error Tests

Testing should cover both happy paths and intentionally broken transforms.

Add tests for:

- missing fields;
- wrong types;
- nullable-to-non-nullable assignment;
- invalid hook signatures;
- ambiguous public methods;
- bad source order;
- unsupported Python methods;
- `join_one(...)` without uniqueness warning;
- duplicate output fields;
- non-boolean filters;
- `@expr_fn` returning non-expression values.

These tests protect developer experience and diagnostics.

## C18. Configuration Schema Validation Is Missing

Since Structure supports TOML config, config errors need structured diagnostics.

Example invalid config:

```toml
lineage = "fieldz"
```

Expected error:

```text
Invalid config value:
  [tool.structure].lineage = "fieldz"

Allowed:
  none
  compiler
  columns
  debug
```

Config resolution order should also be explicit:

1. CLI flags;
2. `[tool.structure]` in `pyproject.toml`;
3. `structure.toml`;
4. built-in defaults.

## C19. Versioning and Compatibility Policy Are Missing

Resolved by public policy `docs/Compatibility.md`, specification
`docs/specifications/CompatibilityPolicy.md`, and decision
`docs/dev/design/decisions/D06182605.Versioning-and-compatibility-policy.md`.

v1 baseline:

- Python 3.11+.
- PySpark 3.5.x and 4.0.x target, with `target_pyspark = ">=3.5,<4.1"` by default.
- Airflow has no hard dependency.
- Linux is the runtime target; Linux and macOS are development targets.
- Spark Connect is scheduled for v4 unless it can be added earlier without changing the public DSL, generated class API,
  or generated-code review model.

## C20. Licensing and Governance Are Not Decided

This item is superceded by C31

## C21. Executable Package Skeleton Does Not Match the Public Contract

The docs and `pyproject.toml` describe an installable `structure` package with a `structure.cli:cli` entrypoint and
public imports such as `Structure`, `field`, `transform`, and `StructureSession`. The implementation tree now belongs
under `src/structure/app` and `src/structure/lib`, but the root package marker, CLI module, module execution hook, and
public API re-exports still need to be completed before the package is truly executable.

Risk: early contributors can read a polished product contract but cannot import or run the product. This makes every
later feature harder to validate because packaging, imports, and CLI wiring may fail late.

Recommended direction:

- Keep the real `src/structure/` package as the only shipped top-level import package.
- Add `src/structure/cli.py` with honest placeholder commands that fail clearly when implementation is incomplete.
- Add public re-exports in `structure.__init__` only for symbols that exist, then grow the surface deliberately.
- Add smoke tests for `import structure`, `structure --help`, and `python -m structure` if module execution is
  supported.
- Treat package-name, distribution-name, and generated-package naming as a release-blocking consistency check.

## C22. The v1 Scope Is Large Enough to Hide the First Useful Release

Resolved by `docs/dev/planning/P06202601.First-executable-contract-v0.plan.md`, the v0 model fixture under
`tests/model/v0`, and the revised Sprint 01 plan.

The roadmap's v1 remains the broad north star: online execution, optional generated PySpark, schemas, validation, joins,
hooks, compiler lineage, static dataflow, streaming compatibility reporting, diagnostics, doctor checks, and build
integration. That scope is coherent, but it is too broad to serve as the first adoption checkpoint.

The first adoption checkpoint is now v0, an internal dev/test planning label. v0 proves one executable contract before
the larger v1 scope hardens: one transform with schema declaration, projection, filtering, one `@expr_fn` helper, input
validation, online execution, generated execution, and parity tests.

Deferred from v0 into v1:

- joins;
- hooks;
- compiler provenance and static dataflow lineage;
- streaming compatibility reporting;
- setup/configuration doctor checks;
- build integration such as `compile --fail-on-diff`.

## C23. Backend Adaptability Needs a Capability Interface Before More Features

Resolved by `docs/specifications/BackendCapabilities.md`, design `docs/dev/design/BackendCapabilities.md`, decision
`docs/dev/design/decisions/D06202604.Backend-capability-interface.md`, and plan
`docs/dev/planning/P06202604.Backend-capability-interface.plan.md`.

Backend adaptability is now an explicit internal capability contract. Compiler checks, online execution, generated
PySpark emission, streaming compatibility checks, and future explain output should ask a `BackendCapabilities` object
whether a `CapabilityRequirement` is supported. The v1 profile supports ordinary PySpark for
`target_pyspark = ">=3.5,<4.1"` without importing PySpark during compiler commands.

Unsupported backend targets fail with `BACKEND-E2401`. Unsupported backend capabilities fail with `BACKEND-E2402`.
New DSL operations must declare capability behavior before they are considered supported.

## C24. Online and Generated Execution Need a Shared Semantic Contract

Resolved by `docs/specifications/ExecutionSemanticContract.md`, design
`docs/dev/design/ExecutionSemanticContract.md`, decision
`docs/dev/design/decisions/D06202601.Online-generated-semantic-contract.md`, and plan
`docs/dev/planning/P06202601.Online-generated-semantic-contract.plan.md`.

Online execution and generated code intentionally share semantics while differing in output form. The shared contract
requires checked `TransformPlan` IR plus `PySparkCapabilities` to lower into deterministic PySpark execution recipes.
`OnlinePySparkRunner` interprets those recipes with live PySpark objects, while `PySparkCodeGenerator` renders the same
recipes as source text.

Projection order, filter order, join aliasing, hook order, schema validation placement, schema projection, literal
typing, capability-selected backend spellings, and compiled-path performance guardrails belong to the shared target
plan. Imports, formatting, file headers, comments, and generated output paths remain generator concerns. Live DataFrame
binding and hook invocation remain online-runner concerns.

Each new compiled operation must add a recipe shape and an online/generated parity test before it is considered
supported.

## C25. Extension Points Are Not Yet Sorted Into Supported and Unsupported

Resolved by clarifying extension compatibility in `Readme.md`, `docs/Compatibility.md`, and the compileability checker
design. Structure now keeps the initial extension surface deliberately small:

- `@expr_fn` is the supported public extension point for reusable compiler-visible expression logic.
- `@before(...)` and `@after(...)` hooks are supported public escape hatches for arbitrary PySpark DataFrame code.
- Backend capability providers, diagnostic renderers, schema type adapters, validation policy plugins, and hook lint
  rule registries remain internal or deferred until their contracts are specified and tested.
- Monkey-patching compiler registries or relying on hidden UDF-like fallback is unsupported.

Hooks remain useful but intentionally opaque. Lineage and explain output should show hook boundaries, while diagnostics
should prefer direct DSL or `@expr_fn` fixes when logic can stay compiler-visible.

## C26. Data Quality Constraints Stop at Schema Shape

Resolved by `docs/specifications/DataQualityConstraints.md`, design
`docs/dev/design/DataQualityConstraints.md`, decision
`docs/dev/design/decisions/D06202602.Data-quality-constraints-boundary.md`, and plan
`docs/dev/planning/P06202602.Data-quality-constraints.plan.md`.

Structure v1 validation is schema-first. Default intermediate validation remains `schema_only`, which checks shape
metadata and must not scan rows. Accepted values, ranges, regex-like constraints, decimal domain rules, freshness,
uniqueness, referential checks, and row-count expectations belong to a future opt-in constraint model.

Generated PySpark schema constants are supported caller-facing `StructType` artifacts. Callers may import them for
`spark.read.schema(...)`, runtime validation, and projection before their own writes. Online execution exposes
equivalent materialized schemas after `.run(session)`, for example through `transform.schemas.output`. Generated
`*_SCHEMA` constants remain shape-only; future constraint metadata must live beside them rather than silently changing
their meaning.

Validation depth is controlled per phase with `input_validation_mode`, `intermediate_validation_mode`, and
`output_validation_mode`. Future constraints also bind to validation phases, so a constraint runs only at intended
boundaries and only when that phase allows `schema_and_constraints`.

## C27. Analytical Join Coverage Is Still Narrow

The v1 `join_one(...)` design is disciplined, but real analytical pipelines often need semi joins, anti joins,
existence checks, temporal/as-of joins, slowly changing dimension lookups, deduped lookup policies, and row-multiplying
joins.

Risk: users may reach for hooks for common join patterns before Structure has compiler-visible syntax for them. That
would reduce optimizer visibility and make lineage less useful in exactly the pipelines Structure targets.

Recommended direction:

- Keep v1 `join_one(...)` narrow, but explicitly document the common join patterns it does not cover.
- Prioritize v2 join forms by production frequency: semi/anti existence joins, `join_many(...)`, prejoin dedupe
  policies, and temporal lookup joins.
- Design temporal and SCD-style joins around explicit cardinality and validity-window semantics.
- Add examples showing when to model a lookup as `join_one(...)`, when to wait for v2 syntax, and when a hook is the
  honest escape hatch.

## C28. Operational Integration Recipes Are Missing

The docs mention Airflow as a caller and Linux as the runtime target, but there are no concrete recipes for CI,
Databricks, EMR, Glue, local development, dependency packaging, generated artifact review, or multi-environment
promotion.

Risk: the library may be technically sound but hard to adopt because teams cannot see how it fits their existing Spark
delivery path.

Recommended direction:

- Add small deployment recipes after the first executable contract works.
- Cover local development, CI with `structure check`, CI with `compile --fail-on-diff`, Airflow-generated execution,
  Databricks notebook or job usage, and packaged wheel usage.
- Document how generated files are committed, reviewed, and promoted across environments.
- Add troubleshooting entries for import roots, missing generated modules, PySpark target mismatch, and stale generated
  output.

## C29. Diagnostics Need a Registry and Documentation Contract

Resolved by public documentation `docs/Diagnostics.md`, specification `docs/specifications/Diagnostics.md`, design
`docs/dev/design/DiagnosticsContract.md`, decision `docs/dev/design/decisions/D06202603.Diagnostics-registry-contract.md`,
and plan `docs/dev/planning/P06202603.Diagnostics-registry-contract.plan.md`.

Structure diagnostics are now specified as a registry-backed contract. Codes use `{component}-{severity}{number}`,
where the prefix identifies the issuing component, such as `CONF`, `DSL`, `GEN`, `STREAM`, `ONLINE`, or `CLI`.

Every published diagnostic must include severity, title, problem, suggested fix, documentation link, and contextual
fields such as source location, transform, field, hook, join, generated path, setting, or runtime input when available.
`docs/Diagnostics.md` owns stable public anchors, while specialized specifications provide deeper context. Tests must
assert the code and high-signal structured fields, and registry validation must reject duplicate codes, malformed
codes, missing documentation links, and invalid lifecycle transitions.

## C30. Fixtures Exist, but Executable Specification Tests Are Missing

The repository has rich model source and generated fixtures under `tests/model`, but there are no normal executable
pytest tests backing the specification sections yet.

Risk: generated examples can drift from the intended compiler behavior, and completed user stories may be marked in
docs without tests proving them. The project also loses a fast feedback loop for packaging, CLI, diagnostics, and
no-Spark compile guarantees.

Recommended direction:

- Add `tests/specs/...` before marking Specification.md stories complete.
- Start with smoke tests for package import, CLI help, config defaults, and a no-op `structure check`.
- Add golden fixture tests that compare generated output only after the compiler can produce it.
- Add intentionally broken transform tests for diagnostic quality as soon as schema and symbolic execution exist.
- Keep model fixtures as inputs and expected outputs, not as a substitute for executable tests.

## C31. Licensing, Governance, and Packaging Signals Conflict

Risk: adoption can be blocked before technical evaluation. Some package indexes, companies, and open-source users may
not treat ethical-use restrictions as open-source-compatible. Naming differences can also make installation,
importing, and generated-code examples confusing.

Recommended direction:

- Decide whether the project is OSI-open-source, source-available with ethical-use restrictions, or dual-licensed.
- Make `Readme.md`, `License.md`, `pyproject.toml`, package metadata, contribution docs, and compatibility docs use the
  same licensing language.
- Decide whether the distribution name is `structure`, `taitl-structure`, or another collision-safe name.
- Add governance basics before public release: contribution guide, security policy, code of conduct if desired,
  release process, support window, and vulnerability reporting path.

# Appendix

## Recommended Pre-Coding Docs to Add

Resolved by implementation-ready specifications:

```text
docs/dev/design/
  DecisionsBeforeCoding.md

docs/specifications/
  DecisionsBeforeCoding.md
  SourceModuleRules.md
  SchemaSemantics.md
  ValidationSemantics.md
  JoinSemantics.md
  HookSemantics.md
  ConfigSchema.md
  CompatibilityPolicy.md
  CompilerPerformanceTargets.md
```

The highest-priority additions are now covered by:

```text
SchemaSemantics.md
ValidationSemantics.md
JoinSemantics.md
HookSemantics.md
CompilerPerformanceTargets.md
```

Related supporting specifications already cover schema declaration syntax, schema model extraction, nullability and
type coercion, diagnostics, online/generated execution semantics, backend capabilities, data-quality constraints,
streaming compatibility, and generated PySpark output.

## Recommended Sprint 0 Spike Tasks

Add these to Sprint 0 before the first vertical slice:

```text
SPIKE: Prove @after(method) inside class bodies.
SPIKE: Prove @expr_fn class-local helper without self parameter.
SPIKE: Prove source-order method discovery with line numbers.
SPIKE: Prove source-root discovery and generated `structure_generated.<source package>` import paths.
SPIKE: Prove compiler can run without PySpark, Java, SparkSession, Spark startup, or a Spark cluster.
SPIKE: Prove minimal generated PySpark execution test with local Spark.
```

## Highest-Risk Challenge

TBD
