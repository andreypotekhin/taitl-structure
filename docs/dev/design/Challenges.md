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

Resolved by `docs/dev/design/specifications/SchemaDeclarationSyntax.spec.md` and decision
`docs/dev/design/decisions/D06172602.Schema-declaration-syntax.md`.

Deprecated examples used:

```python
class OrderRaw(Schema):
    id = field(string, nullable=False)
```

Alternative styles considered included annotation-based or dataclass/Pydantic-inspired forms.

Recommended v1 canonical form:

```python
class OrderRaw(Schema):
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

Resolved by `docs/dev/design/specifications/NullabilityAndTypeCoercion.spec.md` and planned by
`docs/dev/planning/P06172601.Nullability-and-type-coercion-rules.md`.

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
- `join_many(...)` row multiplication.

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

Resolved by `docs/dev/design/specifications/StreamingCompatibility.spec.md` and decision
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

Resolved by `docs/dev/planning/P06182601.Compiler-provenance-static-dataflow-lineage.md`.

Lineage is split into three topics:

1. compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
2. static dataflow lineage, which records transform, table, and column dependencies inferred from IR;
3. runtime LDJSON lineage, which is optional transform-run telemetry deferred beyond v3.

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
  incremental compile under 2 seconds for affected transform
```

These targets should influence architecture decisions such as caching, incremental compilation, and avoiding Spark startup during compile.

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

At minimum, design the compiler so incremental compilation can be added without major rework.

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
`docs/dev/design/specifications/CompatibilityPolicy.spec.md`, and decision
`docs/dev/design/decisions/D06182605.Versioning-and-compatibility-policy.md`.

v1 baseline:

- Python 3.11+.
- PySpark 3.5.x and 4.0.x target, with `target_pyspark = ">=3.5,<4.1"` by default.
- Airflow has no hard dependency.
- Linux is the runtime target; Linux and macOS are development targets.
- Spark Connect is scheduled for v3 unless it can be added earlier without changing the public DSL, generated class API,
  or generated-code review model.

## C20. Licensing and Governance Are Not Decided

README currently leaves license as TBD.

Choose before coding begins because package metadata, headers, contribution rules, and downstream adoption depend on it.

Candidate licenses:

- Apache-2.0;
- MIT;
- BSD-3-Clause.

For data tooling, Apache-2.0 is often a strong choice because it includes an explicit patent grant. MIT is simpler.

## Recommended Pre-Coding Docs to Add

Add a short design set before implementation:

```text
devdocs/
  DecisionsBeforeCoding.md
  SourceModuleRules.md
  SchemaSemantics.md
  ValidationSemantics.md
  JoinSemantics.md
  HookSemantics.md
  ConfigSchema.md
  design/specifications/CompatibilityPolicy.spec.md
  CompilerPerformanceTargets.md
```

Highest-priority additions:

```text
SchemaSemantics.md
ValidationSemantics.md
JoinSemantics.md
HookSemantics.md
CompilerPerformanceTargets.md
```

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

The most dangerous unresolved item is **Python import/package/source layout**.

Recommended default:

```text
Open-source package:
  structure/

User project source:
  src/my_package/

User generated output:
  generated/structure_generated/my_package/
```

This avoids namespace collision and gives clear import paths:

```python
from my_package.schemas.order import OrderRaw
from structure_generated.my_package.pyspark.transforms.order import EnrichOrdersGenerated
```

The paths should remain configurable, but the defaults should be safe.
