# Challenges Before Coding Starts

This document captures the pre-implementation challenges identified for the **Structure** project. They are labeled **C1–C20** for reference in planning, backlog, risk tracking, and sprint discussions.

## C1. Package and Import Layout Is Not Fully Resolved

The default project paths need to avoid confusion with the open-source package name `structure`.

Current candidate layout:

```text
structure/src
structure/generated
```

Risk: if the package itself is named `structure`, and user projects also have a top-level `structure/` directory, imports may become confusing or fragile.

Recommended default:

```text
structure_src/
structure_generated/
```

Config should still allow alternative layouts, including `structure/src` and `structure/generated`, but the default should minimize Python import ambiguity.

## C2. Schema Syntax Needs to Be Finalized

The canonical v1 schema declaration syntax is not fully settled.

Current examples use:

```python
class OrderRaw(Schema):
    id = field(string, nullable=False)
```

Alternative styles include annotation-based or dataclass/Pydantic-inspired forms.

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
Array(String())
Struct(Address)
Timestamp()
Date()
Boolean()
```

This scales better for nested schemas, Spark `StructType` generation, IDE behavior, and future adapters.

## C3. Nullability and Type Coercion Rules Are Missing

Structure needs explicit rules for nullability and type coercion.

Open questions include:

- Can nullable input feed non-nullable output?
- How are string-to-decimal and string-to-timestamp casts handled?
- How are integer widening and decimal precision/scale handled?
- What happens on invalid casts?
- How are defaults and `coalesce(...)` represented?

Recommended v1 rule:

> A nullable expression cannot feed a non-nullable field unless the developer explicitly makes it non-null.

Example:

```python
total=coalesce(to_decimal(order.total, precision=12, scale=2), lit(0))
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

Before coding, decide and document generated-code ownership.

Recommended rules:

- Generated code is committed to git.
- Generated code is never edited manually.
- Generated code is formatted deterministically.
- CI runs `structure compile --fail-on-diff`.
- Generated-code diffs are reviewed like other build artifacts.

This should be part of the Definition of Done.

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

Suggested config:

```toml
validation_mode = "schema_only"
```

Potential future mode:

```toml
validation_mode = "schema_and_constraints"
```

## C11. Streaming Compatibility Needs a Precise v1 Definition

Structure v1/v2 should support generated transforms that can operate on streaming DataFrames when the operations are compatible, but should not generate full streaming orchestration.

Define v1 streaming-compatible operations:

- `select` / projection: yes;
- `where` / filter: yes;
- expression-based derived columns: yes;
- stream-static joins: maybe, with checks;
- stream-stream joins: not v1 unless explicitly checked;
- global `orderBy`: no;
- aggregations: v2, with restrictions;
- hooks: opaque unless explicitly marked safe.

Potential hook annotation:

```python
@after(step, streaming_safe=True)
def hook(self, *, df, spark, ctx):
    ...
```

## C12. Lineage Event Schema Needs Versioning

LDJSON lineage should be versioned.

Add a header event:

```json
{"type":"lineage_file","schema_version":"1.0","structure_version":"0.1.0"}
```

or include `schema_version` on each event.

Recommended default: one header event plus stable event schemas.

Also decide whether the extension is `.ldjson` or `.ndjson`. Recommended docs term:

> LDJSON / newline-delimited JSON, using `.ldjson` files.

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

`structure check` should not require a SparkSession, Spark cluster, Java runtime, or PySpark import if avoidable.

Reasons:

- fast compile;
- easy CI;
- fewer local setup issues;
- no Spark startup just to check DSL code;
- easier adoption in Python project builds.

Generated-code execution tests can require Spark, but compiler checks should not.

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
  basic
  fields
  debug
```

Config resolution order should also be explicit:

1. CLI flags;
2. `[tool.structure]` in `pyproject.toml`;
3. `structure.toml`;
4. built-in defaults.

## C19. Versioning and Compatibility Policy Are Missing

Before open-source coding starts, define:

- supported Python versions;
- supported PySpark versions;
- whether Spark Connect is supported;
- semantic versioning policy;
- generated-code compatibility policy;
- lineage schema versioning;
- config schema versioning.

Suggested v1 baseline:

```text
Python: 3.11+
PySpark: 3.5.x and 4.0.x target
Airflow: no hard dependency
OS: Linux/macOS development, Linux runtime
```

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
  CompatibilityPolicy.md
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
SPIKE: Prove generated class import path with structure_src / structure_generated layout.
SPIKE: Prove compiler can run without PySpark/SparkSession.
SPIKE: Prove minimal generated PySpark execution test with local Spark.
```

## Highest-Risk Challenge

The most dangerous unresolved item is **Python import/package/source layout**.

Recommended default:

```text
Open-source package:
  structure/

User project source:
  structure_src/

User generated output:
  structure_generated/
```

This avoids namespace collision and gives clear import paths:

```python
from structure_src.schemas.order import OrderRaw
from structure_generated.pyspark.transforms.order import EnrichOrdersGenerated
```

The paths should remain configurable, but the defaults should be safe.
