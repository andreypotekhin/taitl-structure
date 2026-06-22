# PySpark Code Generation

## Purpose

PySpark code generation lowers Structure compiler IR into deterministic, readable Python modules that use PySpark
DataFrame and Column APIs. The generated modules are optional for ordinary runtime execution, because online execution
is the v1 default, but they remain first-class artifacts for provenance, code review, debugging, snapshot tests, and
projects that deliberately choose `execution_mode = "generated"`.

The generator is a source-text emitter. It must not redefine transform semantics. Projection, filtering, expression
lowering, join aliasing, hook order, validation placement, schema projection, and performance guardrails must agree
with online PySpark execution.

Shared semantics are owned by `docs/specifications/ExecutionSemanticContract.md`. The generator renders
`PySparkExecutionPlan` recipes, or the local implementation equivalent, into source text. It owns imports, formatting,
file headers, generated paths, and readability. It must not make separate semantic choices that bypass the shared
recipes.

## Scope

This specification owns generated PySpark source shape and generator behavior for:

- generated schema modules;
- generated transform classes;
- generated runtime support modules needed by generated code;
- compiler provenance and static dataflow traceability files;
- deterministic imports, names, aliases, formatting, and write-if-changed behavior;
- backend capability selection for PySpark syntax;
- generated-code diagnostics and acceptance tests.

Semantic contracts are owned by narrower specifications:

- public DSL and transform IR: `docs/specifications/DSL.md`;
- online/generated execution parity: `docs/specifications/ExecutionSemanticContract.md`;
- online and generated runtime selection: `docs/specifications/OnlineExecution.md`;
- schema model and Spark type mapping: `docs/specifications/SchemaModel.md`;
- data quality constraint boundaries: `docs/specifications/DataQualityConstraints.md`;
- schema declaration syntax: `docs/specifications/SchemaDeclarationSyntax.md`;
- schema inheritance: `docs/specifications/SchemaInheritance.md`;
- nullability and assignment compatibility: `docs/specifications/NullabilityAndTypeCoercion.md`;
- join semantics: `docs/specifications/JoinSemantics.md`;
- CLI command behavior and stale generated output checks: `docs/specifications/CLI.md`;
- streaming constraints: `docs/specifications/StreamingCompatibility.md`;
- compatibility policy: `docs/specifications/CompatibilityPolicy.md`.

When this document overlaps those specifications, this document owns how already-decided semantics are rendered as
PySpark source text. The shared execution contract owns parity between online and generated PySpark consumers. The
narrower specification owns feature behavior.

## Generated Layout

Default generated output:

```text
generated/
  structure_generated/
    my_package/
      pyspark/
        schemas/
        transforms/
    runtime/
    traceability/
```

Rules:

- `generated` is the configurable filesystem output root.
- `structure_generated` is the configurable generated Python package name.
- Source package paths are mirrored under the generated package.
- The `pyspark` segment identifies the target backend.
- `schemas/` contains generated Spark `StructType` declarations.
- Generated schema constants are caller-facing shape artifacts that may be imported outside generated transform classes.
- `transforms/` contains generated transform classes.
- `runtime/` contains small generated-runtime helpers, such as schema validation and schema projection.
- `traceability/` contains compiler metadata and static dataflow traceability, not runtime telemetry.
- Every generated Python package directory must contain an `__init__.py` file when the target layout requires it for
  importability.
- Generated paths must be stable for the same source root, module name, class name, configuration, and target backend.

Example mapping:

```text
src/orders/schemas/order.py
  -> generated/structure_generated/orders/pyspark/schemas/order.py

src/orders/transforms/order.py
  -> generated/structure_generated/orders/pyspark/transforms/order.py
```

## Configuration

Generation consumes the resolved Structure configuration used by `structure compile`:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
traceability = "compiler"
validate_inputs = true
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
output_validation_mode = "schema_only"
strict_performance = true
format_generated = true
```

Required behavior:

- `target_backend` must be `pyspark` for this generator.
- `target_pyspark` selects PySpark syntax through the backend capability registry.
- `generated_dir` and `generated_package` determine output paths and import paths.
- `traceability = "compiler"` writes compiler provenance and static dataflow traceability files.
- `traceability = "none"` skips traceability files.
- `validate_inputs`, `validate_intermediate`, `validate_outputs`, and method/class overrides determine validation calls.
- `input_validation_mode`, `intermediate_validation_mode`, and `output_validation_mode` determine how strong validation
  is when enabled.
- `strict_performance = true` turns prohibited generated operations into compile errors.
- `format_generated = true` formats changed Python files when a formatter is available.

If `format_generated` is not implemented as a public setting in the first milestone, formatting may be an internal
compiler option. The generated text must still be deterministic and readable.

## Compiler Boundary

`structure check`, `structure compile`, `structure compile --fail-on-diff`, and `structure explain` must remain
Spark-free. PySpark code generation must not import PySpark while compiling. It emits Python source text that imports
PySpark only when the generated module is later imported at runtime.

The generator consumes compiler data structures, not live Spark objects:

```text
ResolvedConfig
SchemaDef[]
TransformPlan[]
PySparkCapabilities
CompilerProvenance
StaticDataflowTraceability
```

The generator must not:

- start Java;
- create or require a `SparkSession`;
- inspect live DataFrames;
- contact a Spark cluster;
- evaluate PySpark `Column` objects;
- execute hook functions;
- import generated modules while compiling.

## Generated Public API

Each source transform class maps to one generated class.

Source:

```python
@transform
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...
```

Generated:

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()

    def run(self, *, orders: DataFrame, customers: DataFrame) -> TransformResult:
        ...
```

Rules:

- Generated class name is the source class name plus `Generated`.
- Constructor parameters are keyword-only: `spark` and optional `ctx`.
- `spark` is the caller-supplied `SparkSession`.
- `ctx` is passed to hooks.
- `run(...)` uses keyword-only parameters matching declared transform input names.
- `run(...)` returns a generated-runtime `TransformResult`.
- Single-output generated transforms expose the DataFrame as `result.df`.
- Multi-output generated transforms expose declared output names such as `result.accepted` and `result["rejected"]`.
- Input parameter order follows source input declaration order.
- A source transform instance is created only when at least one hook exists.
- Hook-free generated classes must omit the source transform import and `_impl` field.
- The generated class must not inherit from the source transform class.
- Generated classes are owned by the compiler. Users must not subclass or edit generated classes.

Generated execution through `GeneratedPySparkRunner` imports this class, instantiates it with
`spark=session.spark` and `ctx=session.ctx`, and calls `run(...)` with the transform invocation's stored input
DataFrames.

## File Headers

Every generated Python file must start with a stable ownership header:

```python
# Generated by Structure. Do not edit by hand.
# Source: orders.transforms.order.EnrichOrders
```

Rules:

- The header must not include timestamps, machine-specific paths, random IDs, or absolute workspace paths.
- Schema modules may list the source schema module instead of a single transform.
- Runtime support files may use `Source: Structure generated runtime`.
- Traceability metadata files may include compiler version when available, but not wall-clock time unless a future
  reproducibility policy explicitly allows it.
- The `structure clean` command may use this header as one safety marker for generated ownership.

## Imports

Generated imports must be deterministic and minimal.

Common transform imports:

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
```

Schema module imports:

```python
from pyspark.sql import types as T
```

Rules:

- Sort imports by stable groups: standard library, third-party, source project, generated project.
- Within each group, sort lexicographically.
- Import `pyspark.sql.functions as F` only when generated expressions, filters, joins, hooks, or literals require it.
- Import `pyspark.sql.types as T` only in schema or runtime modules that construct Spark types.
- Import `DataFrame` and `SparkSession` when type annotations use them.
- Import source transform classes only when hooks exist.
- Import source schema classes only if generated code needs source schema metadata at runtime. The default generated
  schema modules should not need source schema imports.
- Import generated schema constants from generated schema modules.
- Treat generated schema constants as ordinary PySpark `StructType` values usable by caller code.
- Import generated runtime helpers only when used.
- Do not use wildcard imports.
- Do not emit unused imports.

Formatting may collapse or expand import blocks, but must not make import order nondeterministic.

## Schema Module Generation

Each discovered Structure schema must produce a Spark schema constant.

Example:

```python
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("name", T.StringType(), nullable=True),
    T.StructField("tier", T.StringType(), nullable=True),
])
```

Rules:

- Constant names are upper snake case from the schema class name, suffixed with `_SCHEMA`.
- Field order follows `SchemaDef.fields`.
- Online execution materializes equivalent Spark schemas from the same `SchemaDef.fields` model instead of importing
  generated schema modules.
- Inherited fields are rendered in effective schema order after inheritance resolution.
- Spark type mapping follows `docs/specifications/SchemaModel.md`.
- Nested `Struct(...)` fields render nested `T.StructType(...)` values.
- `Array(...)` and `Map(...)` preserve item, key, value, and nullability metadata.
- `primary_key` and Structure-only metadata do not affect Spark `StructField` nullability except where schema model
  rules say `primary_key=True` implies `nullable=False`.
- Generated `*_SCHEMA` constants are shape-only. Future data-quality constraint metadata must be emitted separately
  unless a later design deliberately adds Spark-compatible metadata without changing schema shape semantics.
- Generated schema text must not import source schema classes.
- Identical schema names in different modules must remain disambiguated by module path, not by altering constant names.

Schema modules may contain multiple constants when multiple schema classes are declared in one source module.

## Runtime Support Generation

Generated code may depend on generated runtime helpers under:

```text
generated/structure_generated/runtime/
```

Required helpers:

```python
def assert_schema(df, schema, *, name: str, mode: str) -> None:
    ...

def project_schema(df, schema):
    ...
```

Rules:

- `assert_schema(...)` validates required columns, unexpected columns in strict mode, Spark data types, nullability
  where reliable, and nested shapes where implemented.
- `project_schema(...)` returns a DataFrame with columns selected in schema field order.
- Runtime helpers may import PySpark because generated modules are runtime artifacts.
- Runtime helpers are also useful to caller code that wants to validate and project before a caller-owned write.
- Runtime helper APIs must stay small and stable, because all generated transform modules import them.
- Runtime helper diagnostics must mention the schema name, validation mode, offending column or field, suggested fix,
  and a link to the relevant specification or troubleshooting document.
- Runtime helpers must not start or stop Spark sessions.
- Runtime helpers must not call `collect`, `toPandas`, or row-wise operations.

The first implementation may keep runtime helpers as source-controlled library code instead of generated files if that
is simpler. The generated import path must still be stable for generated classes.

## Transform Generation Pipeline

Generation for one `TransformPlan` follows this logical sequence:

1. Validate the transform plan and target capabilities.
2. Lower the checked `TransformPlan` to shared PySpark execution recipes.
3. Collect required schema constants, runtime helpers, source hooks, and PySpark function usage from the recipes.
4. Build an in-memory Python module representation.
5. Render the module to text.
6. Format the text when configured.
7. Compare with the existing file content.
8. Write the file only when content changed.

The generator should expose this pipeline through small implementation units:

```text
GeneratePySparkProject
GeneratePySparkSchemas
GeneratePySparkTransforms
GeneratePySparkRuntime
GeneratePySparkTraceability
RenderPySparkModule
RenderPySparkExpression
RenderPySparkJoin
RenderPySparkValidation
WriteGeneratedFiles
FormatGeneratedPython
```

Names are illustrative, but the decomposition should keep orchestration separate from rendering logic.

## Current DataFrame Variable

Generated transform methods use `df` as the current DataFrame variable.

Rules:

- The first compiled step starts from the declared input DataFrame selected by source-order schema flow.
- After each step, `df` contains that step's output DataFrame.
- Hooks receive the current `df` and must return the new current DataFrame.
- Joins may introduce temporary DataFrame variables named from stable aliases, such as `customers_df`.
- Avoid reusing input parameter names for aliased or projected temporary DataFrames.
- Avoid hidden mutation. Each DataFrame operation should assign a resulting DataFrame to `df` or a clearly named
  temporary.

Readable generated code is part of the contract. The generator may introduce short local variables when that makes a
join, hook, or complex expression easier to review.

## Step Shape

Each compiled subtransform renders as a contiguous generated code block.

Canonical order inside one step:

1. before hooks for the step;
2. source-order filters and joins that must run before projection;
3. projection into the step output schema;
4. after hooks for the step;
5. schema validation and optional projection after hooks.

Example:

```python
# Subtransform: normalize
df = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
)

df = self._impl.remove_negative_totals(df=df, spark=self.spark, ctx=self.ctx)
assert_schema(df, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")
```

Rules:

- The step comment is stable and uses the source subtransform name.
- `where(...)` should be generated before projection when source semantics allow it.
- Projection should use `select(...)`.
- Output columns in `select(...)` follow the output schema field order.
- Output field names are explicit with `.alias("field_name")`.
- Generated code must not rely on implicit carry-through columns.
- Generated code must not append right-side join columns implicitly.

## Expression Lowering

Expressions lower to PySpark `Column` expressions.

Rules:

- Field references lower to qualified `F.col("alias.field")` when multiple scopes are present.
- Field references may lower to unqualified `F.col("field")` only when the current DataFrame has a single unambiguous
  scope and no join alias is required.
- Python literals lower to `F.lit(...)` where PySpark requires a Column.
- `None` lowers to `F.lit(None)` with an explicit cast when target context requires one.
- String, numeric, boolean, date, and timestamp literals follow
  `docs/specifications/NullabilityAndTypeCoercion.md`.
- Helper calls lower to PySpark functions selected by `PySparkCapabilities`.
- Boolean combination lowers with Column operators `&`, `|`, and `~` with parentheses that preserve source semantics.
- Normal equality lowers to `==`.
- Null-safe equality lowers to the PySpark null-safe equality operation selected by capabilities.
- Casts lower to Spark SQL type strings or `DataType` objects consistently for the selected PySpark range.
- Unsupported expression kinds must fail before text is written.

The generator must not lower unsupported expressions to Python UDFs, Pandas UDFs, row-wise functions, RDD operations,
or hooks.

## Filter Lowering

`where(...)` operations render as PySpark `.where(...)`.

Rules:

- Multiple adjacent filters may be combined into one `.where(...)` with `&` when source-order semantics are unchanged.
- Filters separated by joins or hooks must not be moved across those boundaries unless a future optimizer proves it is
  safe and parity tests cover the move.
- Filters that reference joined scopes must be rendered after the corresponding join.
- Filter expressions must be parenthesized enough to preserve Column operator precedence.
- `where(...)` must not be replaced with `filter(...)` unless the project deliberately standardizes on that spelling.
  Use one spelling consistently. The canonical spelling is `.where(...)`.

## Projection Lowering

Schema output construction renders as `.select(...)`.

Rules:

- Projection order follows the output schema field order.
- Every output field is explicitly selected.
- Every selected expression is explicitly aliased to the output field name.
- `SchemaClass.base(row)(...)` first maps inherited base fields from the base row, then applies explicit overrides and
  additions according to schema construction semantics.
- Extra columns are not preserved through compiled projection unless a hook with `project_output=False` is allowed to
  leave them and validation mode permits it.
- Duplicate output field names are compile errors before generation.

Projection is the boundary where generated code becomes reviewable: a reader must be able to see exactly which columns
enter the next step.

## Join Lowering

Joins lower according to `docs/specifications/JoinSemantics.md`.

Canonical generated shape:

```python
df = df.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))
df = df.join(
    customers_df,
    F.col("customers.id") == F.col("order_normalized.customer_id"),
    "left",
).select(
    F.col("order_normalized.id").alias("id"),
    F.col("customers.name").alias("customer_name"),
)
```

Rules:

- The current DataFrame receives a stable alias derived from the current schema or step name.
- The right DataFrame receives a stable alias derived from the input name.
- Repeated joins of the same input receive deterministic suffixes such as `customers_2`.
- Diagnostics and traceability refer to repeated joins as `customers#1`, `customers#2`, and so on.
- `Join.LEFT` lowers to Spark join type `"left"`.
- `Join.INNER` lowers to Spark join type `"inner"`.
- `JoinHint.BROADCAST` applies to the right side and may lower to `F.broadcast(right_df)`.
- Composite keys render key comparisons in IR order, combined with `&`.
- Null-safe key pairs render with the selected PySpark null-safe equality syntax.
- Right-side projection should carry only right-side key expressions and fields needed by downstream filters,
  projections, diagnostics, or traceability.
- The final projection after the join must remove duplicate and temporary right-side columns.

The generator must not silently deduplicate right-side rows for `join_one(...)`. Unproven uniqueness is a warning or
error owned by join compileability checks, not by generated code.

## Hook Lowering

Hooks are explicit runtime escape hatches and are called on the source transform instance.

Generated call without input namespace:

```python
df = self._impl.remove_negative_totals(df=df, spark=self.spark, ctx=self.ctx)
```

Generated call with input namespace:

```python
inputs = HookInputs(orders=orders, customers=customers)
df = self._impl.compare_to_raw(df=df, inputs=inputs, spark=self.spark, ctx=self.ctx)
```

Rules:

- Generate `_impl = SourceTransform()` only when at least one hook exists.
- Before hooks render before compiled operations for the target step.
- After hooks render after compiled operations for the target step.
- Multiple hooks for the same target and timing follow source order.
- Hooks always receive keyword arguments.
- Generate a read-only `HookInputs` namespace only when at least one hook declares `pass_inputs=True`.
- Build the hook input namespace once near the start of `run(...)` when needed.
- The namespace contains original declared input DataFrames, not intermediate DataFrames.
- Hook return values become the new current `df`.
- Hook output validation and optional projection follow the hook metadata.

Generated code does not inspect hook internals. Hook behavior is opaque to compiler traceability except for the declared
hook boundary.

## HookInputs Namespace

Generated `HookInputs` may be imported from generated runtime support or emitted into a runtime support module.

Required behavior:

```python
inputs.orders
inputs.customers
```

Rules:

- Attributes are read-only after construction.
- Unknown attributes raise normal `AttributeError`.
- The namespace is lightweight and does not copy DataFrames.
- It must not expose mutation helpers such as `__setitem__`.
- Its constructor preserves input declaration order for deterministic `repr` if a `repr` is implemented.

Using a frozen dataclass, named tuple, or small custom class is acceptable. The public API is attribute access.

## Validation Placement

Generated validation must match online execution.

Rules:

- Validate declared input DataFrames at the start of `run(...)`.
- Validate subtransform outputs when validation policy says to validate them.
- Validate after hooks according to the hook's `schema_mode`.
- If `project_output=True`, validate with the hook schema mode, project to the declared output schema, then validate
  strictly.
- Validate the final returned DataFrame unless validation is disabled for that final step by policy.
- Validation uses generated schema constants and generated runtime helpers.
- Validation calls must include the schema display name and validation mode.

Example:

```python
assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="allow_extra_columns")
df = project_schema(df, ORDER_ENRICHED_SCHEMA)
assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
```

Disabling validation should remove the corresponding `assert_schema(...)` call, not call it with a no-op mode, unless a
future runtime helper contract explicitly chooses no-op modes.

## Capability Registry

PySpark API choices must go through the backend capability interface specified in
`docs/specifications/BackendCapabilities.md`.

Rules:

- Capability selection uses configured `target_backend` and `target_pyspark`.
- Capability checks run during compiler phases without importing PySpark.
- Unsupported backend targets fail with `BACKEND-E2401`.
- Unsupported feature requirements fail with `BACKEND-E2402`.
- The online runner and generated generator use the same capability data.
- PySpark-specific syntax choices belong in the PySpark target layer, not in DSL objects.
- Adding support for a new PySpark range should usually add capability tests and snapshot tests, not change transform
  source.

## Performance Guardrails

Generated compiled paths must not contain:

- Python UDFs;
- Pandas UDFs;
- RDD operations;
- `collect`;
- `toPandas`;
- row-wise maps;
- Python loops over DataFrame rows;
- local materialization of DataFrame contents.

Rules:

- If `strict_performance = true`, any IR operation requiring a prohibited construct is a compile error.
- If a user needs arbitrary PySpark, they must use an explicit hook.
- Hooks may contain arbitrary user PySpark code, but generated compiled code must keep hook boundaries visible.
- Generated source should remain optimizer-visible through DataFrame and Column APIs.

The generator should include simple static self-checks before writing files so prohibited text patterns are caught even
if an emitter is accidentally changed.

## Determinism

For identical source, configuration, Structure version, and target capabilities, generated output must be identical.

Rules:

- Do not include timestamps.
- Do not include absolute workspace paths.
- Do not include memory addresses or object IDs.
- Do not depend on dictionary iteration unless the dictionary is ordered by construction or explicitly sorted.
- Preserve source order where semantics depend on source order.
- Sort independent artifacts and import names deterministically.
- Use stable alias names.
- Use stable line wrapping through the formatter or renderer.
- Normalize line endings for `--fail-on-diff` comparisons as specified by `docs/specifications/CLI.md`.

Determinism is required for code review, snapshot tests, and CI stale-output checks.

## Write-If-Changed and Formatting

The compiler should generate all file contents in memory before writing.

Rules:

- Write only changed files.
- Preserve timestamps of unchanged files.
- Format only changed Python files.
- Count written, unchanged, added, and removed files for CLI summaries.
- In normal `structure compile`, remove generated files that are no longer present in the manifest when they are known
  Structure-owned files.
- In `structure compile --fail-on-diff`, write to a temporary directory and do not modify configured generated output.
- If formatting fails, fail the compile with a diagnostic that names the generated path and formatter command or
  library.

The first implementation may skip deleting obsolete files if no manifest exists yet. In that case, `structure clean`
and `--fail-on-diff` must still avoid deleting unknown user files.

## Parallel Generation

The generator should support parallel generation by transform module.

Rules:

- Build immutable IR before parallel rendering starts.
- Render each schema module, transform module, runtime module, and traceability file independently.
- Do not mutate shared import collectors or name registries from multiple workers.
- Merge rendered files by sorted generated path.
- Write files in sorted path order after rendering completes.
- Report diagnostics in deterministic order even if rendering ran in parallel.

Parallelism is an implementation optimization. It must not change generated output.

## Compiler Provenance and Traceability

When traceability is enabled, generation writes compiler metadata under `traceability/`.

Required metadata:

```text
source transform
generated transform class
source schemas
generated schema constants
steps
inputs
filters
joins
projections
hooks
validation points
opaque hook boundaries
target backend
target capabilities
```

Rules:

- Traceability files are deterministic compiler artifacts, not runtime telemetry.
- Traceability must not include row counts, execution time, Spark application IDs, or cluster details.
- File format may be JSON in the first implementation because it is easy to diff and consume.
- JSON keys must be sorted or rendered in a stable order.
- Paths inside traceability should be project-relative where paths are needed.
- Hook boundaries must be marked opaque.
- Generated transform files and traceability files must agree on step, alias, join, hook, and validation names.

## Diagnostics

Generation diagnostics must include:

- diagnostic code;
- generated path when available;
- source transform or schema when relevant;
- source subtransform, hook, join, field, or expression when relevant;
- target backend and target PySpark range when relevant;
- problem;
- why it matters when not obvious;
- suggested fix;
- link to this specification or the narrower semantic specification.

Unsupported target example:

```text
CompileError BACKEND-E2402: Unsupported backend capability

Target:
  target_pyspark = "<3.4"

Problem:
  The PySpark generator has no capability profile for this target range.

Use:
  Set target_pyspark to a supported range such as ">=3.5,<4.1".

See docs/specifications/BackendCapabilities.md
```

Generation failure example:

```text
CompileError GEN-E0902: Cannot render expression as PySpark

Transform:
  orders.transforms.order.EnrichOrders

Subtransform:
  normalize

Expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods are not part of the compileable expression IR.

Use:
  customer_id=lower(trim(order.customer_id))

See docs/specifications/DSL.md
```

Formatting failure example:

```text
CompileError GEN-E0903: Generated formatter failed

Generated path:
  generated/structure_generated/orders/pyspark/transforms/order.py

Problem:
  The formatter could not parse the generated Python module.

Use:
  Run structure compile --profile and report this as a Structure generator bug.

See docs/specifications/PySparkCodeGeneration.md
```

## Generated Mode Import Failures

Generated code generation must support the generated execution diagnostics specified by
`docs/specifications/OnlineExecution.md`.

When generated mode cannot import a generated class, runtime diagnostics should suggest:

- running `structure compile`;
- ensuring `generated_dir` is on `PYTHONPATH` or marked as a source root;
- checking `generated_package`;
- switching to `execution_mode = "online"` when generated artifacts are not desired.

The generator should make this easy by using stable, predictable module names and by writing every required
`__init__.py` file.

## Non-Goals

The following are outside v1 PySpark generation scope:

- generating Python UDFs or Pandas UDFs from compiled expressions;
- generating RDD-based implementations;
- generating Airflow DAGs, Spark submit scripts, or orchestration code;
- managing `readStream`, `writeStream`, triggers, checkpoints, or query lifecycle;
- managing caller-owned storage writes, table creation, partitioning, write modes, or storage options;
- starting or configuring Spark sessions;
- generating non-PySpark backends;
- accepting manual edits inside `generated/` as source of truth;
- optimizing source-order operations across hook boundaries;
- generating row-multiplying `join_many(...)` before the join semantics spec admits it;
- generating aggregations, windows, deduplication, and higher-order collection transforms before their specs exist;
- producing runtime telemetry under `traceability/`.

## Implementation Checklist

1. Use the backend capability interface selected from `target_backend` and `target_pyspark`.
2. Define generated path and module-name mapping from source modules.
3. Add shared PySpark execution recipe lowering as specified by `ExecutionSemanticContract.md`.
4. Generate package `__init__.py` files.
5. Generate schema modules from `SchemaDef.fields`.
6. Generate or expose runtime helpers for `assert_schema`, `project_schema`, and `HookInputs`.
7. Keep generated schema constants shape-only when future constraint metadata is added.
8. Share schema rendering/materialization rules with online execution.
9. Generate one transform class per `TransformPlan`.
10. Generate deterministic imports and ownership headers.
11. Render input validation at the start of `run(...)` from shared validation recipes.
12. Render `df`-based step blocks in source order from shared step recipes.
13. Render expression, filter, projection, join, hook, and validation recipes.
14. Generate hook source imports and `_impl` only when hooks exist.
15. Generate read-only hook input namespace only when `pass_inputs=True` exists.
16. Enforce performance guardrails before writing generated code.
17. Generate compiler provenance and static traceability when configured.
18. Generate all file contents in memory.
19. Write only changed files and format only changed files.
20. Support `--fail-on-diff` through temporary output comparison.
21. Add diagnostics with links to this specification and narrower semantic specs.
22. Add snapshot tests for generated schemas, transforms, runtime helpers, and traceability.
23. Add tests proving caller code can import generated schemas for reads and pre-write validation/projection.
24. Add tests proving online-materialized schemas match generated schema constants.
25. Add parity tests proving generated and online execution agree.

## Acceptance Criteria

The implementation is complete when tests prove:

- `structure compile` writes the default generated package layout.
- Generated package directories include importable package markers.
- Source schema modules map to generated schema modules deterministically.
- Source transform modules map to generated transform modules deterministically.
- Generated files contain stable ownership headers without timestamps or absolute paths.
- Generated schema constants use `SchemaDef.fields` order.
- Generated schema constants are importable as ordinary PySpark `StructType` values by caller code.
- Online-materialized schemas match generated schema constants for the same source schema.
- Primitive, decimal, array, map, nested struct, and inherited schemas render as Spark `StructType` values.
- Future data-quality constraint metadata does not alter existing generated schema constants.
- Generated transform class names are source class names suffixed with `Generated`.
- Generated constructors accept keyword-only `spark` and optional `ctx`.
- Generated `run(...)` methods accept keyword-only input DataFrames in declaration order.
- Hook-free transforms omit source transform imports and `_impl`.
- Hooked transforms import the source transform and call hooks on `_impl`.
- `HookInputs` is generated or imported only when a hook declares `pass_inputs=True`.
- `HookInputs` exposes original inputs as read-only attributes.
- Input schema validation renders at the start of `run(...)`.
- Intermediate and final validation placement matches validation policy.
- `project_output=True` validates, projects, then validates strictly.
- Projection-only transforms render explicit `select(...)` calls with aliases.
- Filters render as `.where(...)` before projection when source semantics allow it.
- Multiple filters preserve source order and boolean semantics.
- Expression helpers lower to PySpark Column expressions.
- Python literals lower to `F.lit(...)` where needed.
- Null-safe equality lowers to the configured PySpark syntax.
- Left and inner joins render explicit join calls with stable aliases.
- Composite joins preserve key-pair order.
- Broadcast hints apply to the right side of the join.
- Repeated joins of the same input produce deterministic aliases.
- Right-side join columns do not leak into output projection unless explicitly selected.
- Generated code contains no UDFs, Pandas UDFs, RDD operations, `collect`, `toPandas`, or row-wise maps in compiled
  paths.
- Generated output is byte-stable for identical source and configuration.
- Unchanged files are not rewritten by `structure compile`.
- Formatting runs only for changed files when formatting is enabled.
- `structure compile --fail-on-diff` detects added, removed, and modified generated files without changing
  `generated_dir`.
- Traceability files are deterministic and include inputs, steps, filters, joins, projections, hooks, validation points,
  target backend, and target capabilities.
- `execution_mode = "generated"` can import a generated class and run it through `GeneratedPySparkRunner`.
- Missing generated classes in generated mode produce the import-failure guidance from
  `docs/specifications/OnlineExecution.md`.
- `structure check` and `structure compile` run without PySpark, Java, Spark startup, a `SparkSession`, or a Spark
  cluster.
- Generated and online execution produce equivalent DataFrames for projection, filtering, expression helpers, joins,
  hooks, `pass_inputs=True`, validation, `schema_mode`, and `project_output`.

## Test Placement

Generator implementation tests belong under `tests/app/compiler/...` or the current compiler app test package
equivalent. Runtime parity tests that require Spark belong under `tests/app/runtime/...` or the current runtime test
package equivalent. Specification-backed user stories from `docs/dev/Specification.md` belong under `tests/specs/...`.
Tests that directly back this specification document belong under `tests/specifications/pyspark-code-generation/...`.

Recommended test groups:

- generated path and package mapping;
- schema module snapshots;
- transform module snapshots;
- runtime helper behavior;
- import minimization;
- hook and `HookInputs` generation;
- expression, filter, projection, and join rendering;
- validation placement;
- capability-specific syntax;
- performance guardrail checks;
- write-if-changed behavior;
- `--fail-on-diff` stale output detection;
- deterministic traceability output;
- Spark-free compiler command checks;
- online/generated parity scenarios.
