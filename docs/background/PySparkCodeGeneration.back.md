# PySpark Code Generation

PySpark code generation lowers Structure compiler IR into deterministic, readable Python modules that use PySpark
DataFrame and Column APIs. The generated modules are optional for ordinary runtime execution because execution
is the default, but they remain first-class artifacts for provenance, code review, debugging, snapshot tests, and
projects that deliberately choose `execution_mode = "generated"`.

The generator is a source-text emitter. It does not redefine transform semantics. Projection, filtering, expression
lowering, join aliasing, hook order, validation placement, schema projection, and performance guardrails agree with
PySpark execution.

Generated transform modules carry a deterministic fingerprint of the compiled artifact they render. Generated mode
checks it before execution and asks users to regenerate stale output rather than running it silently.

Shared semantics are owned by [ExecutionSemanticContract.md](ExecutionSemanticContract.back.md)). The generator renders
`PySparkExecutionPlan` recipes, or the local implementation equivalent, into source text. It owns imports, formatting,
file headers, generated paths, and readability. It must not make separate semantic choices that bypass the shared
recipes.

## Scope

This reference covers generated PySpark source shape and generator behavior for:

- generated schema modules;
- generated transform classes;
- generated runtime support modules needed by generated code;
- compiler provenance and static dataflow traceability files;
- deterministic imports, names, aliases, formatting, and write-if-changed behavior;
- backend capability selection for PySpark syntax;
- generated-code diagnostics and verification behavior.

Semantic behavior is covered by narrower references:

- public DSL and transform IR: [DSL.md](DSL.back.md));
- execution/generated-code execution parity: [ExecutionSemanticContract.md](ExecutionSemanticContract.back.md));
- execution and generated-code runtime selection: [Execution.md](Execution.back.md));
- schema model and Spark type mapping: [SchemaModel.md](SchemaModel.back.md));
- data quality constraint boundaries: [DataQualityConstraints.md](DataQualityConstraints.back.md));
- schema declaration syntax: [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.back.md));
- schema inheritance: [SchemaInheritance.md](SchemaInheritance.back.md));
- nullability and assignment compatibility: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md));
- join semantics: [JoinSemantics.md](JoinSemantics.back.md));
- CLI command behavior and stale generated output checks: [CLI.md](CLI.back.md));
- streaming constraints: [StreamingCompatibility.md](StreamingCompatibility.back.md));
- compatibility policy: [CompatibilityPolicy.md](CompatibilityPolicy.back.md)).

When this document overlaps those references, this document owns how already-decided semantics are rendered as
PySpark source text. The shared execution contract owns parity between execution and generated-code consumers. The
narrower reference owns feature behavior.

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
  -> generated/structure_generated/store/pyspark/schemas/order.py

src/orders/transforms/order.py
  -> generated/structure_generated/store/pyspark/transforms/order.py
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
target_profile = ">=3.5,<4.1"
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
- `target_profile` selects PySpark syntax through the backend capability registry.
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
    enriched = output(OrderEnriched)

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
- Generated transforms expose declared output names such as `result.published`, `result.accepted`, and
  `result["rejected"]`. There is no automatic `df` alias.
- Generated transform bodies keep one local DataFrame variable per source-order lane producer, so a branch step reads
  the frame named by its input lane rather than the immediately preceding step.
- Input parameter order follows source input declaration order.
- A source transform instance is created only when at least one hook exists.
- Hook-free generated classes must omit the source transform import and `_impl`
- The generated class must not inherit from the source transform class.
- Generated classes are owned by the compiler. Users must not subclass or edit generated classes.

Generated-code execution through `GeneratedPySparkRunner` imports this class, instantiates it with
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
- Field names in generated Spark `StructField` declarations use the field's Spark column name, which is `alias` when
  supplied and the Python field name otherwise.
- Execution materializes equivalent Spark schemas from the same `SchemaDef.fields` model instead of importing
  generated schema modules.
- Inherited fields are rendered in effective schema order after inheritance resolution.
- Spark type mapping follows [SchemaModel.md](SchemaModel.back.md)).
- Nested `struct(...)` fields render nested `T.StructType(...)` values.
- `array(...)` and `map(...)` preserve item, key, value, and nullability metadata.
- Structure metadata does not affect Spark `StructField` nullability.
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
  and a link to the relevant reference or troubleshooting document.
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

Generated transform methods use the current lane name as the DataFrame variable.

Rules:

- The first compiled step starts from the declared input DataFrame selected by source-order schema flow.
- After each step, the current lane variable contains that step's output DataFrame.
- Hooks receive the selected lane keyword and must return the new selected lane DataFrame.
- Joins may introduce temporary DataFrame variables named from stable aliases, such as `customers_df`.
- Avoid reusing input parameter names for aliased or projected temporary DataFrames.
- For a multi-result step method, emit one shared join/filter frame and use each output declaration name as the
  projected DataFrame variable, such as `accepted` and `audited`.
- Avoid hidden mutation. Each DataFrame operation should assign a resulting DataFrame to the current lane or a clearly named
  temporary.

Readable generated code is part of the contract. The generator may introduce short local variables when that makes a
join, hook, or complex expression easier to review.

## Step Shape

Each compiled step method renders as a contiguous generated code block.

Canonical order follows the shared source-order recipe:

1. raw hooks placed before the step boundary;
2. source-order filters and joins that must run before projection;
3. projection into the step output schema;
4. raw hooks placed after the step boundary;
5. schema validation and optional projection at raw hook boundaries.

Example:

```python
# Step method: normalize
orders = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
)

orders = self._impl.remove_negative_totals(orders=orders, spark=self.spark, ctx=self.ctx)
assert_schema(orders, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")
```

Rules:

- The step comment is stable and uses the source step method name.
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
  [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md)).
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
- Every selected expression is explicitly aliased to the output field's Spark column name.
- Aliases are schema-local: an input alias is used to read that input schema, and the target schema decides the
  projected output column name.
- Nested schema constructors assigned to `Struct(...)` fields render as `F.struct(...)` with child expressions aliased
  to the nested schema's Spark field names.
- Nested field references preserve path segments. Segments containing dots or backticks are escaped so aliases such as
  `alias="postal.code"` are treated as one nested field name.
- `SchemaClass.base(row)(...)` first maps inherited base fields from the base row, then applies explicit overrides and
  additions according to schema construction semantics.
- Extra columns are not preserved through compiled projection unless a hook with `project_output=False` is allowed to
  leave them and validation mode permits it.
- Duplicate output field names are compile errors before generation.

Projection is the boundary where generated code becomes reviewable: a reader must be able to see exactly which columns
enter the next step.

## Join Lowering

Joins lower according to [JoinSemantics.md](JoinSemantics.back.md)).

Canonical generated shape:

```python
orders = orders.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))
orders = orders.join(
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
- `"left"` lowers to Spark join type `"left"`.
- `"inner"` lowers to Spark join type `"inner"`.
- `"broadcast"` applies to the right side and may lower to `F.broadcast(right_df)`.
- Composite keys render key comparisons in IR order, combined with `&`.
- Null-safe key pairs render with the selected PySpark null-safe equality syntax.
- Right-side projection should carry only right-side key expressions and fields needed by downstream filters,
  projections, diagnostics, or traceability.
- The final projection after the join must remove duplicate and temporary right-side columns.

The generator must not silently deduplicate right-side rows for plain `lookup_join(...)`. Unproven uniqueness is a warning
or error owned by join compileability checks, not by generated code.

When `lookup_join(...)` carries an explicit `JoinDedupe` policy, generated code may reduce the right side before joining.
The PySpark recipe must use deterministic ordering, currently `row_number()` over a `Window.partitionBy(...)` built
from the right-side join keys and `orderBy(...)` built from the policy expression. It must not use arbitrary
`first(...)` or nondeterministic `dropDuplicates(...)`.

Selected-row helpers use the same visible ranking pattern on the current step frame. `latest_by(...)` and
`dedupe_latest_by(...)` render `row_number()` over `Window.partitionBy(...).orderBy(order.desc())`;
`earliest_by(...)` and `dedupe_earliest_by(...)` use ascending order. Generated code keeps rank `1` and drops the
temporary rank column before projection or output validation.

Projection window helpers render directly in `select(...)` expressions. `row_number(...)`, `rank(...)`,
`dense_rank(...)`, `lag(...)`, and `lead(...)` render as `F.<helper>(...).over(Window.partitionBy(...).orderBy(...))`.
Rolling metric helpers render as `F.sum(...)`, `F.avg(...)`, `F.min(...)`, or `F.max(...)` over the same ordered window
with `.rowsBetween(-preceding, 0)`.
Generated modules import `Window` whenever a join, selected-row operation, or projection expression needs it.

Exact duplicate cleanup from `distinct(relation)` or `drop_duplicates(relation)` renders `dropDuplicates([...])` using
all fields from that relation. Subset `drop_duplicates(field, ...)` renders `dropDuplicates(["field", ...])` using
Spark column names from the typed field expressions and infers relation scope when all fields come from one relation.
Empty `distinct()` and `drop_duplicates()` still render `dropDuplicates()` on the current active frame. Generated code
preserves source order: relation dedupe before a join prepares that join source; relation dedupe after a join applies to
the active joined frame. Generated code must not use keyed `dropDuplicates(...)` as a replacement for deterministic
selected-row dedupe when a specific representative row matters.

## Hook Lowering

Hooks are explicit runtime escape hatches and are called on the source transform instance.

Generated call for a single selected lane:

```python
orders = self._impl.remove_negative_totals(orders=orders, spark=self.spark, ctx=self.ctx)
```

Generated call with an additional selected input:

```python
orders = self._impl.compare_to_raw(orders=orders, customers=customers, spark=self.spark, ctx=self.ctx)
```

Rules:

- Generate `_impl = SourceTransform()` only when at least one hook exists.
- Raw hooks render at their source-order boundary.
- Adjacent raw hooks follow source order.
- Hooks always receive keyword arguments.
- Pass every DataFrame selected by the hook binding as a keyword argument.
- An `input(...)` selector passes the original declared input; a `lane(...)` selector passes the current lane.
- Hook return values become the new selected lane DataFrame.
- Hook output validation and optional projection follow the hook metadata.

Generated code does not inspect hook internals. Hook behavior is opaque to compiler traceability except for the declared
hook boundary.

`generated_code_options = ["embed_hooks"]` changes only generated layout. It AST-copies an eligible raw hook after
generated `run(...)`, removes `@raw`, and calls the copied method at the same boundary. The source transform is not
imported solely for embedded hooks. Supported dependencies are local imports, parameters, local values, builtins, and
`self.spark`/`self.ctx`; source globals, closures, `super()`, and other instance state fail with `GEN-E0903`. The body
remains an optimizer-opaque hook. Python UDFs require delegated source implementation unless `embed_udfs` is selected.

## Validation Placement

Generated validation must match execution.

Rules:

- Validate declared input DataFrames at the start of `run(...)`.
- Validate step method outputs when validation policy says to validate them.
- Validate raw hooks according to the hook's `schema_mode`.
- If `project_output=True`, validate with the hook schema mode, project to the declared output schema, then validate
  strictly.
- Validate the final returned DataFrame unless validation is disabled for that final step by policy.
- Validation uses generated schema constants and generated runtime helpers.
- Validation calls must include the schema display name and validation mode.

Example:

```python
assert_schema(orders, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="allow_extra_columns")
orders = project_schema(orders, ORDER_ENRICHED_SCHEMA)
assert_schema(orders, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
```

Disabling validation should remove the corresponding `assert_schema(...)` call, not call it with a no-op mode, unless a
future runtime helper contract explicitly chooses no-op modes.

## Capability Registry

PySpark API choices must go through the backend capability interface specified in
[BackendCapabilities.md](BackendCapabilities.back.md)).

Rules:

- Capability selection uses configured `target_backend` and `target_profile`.
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
- Normalize line endings for `--fail-on-diff` comparisons as specified by [CLI.md](CLI.back.md)).

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

Unsupported target example:

```text
CompileError BACKEND-E2402: Unsupported backend capability

Target:
  target_profile = "<3.4"

Problem:
  The PySpark generator has no capability profile for this target range.

Use:
  Set target_profile to a supported range such as ">=3.5,<4.1".

See docs/background/Execution.back.md
```

Generation failure example:

```text
CompileError GEN-E0902: Cannot render expression as PySpark

Transform:
  orders.transforms.order.EnrichOrders

Step method:
  normalize

Expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods are not part of the compileable expression IR.

Use:
  customer_id=lower(trim(order.customer_id))

See docs/background/DSL.back.md
```

Formatting failure example:

```text
CompileError GEN-E0903: Generated formatter failed

Generated path:
  generated/structure_generated/store/pyspark/transforms/order.py

Problem:
  The formatter could not parse the generated Python module.

Use:
  Run structure compile --profile and report this as a Structure generator bug.

See docs/background/PySparkCodeGeneration.back.md
```

## Generated Mode Import Failures

Generated code generation must support the generated-code execution diagnostics specified by
[Execution.md](Execution.back.md)).

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
- generating unsupported broad window, keyed deduplication, or advanced grouping forms before their specs exist;
- producing runtime telemetry under `traceability/`.
