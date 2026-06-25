# Symbolic Execution

## Purpose

Symbolic execution is the compiler phase that turns user-written compiled subtransform methods into backend-neutral
IR. It executes the method body with symbolic schema row proxies instead of real data, records filters, joins,
expressions, and output projection, and then hands a deterministic `StepPlan` to compileability checks, online
execution, generated PySpark emission, compiler provenance, and static dataflow traceability.

The purpose is not to run the user's pipeline in Python. The purpose is to let developers write readable schema-oriented
Python while preserving Spark optimizer visibility. Any source behavior that cannot be represented as Structure IR
must fail at compile time with a structured diagnostic instead of falling back to UDFs, row-wise callbacks, RDD
operations, or opaque generated code.

## Scope

This specification owns the compiler behavior for:

- symbolic row proxies for subtransform input rows;
- symbolic input scopes declared with `input(Structure)`;
- field reference capture;
- Python literal capture in expression positions;
- expression helper calls and `@expr_fn` expansion;
- `where(...)` operation capture;
- `join_one(...)` operation capture;
- schema constructor projection capture;
- schema base overlay expansion;
- active transform, subtransform, field, and source context tracking;
- unsupported-operation diagnostics;
- deterministic `StepPlan` construction.

Related specifications own detailed semantics for narrower topics:

- public DSL shape: `docs/specifications/DSL.md`;
- schema construction and base overlays: `docs/specifications/SchemaDeclarationSyntax.md`;
- schema inheritance and field origin: `docs/specifications/SchemaInheritance.md`;
- expression type and nullability checks: `docs/specifications/NullabilityAndTypeCoercion.md`;
- join condition, alias, and cardinality checks: `docs/specifications/JoinSemantics.md`;
- online lowering: `docs/specifications/OnlineExecution.md`;
- streaming checks: `docs/specifications/StreamingCompatibility.md`;
- CLI behavior and metrics: `docs/specifications/CLI.md`.

When this document overlaps with a narrower specification, this document owns how symbolic execution captures the
source event. The narrower specification owns final validity, type rules, backend capability, and runtime behavior.

## Compile Flow Position

Symbolic execution runs after discovery and schema inspection, and before compileability checks:

```text
load config
discover source modules
inspect schemas and transforms
symbolically execute subtransforms
build TransformPlan IR
run compileability checks
emit or execute target output
```

Rules:

- Discovery decides which classes and methods are compiled.
- Schema inspection provides `SchemaDef`, `FieldDef`, inheritance, and field-origin metadata.
- Symbolic execution must not decide backend-specific PySpark details.
- Compileability checks may reject IR created by symbolic execution.
- Online execution and generated code must consume the same IR.

## Canonical Example

Source:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())

    return OrderNormalized(
        id=order.id,
        customer_id=lower(trim(order.customer_id)),
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Symbolic result:

```text
StepPlan normalize
  input_schema: OrderRaw
  output_schema: OrderNormalized
  operations:
    Filter
      predicate: is_not_null(FieldRef(order.id))
    Project
      id <- FieldRef(order.id)
      customer_id <- lower(trim(FieldRef(order.customer_id)))
      total <- to_decimal(FieldRef(order.total), precision=12, scale=2)
```

The engine records expression shape, source order, scopes, schema fields, and source context. It does not evaluate real
rows, call Spark, or inspect live DataFrames.

## Public Source Forms

The v1 symbolic engine must support these source forms inside compiled subtransforms:

```python
order.id
lower(trim(order.customer_id))
where(order.id.is_not_null())
self.customers.join_one(on=self.customers.id == order.customer_id, how=Join.LEFT)
OrderNormalized(id=order.id)
OrderWithCustomer.base(order)(customer_name=customer.name)
```

The engine must also support literals accepted by `NullabilityAndTypeCoercion.md`:

```python
coalesce(order.total, "0")
coalesce(to_decimal(order.total, precision=12, scale=2), 0)
when(order.total.is_null(), 0).otherwise(order.total)
```

Public examples should use these forms. Source-level `F.col`, `F.lit`, PySpark `Column` methods, Python string methods
on symbolic expressions, and raw string column paths are not compiled-source forms in v1.

The symbolic source surface is intentionally curated. Structure should not add one thin wrapper for every PySpark
function. When a Spark capability becomes compiler-visible, define the smallest Structure-level operation family that
captures the intended data-pipeline meaning, add IR for that operation, and lower it through target recipes. For
example, future aggregation support should introduce aggregation and grouping semantics, while future array, map, and
higher-order support should introduce symbolic collection operations and symbolic callback rules. Rare or arbitrary
PySpark should stay in explicit hooks, where the compiler records an opaque boundary instead of pretending to understand
the body.

## Execution Model

For each compiled subtransform, the engine:

1. Creates a `SymbolicContext` for the transform class, subtransform method, input schema, output schema, configuration,
   metadata, and diagnostics.
2. Creates one symbolic driving-row proxy and one symbolic relation proxy for each additional schema parameter.
3. Creates symbolic input scopes for every declared transform input accessible through `self`.
4. Calls the user method with all symbolic parameters in declaration order.
5. Records `where(...)` and `join_one(...)` calls in source order as they occur.
6. Captures one returned schema construction as one projection, or a fixed tuple as ordered result projections.
7. Builds one deterministic `StepPlan`.
8. Discards the active context before moving to the next subtransform.

Rules:

- The active context must be thread-local or otherwise isolated so concurrent compilations cannot mix operations.
- Only one active subtransform context may receive `where(...)` and join events at a time.
- The engine must clear the active context in a `finally`-style cleanup path after successful or failed execution.
- Hooks are not executed.
- Private helper methods are ordinary Python and are unsupported when they try to manipulate symbolic expressions in
  ways the DSL cannot capture. Reusable expression logic should use `@expr_fn`.
- If user code performs side effects during symbolic execution, Structure is not required to undo them. Diagnostics
  should still guide developers toward pure compiled subtransforms or explicit hooks.

## Symbolic Context

`SymbolicContext` is the per-subtransform capture state.

It must contain at least:

```text
transform definition
subtransform definition
input schema definition
output schema definition
declared input scopes
recorded operations
recorded filters
recorded joins
current output field when building projection
source context stack
diagnostic collector
configuration snapshot
```

Rules:

- Operation order is append-only and follows source execution order.
- Source context stack entries may include helper name, schema constructor, output field, filter call, join call, and
  base overlay call.
- The context must preserve enough information to report transform, subtransform, output field, source expression, and
  suggested fix when available.
- The context should be immutable after `StepPlan` construction or treated as consumed.

## Transform Instance During Symbolic Execution

The compiler invokes subtransform methods on a transform implementation object. During symbolic execution:

- `self.<input_name>` returns a symbolic input scope for declared inputs.
- `self.<expr_helper_name>(...)` calls a class-local `@expr_fn` helper symbolically.
- Hook methods are ignored except for previously discovered metadata.
- Constructor-bound live DataFrames are not used.

Rules:

- Input scope access must not expose a live DataFrame API.
- Unknown `self` attributes use normal Python behavior unless the compiler can provide a clearer diagnostic.
- The transform object must not be given a Spark session, runtime `ctx`, or generated-code state during compilation.
- If a transform method tries to call `self.run(...)`, access runtime inputs, or perform runtime execution, it must fail
  as unsupported compiled-transform behavior.

## Row Proxies

A row proxy represents one symbolic row stream with a schema and a scope name.

Minimum row proxy metadata:

```text
schema
scope kind: current_row | input | joined | constructed
scope name
stable occurrence id
available fields
field nullability overrides
source location when available
```

Rules:

- Attribute access for a known schema field returns a `FieldRef`.
- Attribute access for an unknown field fails with a structured diagnostic.
- Field access order is not itself an operation. Only expressions using the field in filters, joins, or projections are
  recorded in the final step.
- The first proxy is the current row. Additional relation proxies become readable projection scopes only after
  `join_one(relation, ...)` records their relational relationship.
- A joined row proxy is returned by `join_one(...)` and owns the right-side joined fields.
- A constructed row proxy may be used for intermediate symbolic schema objects created inside a method, such as
  `flags = PublicationFlags(...)`.

## Field References

Field access produces `FieldRef` expression nodes.

Minimum `FieldRef` metadata:

```text
scope name
scope occurrence id
schema
field name
field path for nested fields
field type
field nullability
field origin
source expression text when available
source location when available
```

Rules:

- `order.customer_id` becomes a scoped reference, not a string column name.
- Nested struct field references should preserve path order when nested field access is supported.
- A field reference keeps the field's declared type and static nullability, adjusted by current narrowing facts.
- A joined scope from a left join makes right-side fields nullable as described by `JoinSemantics.md`.
- Generated aliases are target-layer concerns, but symbolic references must carry stable scope identity so target
  layers can produce deterministic aliases.

## Expression Capture

Every expression object created during symbolic execution must carry:

```text
kind
children
Structure type
static nullability
referenced scopes
source metadata when available
```

The v1 symbolic expression kinds are:

```text
FieldRef
Literal
CallExpr
BinaryExpr
BooleanExpr
CastExpr
WhenExpr
```

Rules:

- Python literals in expression positions become `Literal` nodes.
- Comparison operators on expressions create `BinaryExpr` or equivalent comparison nodes.
- `&`, `|`, and `~` on boolean expressions create boolean expression nodes.
- Python `and`, `or`, and `not` must fail because they ask Python for truthiness.
- Symbolic expressions must not implement truthiness. `if order.id:` and `order.id and order.customer_id` must raise
  diagnostics.
- Expression nodes must not import PySpark or store PySpark `Column` objects.
- Target-specific lowering metadata belongs in target layers, not in symbolic expression objects.

## Expression Helpers

Public DSL helpers such as `lower(...)`, `trim(...)`, `to_decimal(...)`, `coalesce(...)`, and `when(...)` create
symbolic call expressions when any argument is symbolic.

Rules:

- Helper calls preserve function identity, argument order, keyword arguments, result type, result nullability, and
  source context.
- Helper keyword arguments must be explicit IR data, not hidden Python closures.
- Helper validation may run during capture when the result type is needed immediately.
- Final compatibility checks may still reject helper calls after IR construction.
- Helper calls with only non-symbolic arguments may return ordinary Python values only when the public DSL explicitly
  allows it. Compiled expression positions should normalize accepted values to literals.

## `@expr_fn` Expansion

`@expr_fn` helpers are reusable compileable expression functions.

Rules:

- Calling an `@expr_fn` with symbolic arguments executes the helper body under a helper source context.
- The helper result must be a symbolic expression or a Python literal accepted in expression position.
- The engine must record the outer helper call identity for diagnostics and provenance.
- The engine may either inline the expanded expression into IR or preserve a `CallExpr` with expansion metadata, as long
  as online execution, generated code, traceability, and diagnostics agree.
- Class-local helpers declared without `self` must be callable through `self`.
- Recursive helpers are invalid in v1 unless a future spec defines recursion limits.
- Helper expansion should be cacheable when the helper identity, argument symbolic shapes, and keyword values are the
  same and caching cannot hide diagnostics or source context.

Diagnostic rule:

- When a helper is invalid, diagnostics should name the helper and call site before showing expanded internals.

## Filters

`where(predicate)` records a filter operation in the active symbolic context.

Rules:

- `where(...)` is valid only while symbolically executing a compiled subtransform.
- The predicate must be a symbolic boolean expression or a value accepted as one by the expression checker.
- Multiple `where(...)` calls remain separate recorded filter events until IR construction; IR may combine them with
  logical AND while preserving source order.
- A filter may reference only scopes available at the point where it is recorded.
- A filter recorded before a join cannot reference that joined scope.
- A filter recorded after a join may reference the joined scope.
- A filter with simple `field.is_not_null()` narrows that field for later expressions in the same subtransform.
- Narrowing facts do not cross hook boundaries unless a future spec adds explicit hook postconditions.

Minimum filter operation metadata:

```text
predicate expression
source order index
available scopes
narrowing facts
source context
```

## Joins

`join_one(...)` records a lookup join operation and returns a joined row proxy.

Rules:

- `join_one(...)` is valid only during symbolic execution.
- It may be called only on a declared input scope in v1.
- `on` and `how` are required.
- `hint` is optional.
- The `on` argument is captured as a symbolic expression.
- The engine records the join in source order before returning the joined scope.
- The joined scope occurrence id must be deterministic.
- Repeated joins of the same input must receive stable occurrence ids.
- Join condition validity, supported join types, null semantics, aliases, right-side projection, and uniqueness warnings
  are checked according to `JoinSemantics.md`.

Minimum join operation metadata:

```text
joined input name
joined input schema
join method: join_one
join type
optional hint
condition expression
joined scope occurrence id
source order index
source context
```

## Schema Construction

Calling a schema class inside a compiled subtransform captures a symbolic output record.

Rules:

- Positional arguments are rejected.
- Keyword names are schema field names.
- Unknown keyword names are errors.
- Missing fields are checked according to `SchemaDeclarationSyntax.md`.
- The returned object must preserve assignment expressions by target field name.
- Projection order follows output schema field order, not keyword argument order.
- Assignments must be type- and nullability-checked later according to `NullabilityAndTypeCoercion.md`.
- The final returned schema construction becomes the `Project` operation for the step.
- Intermediate schema constructions may produce constructed row proxies if assigned to local variables and used later.

Minimum projection assignment metadata:

```text
target schema
target field
source expression
assignment source context
field origin
```

## Base Overlay Construction

`SchemaClass.base(...)(...)` is shorthand for a projection that copies inherited fields from one or more source rows and
then applies explicit overrides.

Rules:

- For one direct schema base, `base(source)` receives one source row compatible with that base.
- For multiple direct schema bases, `base(source_a, source_b, ...)` receives one source row per direct base in schema
  declaration order.
- Field copying is based on inherited field origin, not fuzzy field-name matching.
- Extra fields on source rows are ignored.
- Explicit overrides win over copied fields.
- Locally declared target fields must be supplied explicitly.
- Target fields that locally override inherited fields must be supplied explicitly.
- Missing copied or explicit fields are errors.
- The symbolic result is the same projection shape as the equivalent explicit constructor.

Example:

```python
flags = PublicationFlags(
    has_promotion=order.promotion_name.is_not_null(),
)

return OrderPublished.base(order, flags)
```

Symbolic projection:

```text
Project
  fields inherited through OrderPublication <- order by field origin
  fields inherited through PublicationFlags <- flags by field origin
```

## StepPlan Construction

At the end of a successful subtransform, symbolic execution creates one `StepPlan`.

Minimum step IR:

```text
StepPlan
  name
  input_schema
  output_schema
  operations
  hooks_before
  hooks_after
  validate_output
  provenance
```

Operation order:

```text
before hooks metadata
filters and joins in source order
project from returned schema construction
after hooks metadata
validation metadata
```

Rules:

- The returned value must be a symbolic schema construction compatible with the subtransform return annotation.
- Exactly one final projection is allowed per subtransform in v1.
- A subtransform returning `None`, a DataFrame, a Python list, a dict, a generator, or an arbitrary object is invalid.
- A method may construct helper symbolic schema objects before the final return.
- The step must contain enough source and provenance data for diagnostics, explain output, and static dataflow.
- IR objects should be immutable or treated as immutable after construction.

## Unsupported Operations

Unsupported behavior must fail with structured compile errors. Required unsupported cases include:

- Python truthiness on symbolic expressions;
- Python `and`, `or`, and `not` for symbolic boolean logic;
- Python string methods on symbolic string expressions, such as `.strip()` or `.lower()`;
- arbitrary Python functions that are not public DSL helpers or `@expr_fn` helpers;
- source-level PySpark `Column` construction inside compiled subtransforms;
- raw string column paths;
- DataFrame methods inside compiled subtransforms;
- iteration over symbolic rows or expressions;
- indexing a symbolic row by string unless a future spec permits it;
- mutation of symbolic rows or expressions;
- async, generator, or coroutine subtransform behavior;
- returning non-schema symbolic values from compiled subtransforms;
- implicit UDF, Pandas UDF, RDD, `collect`, or `toPandas` lowering.

Rules:

- The engine should reject unsupported operations as close to the source operation as practical.
- Diagnostics must prefer a direct DSL replacement when one exists.
- Diagnostics should suggest `@expr_fn` for reusable expression logic.
- Diagnostics should suggest hooks only when arbitrary PySpark is genuinely appropriate.
- Configuration workarounds should be shown only when a safe setting exists. Unsupported compiled expressions do not
  have a configuration workaround.

## Diagnostics

Symbolic execution diagnostics must include:

- diagnostic code;
- severity;
- transform class when available;
- subtransform method when available;
- output field when available;
- helper, filter, join, or schema constructor context when relevant;
- source expression or source operation when available;
- problem;
- why it matters when not obvious;
- suggested DSL fix;
- `@expr_fn` helper fix when reuse is likely;
- hook workaround when arbitrary PySpark is appropriate;
- documentation link.

Unsupported expression example:

```text
CompileError DSL-E0401: Unsupported expression

Transform:
  EnrichOrders

Subtransform:
  normalize

Output field:
  OrderNormalized.customer_id

Source expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods cannot be compiled to Spark Column expressions.

Why this matters:
  Silent fallback to UDFs would reduce Spark optimizer visibility.

Use:
  customer_id=lower(trim(order.customer_id))

For reuse:
  @expr_fn
  def clean_id(value):
      return lower(trim(value))

Hook workaround:
  @after(normalize, lane=orders)
  def clean_customer_id(self, *, orders, spark, ctx):
      return orders.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

See docs/specifications/SymbolicExecution.md
```

Invalid return example:

```text
CompileError IR-E0503: Invalid subtransform return

Transform:
  EnrichOrders

Subtransform:
  normalize

Problem:
  Compiled subtransforms must return a Structure schema construction.

Use:
  return OrderNormalized(id=order.id, customer_id=order.customer_id)

See docs/specifications/SymbolicExecution.md
```

## Source Metadata

Symbolic execution should capture source metadata when practical:

- module path;
- source file;
- line and column;
- transform class;
- subtransform method;
- output field;
- helper call;
- filter call;
- join call;
- schema constructor argument;
- expression text.

Rules:

- Lack of source spans must not prevent compilation when the semantic source objects are valid.
- AST parsing should be avoided except for source spans, expression text, and diagnostics.
- Source metadata must not change semantic behavior.
- Source metadata should be stable enough for snapshot tests and compiler provenance.

## Import and Runtime Safety

Symbolic execution must preserve the no-Spark compiler contract.

Compiler phases must not:

- import PySpark;
- create a Spark session;
- start Java;
- contact a Spark cluster;
- inspect live DataFrames;
- read project data;
- write generated files as a side effect of symbolic execution.

Rules:

- Public DSL imports must be import-safe.
- User module import may execute normal Python class declarations, but symbolic execution happens only in compiler or
  runtime compile phases.
- Online execution may import PySpark after it receives IR and live DataFrames; that belongs to the runtime runner.
- Generated code emission may produce PySpark source text without importing PySpark.

## Determinism and Performance

Symbolic execution is on the developer feedback path for `structure check`, `structure compile`, online first run, and
CI. It must be deterministic and fast.

Rules:

- The same source and configuration must produce the same IR order, ids, aliases, diagnostics, and provenance paths.
- Do not rely on Python object identity where stable semantic ids are required.
- Avoid AST parsing on the hot path except for diagnostics.
- Avoid importing target backends.
- Avoid broad reflection after discovery has produced metadata.
- Cache safe expression helper expansions when it materially improves compile time.
- Do not cache results in a way that hides source context, warnings, or diagnostics.
- Keep IR immutable or effectively immutable after construction to support v2 incremental compile fingerprints.

Required compile metrics:

- symbolic execution time per transform;
- symbolic execution time per subtransform when detailed profiling is enabled;
- number of steps;
- number of expression nodes;
- number of recorded filters and joins;
- diagnostic count.

## Non-Goals

The following are outside v1 symbolic execution scope:

- arbitrary Python control-flow lowering into multiple dynamic DataFrame branches;
- subtransform branching and merging;
- row-multiplying `join_many(...)`;
- aggregations, windows, grouping sets, rollups, cubes, and deduplication;
- higher-order array and map transforms unless separately accepted by a v2 spec;
- automatic fallback to hooks;
- implicit UDF or Pandas UDF generation;
- source-level PySpark expressions inside compiled subtransforms;
- automatic data scans for uniqueness or validation;
- Spark Connect-specific symbolic behavior;
- non-PySpark backend-specific capture rules.

## Implementation Checklist

1. Define symbolic data objects for row proxies, input scopes, joined scopes, constructed rows, expressions, operations,
   and context.
2. Add active `SymbolicContext` management with safe cleanup and concurrency isolation.
3. Implement transform self binding so declared inputs resolve to symbolic input scopes.
4. Implement row proxy field access and unknown-field diagnostics.
5. Implement `FieldRef` expression metadata with scope, type, nullability, field origin, and source context.
6. Implement literal normalization for accepted Python literal values.
7. Implement expression operators for comparison, boolean `&`, boolean `|`, and boolean `~`.
8. Reject expression truthiness and unsupported Python boolean operators with actionable diagnostics.
9. Implement public DSL helper calls as symbolic `CallExpr` or equivalent expression nodes.
10. Implement `@expr_fn` module-level and class-local symbolic expansion.
11. Add recursion detection for expression helpers.
12. Implement `where(...)` active-context capture, source-order recording, predicate metadata, and simple nullability
    narrowing facts.
13. Implement symbolic input-scope `join_one(...)` capture and joined scope creation.
14. Implement schema constructor capture for output projection.
15. Implement `SchemaClass.base(...)(...)` expansion using schema field origins.
16. Build deterministic `StepPlan` objects from recorded operations and final projection.
17. Preserve source metadata for transform, subtransform, helper, filter, join, constructor, and output field contexts.
18. Add diagnostics linked to this specification and narrower specifications.
19. Add compile metrics for symbolic execution time and expression counts.
20. Ensure `structure check` and `structure compile` run symbolic execution without importing PySpark.

## Acceptance Criteria

The implementation is complete when tests prove:

- A projection-only subtransform produces one `StepPlan` with a `Project` operation.
- Field access on the current row produces scoped `FieldRef` expressions.
- Unknown field access fails with transform, subtransform, schema, field, and documentation link.
- Python literals in expression positions produce typed literal expressions.
- Public expression helpers produce expression IR without importing PySpark.
- Module-level `@expr_fn` helpers expand when called with symbolic arguments.
- Class-local `@expr_fn` helpers without `self` expand when called through `self`.
- Recursive `@expr_fn` helpers fail clearly.
- Python string methods on symbolic expressions fail with a direct DSL replacement suggestion.
- Python `and`, `or`, and `not` fail with suggestions for `&`, `|`, and `~`.
- Symbolic expressions are not truthy or falsey in Python.
- `where(...)` records filters in source order.
- Multiple `where(...)` calls preserve source order and combine as logical AND in IR or target lowering.
- `where(order.id.is_not_null())` narrows `order.id` for later projection in the same subtransform.
- `where(...)` outside active symbolic execution fails clearly.
- `join_one(...)` records a join operation and returns a joined scope.
- Repeated `join_one(...)` calls on the same input receive deterministic occurrence ids.
- A filter recorded before a join cannot reference the joined scope.
- A filter recorded after a join can reference the joined scope.
- Schema constructors record projection assignments by target field.
- Projection output order follows schema field order, not keyword order.
- `SchemaClass.base(source)(...)` expands to the same projection as the equivalent explicit constructor.
- `SchemaClass.base(source_a, source_b)(...)` maps sources to multiple direct bases by declaration order and field
  origin.
- Locally declared and locally overridden target fields in base overlays must be explicit.
- Returning `None`, a DataFrame, a list, a dict, or an arbitrary object from a compiled subtransform fails clearly.
- Hooks are not executed during symbolic execution.
- Symbolic execution diagnostics include transform, subtransform, output field when available, source expression, fix,
  and documentation link.
- The same source and configuration produce stable `StepPlan` ids, operation order, expression order, and diagnostics.
- `structure check` and `structure compile` can run symbolic execution without PySpark, Java, SparkSession, Spark
  startup, or a Spark cluster.
