# Symbolic Execution

Symbolic execution is the compiler phase that turns user-written compiled step methods into backend-neutral
IR. It executes the method body with symbolic schema row proxies instead of real data, records filters, joins,
expressions, and output projection, and then hands a deterministic `StepPlan` to compileability checks, execution,
generated PySpark emission, compiler provenance, and static dataflow traceability.

The purpose is not to run the user's pipeline in Python. The purpose is to let developers write readable schema-oriented
Python while preserving Spark optimizer visibility. Any source behavior that cannot be represented as Structure IR
must fail at compile time with a structured diagnostic instead of falling back to UDFs, row-wise callbacks, RDD
operations, or opaque generated code.

## Scope

This reference covers the compiler behavior for:

- symbolic row proxies for step-method input rows;
- symbolic input scopes declared with `input(Structure)`;
- field reference capture;
- Python literal capture in expression positions;
- expression helper calls and `@special(type="expr")` expansion;
- `where(...)` operation capture;
- `lookup_join(...)` operation capture;
- schema constructor projection capture;
- schema base overlay expansion;
- active transform, step method, field, and source context tracking;
- unsupported-operation diagnostics;
- deterministic `StepPlan` construction.

Related references own detailed semantics for narrower topics:

- public DSL shape: [DSL.md](DSL.back.md));
- schema construction and base overlays: [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.back.md));
- schema inheritance and field origin: [SchemaInheritance.md](SchemaInheritance.back.md));
- expression type and nullability checks: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md));
- join condition, alias, and cardinality checks: [JoinSemantics.md](JoinSemantics.back.md));
- execution lowering: [Execution.md](Execution.back.md));
- streaming checks: [StreamingCompatibility.md](StreamingCompatibility.back.md));
- CLI behavior and metrics: [CLI.md](CLI.back.md)).

When this document overlaps with a narrower reference, this document owns how symbolic execution captures the
source event. The narrower reference owns final validity, type rules, backend capability, and runtime behavior.

## Compile Flow Position

Symbolic execution runs after discovery and schema inspection, and before compileability checks:

```text
load config
discover source modules
inspect schemas and transforms
symbolically execute step methods
build TransformPlan IR
run compileability checks
emit or execute target output
```

Rules:

- Discovery decides which classes and methods are compiled.
- Schema inspection provides `SchemaDef`, `FieldDef`, inheritance, and field-origin metadata.
- Symbolic execution must not decide backend-specific PySpark details.
- Compileability checks may reject IR created by symbolic execution.
- Execution and generated code must consume the same IR.

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

The v1 symbolic engine must support these source forms inside compiled step methods:

```python
order.id
lower(trim(order.customer_id))
upper(trim(order.customer_id))
where(order.id.is_not_null())
lookup_join(on=order.customer_id == customer.id, how=Join.LEFT)
OrderNormalized(id=order.id)
OrderWithCustomer.base(order)(customer_name=customer.name)
```

The engine must also support literals accepted by `NullabilityAndTypeCoercion.md`:

```python
coalesce(order.total, "0")
coalesce(to_decimal(order.total, precision=12, scale=2), 0)
when(order.total.is_null(), 0).otherwise(order.total)
when(order.total >= 1000, "large").otherwise("standard")
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

For each compiled step method, the engine:

1. Creates a `SymbolicContext` for the transform class, step method, input schema, output schema, configuration,
   metadata, and diagnostics.
2. Creates one symbolic driving-row proxy and one symbolic relation proxy for each additional schema parameter.
3. Creates symbolic input scopes for every declared transform input accessible through `self`.
4. Calls the user method with all symbolic parameters in declaration order.
5. Records `where(...)` and `lookup_join(...)` calls in source order as they occur.
6. Captures one returned schema construction as one projection, or a fixed tuple as ordered result projections.
7. Builds one deterministic `StepPlan`.
8. Discards the active context before moving to the next step method.

Rules:

- The active context must be thread-local or otherwise isolated so concurrent compilations cannot mix operations.
- Only one active step-method context may receive `where(...)` and join events at a time.
- The engine must clear the active context in a `finally`-style cleanup path after successful or failed execution.
- Hooks are not executed.
- Private helper methods are ordinary Python and are unsupported when they try to manipulate symbolic expressions in
  ways the DSL cannot capture. Reusable expression logic should use `@special(type="expr")`.
- If user code performs side effects during symbolic execution, Structure is not required to undo them. Diagnostics
  should still guide developers toward pure compiled step methods or explicit hooks.

## Symbolic Context

`SymbolicContext` is the per-step-method capture state.

It must contain at least:

```text
transform definition
step-method definition
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
- The context must preserve enough information to report transform, step method, output field, source expression, and
  suggested fix when available.
- The context should be immutable after `StepPlan` construction or treated as consumed.

## Transform Instance During Symbolic Execution

The compiler invokes step methods on a transform implementation object. During symbolic execution:

- `self.<input_name>` returns a symbolic input scope for declared inputs.
- `self.<expr_helper_name>(...)` calls a class-local `@special(type="expr")` helper symbolically.
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
  `lookup_join(...)` records their relational relationship.
- `lookup_join(...)` returns or updates a relation proxy whose fields read from the joined right-side scope.
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
- Nested field references carry path segments separately from rendered Spark names so aliases containing dots remain one
  field segment.
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
- Comparison operators `==`, `!=`, `<`, `<=`, `>`, and `>=` on expressions create `BinaryExpr` or equivalent
  comparison nodes.
- Basic row-local arithmetic `+`, `-`, and `*` on expressions creates binary expression nodes.
- `&`, `|`, and `~` on boolean expressions create boolean expression nodes.
- Python `and`, `or`, and `not` must fail because they ask Python for truthiness.
- Symbolic expressions must not implement truthiness. `if order.id:` and `order.id and order.customer_id` must raise
  diagnostics.
- Expression nodes must not import PySpark or store PySpark `Column` objects.
- Target-specific lowering metadata belongs in target layers, not in symbolic expression objects.

## Expression Helpers

Public DSL helpers such as `lower(...)`, `upper(...)`, `trim(...)`, `to_decimal(...)`, `coalesce(...)`, and
`when(...).otherwise(...)` create symbolic expressions when any argument is symbolic.

Rules:

- Helper calls preserve function identity, argument order, keyword arguments, result type, result nullability, and
  source context.
- Helper keyword arguments must be explicit IR data, not hidden Python closures.
- Helper validation may run during capture when the result type is needed immediately.
- Final compatibility checks may still reject helper calls after IR construction.
- Helper calls with only non-symbolic arguments may return ordinary Python values only when the public DSL explicitly
  allows it. Compiled expression positions should normalize accepted values to literals.

## `@special(type="expr")` Expansion

`@special(type="expr")` helpers are reusable compileable expression functions.

Rules:

- Calling an `@special(type="expr")` with symbolic arguments executes the helper body under a helper source context.
- The helper result must be a symbolic expression or a Python literal accepted in expression position.
- The engine must record the outer helper call identity for diagnostics and provenance.
- The engine may either inline the expanded expression into IR or preserve a `CallExpr` with expansion metadata, as long
  as execution, generated code, traceability, and diagnostics agree.
- Class-local helpers declared without `self` must be callable through `self`.
- Recursive helpers are invalid in v1 unless a future spec defines recursion limits.
- Helper expansion should be cacheable when the helper identity, argument symbolic shapes, and keyword values are the
  same and caching cannot hide diagnostics or source context.

Diagnostic rule:

- When a helper is invalid, diagnostics should name the helper and call site before showing expanded internals.

## Filters

`where(predicate)` records a filter operation in the active symbolic context.

Rules:

- `where(...)` is valid only while symbolically executing a compiled step method.
- The predicate must be a symbolic boolean expression or a value accepted as one by the expression checker.
- Multiple `where(...)` calls remain separate recorded filter events until IR construction; IR may combine them with
  logical AND while preserving source order.
- A filter may reference only scopes available at the point where it is recorded.
- A filter recorded before a join cannot reference that joined scope.
- A filter recorded after a join may reference the joined scope.
- A filter with simple `field.is_not_null()` narrows that field for later expressions in the same step method.
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

`lookup_join(...)` records a lookup join operation and returns a relation proxy with joined-field access.

Rules:

- `lookup_join(...)` is valid only during symbolic execution.
- The documented form stays bare when `on` references exactly one unjoined declared input scope or schema relation
  parameter.
- Legacy explicit-selection overloads, when used, accept only declared input scopes or schema relation parameters in
  v1.
- Member joins such as `self.customers.lookup_join(...)` are rejected with migration guidance.
- `on` and `how` are required.
- `hint` is optional.
- The `on` argument is captured as a symbolic expression.
- The engine records the join in source order before returning the relation proxy.
- For schema relation parameters and cached class input scopes, the symbolic proxy is updated after `lookup_join(...)` so
  later field access reads the joined scope even when the return value is not assigned.
- Inferred and explicit joins must append equivalent ordered join operations.
- The joined scope occurrence id must be deterministic.
- Repeated joins of the same input must receive stable occurrence ids.
- Join condition validity, supported join types, null semantics, aliases, right-side projection, and uniqueness warnings
  are checked according to `JoinSemantics.md`.

Minimum join operation metadata:

```text
joined input name
joined input schema
join operation: lookup_join
join type
optional hint
condition expression
joined scope occurrence id
source order index
source context
```

## Schema Construction

Calling a schema class inside a compiled step method captures a symbolic output record.

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
- A schema construction assigned to a `Struct(...)` field becomes a nested struct expression, preserving the nested
  schema identity and child assignments in nested schema field order.

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

At the end of a successful step method, symbolic execution creates one `StepPlan`.

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
raw hooks metadata
filters and joins in source order
project from returned schema construction
raw hooks metadata
validation metadata
```

Rules:

- The returned value must be a symbolic schema construction compatible with the step method return annotation.
- Exactly one final projection is allowed per step method in v1.
- A step method returning `None`, a DataFrame, a Python list, a dict, a generator, or an arbitrary object is invalid.
- A method may construct helper symbolic schema objects before the final return.
- The step must contain enough source and provenance data for diagnostics, explain output, and static dataflow.
- IR objects should be immutable or treated as immutable after construction.

## Unsupported Operations

Unsupported behavior must fail with structured compile errors. Required unsupported cases include:

- Python truthiness on symbolic expressions;
- Python `and`, `or`, and `not` for symbolic boolean logic;
- Python string methods on symbolic string expressions, such as `.strip()` or `.lower()`;
- arbitrary Python functions that are not public DSL helpers or `@special(type="expr")` helpers;
- source-level PySpark `Column` construction inside compiled step methods;
- raw string column paths;
- DataFrame methods inside compiled step methods;
- iteration over symbolic rows or expressions;
- indexing a symbolic row by string unless a future spec permits it;
- mutation of symbolic rows or expressions;
- async, generator, or coroutine step method behavior;
- returning non-schema symbolic values from compiled step methods;
- implicit UDF, Pandas UDF, RDD, `collect`, or `toPandas` lowering.

Rules:

- The engine should reject unsupported operations as close to the source operation as practical.
- Diagnostics must prefer a direct DSL replacement when one exists.
- Diagnostics should suggest `@special(type="expr")` for reusable expression logic.
- Diagnostics should suggest hooks only when arbitrary PySpark is genuinely appropriate.
- Configuration workarounds should be shown only when a safe setting exists. Unsupported compiled expressions do not
  have a configuration workaround.

## Diagnostics

Unsupported expression example:

```text
CompileError DSL-E0401: Unsupported expression

Transform:
  EnrichOrders

Step method:
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
  @special(type="expr")
  def clean_id(value):
      return lower(trim(value))

Hook workaround:
  @raw(inout=lane(orders) | lane(orders))
  def clean_customer_id(self, *, orders, spark, ctx):
      return orders.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

See docs/background/DSL.back.md
```

Invalid return example:

```text
CompileError IR-E0503: Invalid step method return

Transform:
  EnrichOrders

Step method:
  normalize

Problem:
  Compiled step methods must return a Structure schema construction.

Use:
  return OrderNormalized(id=order.id, customer_id=order.customer_id)

See docs/background/DSL.back.md
```

## Source Metadata

Symbolic execution should capture source metadata when practical:

- module path;
- source file;
- line and column;
- transform class;
- step method;
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
- Execution may import PySpark after it receives IR and live DataFrames; that belongs to the runtime runner.
- Generated code emission may produce PySpark source text without importing PySpark.

## Determinism and Performance

Symbolic execution is on the developer feedback path for `structure check`, `structure compile`, first execution run, and
CI. It must be deterministic and fast.

Rules:

- The same source and configuration must produce the same IR order, ids, aliases, diagnostics, and provenance paths.
- Do not rely on Python object identity where stable semantic ids are required.
- Avoid AST parsing on the hot path except for diagnostics.
- Avoid importing target backends.
- Avoid broad reflection after discovery has produced metadata.
- Cache safe expression helper expansions when it materially improves compile time.
- Do not cache results in a way that hides source context, warnings, or diagnostics.
- Keep IR immutable or effectively immutable after construction to support future incremental compile fingerprints.

Required compile metrics:

- symbolic execution time per transform;
- symbolic execution time per step method when detailed profiling is enabled;
- number of steps;
- number of expression nodes;
- number of recorded filters and joins;
- diagnostic count.

## Non-Goals

The following are outside v1 symbolic execution scope:

- arbitrary Python control-flow lowering into multiple dynamic DataFrame branches;
- step method branching and merging;
- aggregations, broad windows, grouping sets, rollups, cubes, and general-purpose deduplication;
- higher-order array and map transforms unless separately accepted by a v2 spec;
- automatic fallback to hooks;
- implicit UDF or Pandas UDF generation;
- source-level PySpark expressions inside compiled step methods;
- automatic data scans for uniqueness or validation;
- Spark Connect-specific symbolic behavior;
- non-PySpark backend-specific capture rules.
