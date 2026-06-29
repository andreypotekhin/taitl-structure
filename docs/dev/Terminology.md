# Terminology

Structure is an IR-first runtime/compiler toolkit.
For exact behavior, see the specifications under `docs/specifications/`.

## General flow

The common flow is:

```text
Structure DSL source code
  -> discovery and inspection
  -> symbolic execution
  -> backend-neutral IR
  -> compileability and capability checks
  -> PySpark target recipes
  -> online execution or generated PySpark
```

DSL and IR describe transform meaning. PySpark-specific choices belong behind the target boundary.

## DSL

The DSL is the public Python authoring surface exported from `structure`. It covers schemas, transforms, expression
helpers, filters, joins, hooks, validation policy, and runtime invocation.

The DSL is not a general wrapper around PySpark. A DSL feature is compiler-visible only when Structure can capture it,
represent it in IR, check it, and lower it to optimizer-visible target operations.

Example source shape:

```python
@transform
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    enriched = output(OrderEnriched)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized(...)

    @transform(input=normalized, output=enriched)
    def publish(self, order: OrderNormalized) -> OrderEnriched:
        return OrderEnriched.base(order)(...)
```

### Schema

A schema is a `Structure` subclass that describes a row contract: field names, order, types, nullability, inheritance, and Structure metadata such as primary keys.

Schemas are used for input rows, intermediate rows, and output rows. 

A schema is not a Spark DataFrame and does not contain data.

Example:

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=True)
    total = field(Decimal(12, 2), nullable=True)
```

### Field

A field is one named column in a schema. 

Field metadata is the source of type, nullability, field order, generated Spark schema shape, projection checks, and many diagnostics.

### Transform

A transform is a `Transform` subclass marked with `@transform`. It declares input DataFrames, output results, lanes, subtransform methods, expression helpers, and hooks.

A transform instance created with `EnrichOrders(orders=df)` is a deferred invocation that stores runtime inputs until `.run(session)` is called.

Example:

```python
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
enriched_df = result.enriched
```

### Input

An input is a class-level `input(Schema)` declaration on a transform. The attribute name becomes the runtime input name, generated `run(...)` parameter name, hook input namespace attribute, and source scope name.

During symbolic execution, `self.orders` resolves to a symbolic input scope. During runtime, the invocation stores the actual DataFrame under the same declared name.

Example:

```python
orders = input(OrderRaw)
customers = input(Customer)
```

### Output

An output is a class-level `output(Schema)` declaration. Outputs are the public result names returned from
`TransformResult`, such as `result.enriched` or `result["enriched"]`.

Output declarations are part of the transform contract. 

Example:

```python
accepted = output(OrderAccepted)
rejected = output(OrderRejected)
```

### Lane

A lane is an intermediate DataFrame stream inside a transform. Lanes let a transform identify internal funnel states, branch outputs, or disambiguate repeated schemas.

Lanes are neither constructor inputs nor public transform results. They are internal flow identificators used by method-level binding, hooks, IR, online execution, and generated code.

Example:

```python
normalized = lane(OrderNormalized)
with_customer = lane(OrderWithCustomer)
```

### Subtransform

A subtransform is a public instance **method** within Transform class, returning a `Structure` or schemas tuple. The compiler symbolically executes subtransforms in the order of their appearance in the source.

The first schema parameter is the driving row. Additional schema parameters are symbolic relations that must be joined before their fields are used in filters or projections.

Example:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(id=order.id, ...)
```

### Binding of Inputs and Outputs

Method-level `@transform(input=...)`, `@transform(output=...)`, and `@transform(inout=...)` select which declared input, lane, or output a subtransform consumes or writes.

Binding is optional: most single-lane transforms rely on inference. Explicit binding is used for repeated schemas, branching, funnel lanes, and cases where a lane intentionally shadows an original input name.

Example:

```python
@transform(input=lane(normalized), output=enriched)
def add_product(self, order: OrderNormalized) -> OrderEnriched:
    return OrderEnriched.base(order)(...)
```

### Expressions

In the context of this library, an expression is a compiler-visible symbolic value, such as a field reference, literal, comparison, boolean expression, cast, conditional, or helper call.

Expressions carry Structure type, nullability, referenced scopes, and source context. They do not contain PySpark `Column` objects.

Example:

```python
lower(trim(order.customer_id)) == "c-001"
```

### Expression Helper

An expression helper function is a reusable compiler-visible function marked with `@expr_fn`. When called with symbolic arguments, the helper is expanded as expression IR.

Expression helper functions are Structure's preferred way to use expression logic while keeping it visible to the compiler checks, traceability, online execution, and generated code.

Example:

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

### Filter

A filter is recorded with `where(predicate)` inside a compiled subtransform method. Multiple filters preserve source order and are semantically combined with logical AND where legal.

Filters are operations in IR, not immediate DataFrame calls. A filter can reference only scopes available at the point where it is recorded.

Example:

```python
where(order.id.is_not_null())
where(to_decimal(order.total, precision=12, scale=2) >= 0)
```

### Join

A join is a symbolic relationship between the current row and a declared input. In v1, the main supported form is `join_one(...)`, which represents a lookup-style join.

A join creates a joined scope. Fields from that scope can be used in later filters or in the returned output schema.

Example:

```python
def add_customer(self, order: OrderRaw, customer: Customer) -> OrderWithCustomer:
    customer = join_one(
        customer,
        on=order.customer_id == customer.id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )
    return OrderWithCustomer.base(order)(customer_name=customer.name)
```

### Hook

A hook is an explicit PySpark escape hatch for adding arbitrary (PySpark) code

Hooks are declared as `@before(...)` or `@after(...)`. 

Hooks are opaque compiler boundaries. Structure validates hook metadata and signatures, preserves hook order, records the boundary in IR and traceability, and calls the hook in execution. It does not inspect the
hook body as compiler-visible logic.

Example:

```python
@after(normalize, lane=orders, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, orders, spark, ctx):
    return published.withColumn("_checked", F.lit(True))
```

### Validation Policy

Validation policy decides where Structure checks DataFrame schema: inputs, intermediate outputs, hook outputs, and final outputs. 

Policy is affected by project configuration, transform-level settings and method-level overrides.

### StructureSession

`StructureSession` is the runtime session. It owns the caller-supplied Spark session, optional hook context, library configuration, execution mode, target backend, runner selection, and optional plan cache.

The session does not own Spark lifecycle, orchestration lifecycle, reads, writes, or streaming query management.

Example:

```python
session = StructureSession(spark=spark, ctx=ctx)
result = session.run(EnrichOrders(orders=orders_df))
enriched_df = result.enriched
```

## Symbolic Execution

Symbolic execution is the compiler phase that runs compiled subtransform methods with symbolic row objects instead of real rows or DataFrames.

Its job is to capture source semantics: field references, expressions, filters, joins, and output projection. 

It is not a Python execution engine for data.

Example capture:

```text
Source:
  where(order.id.is_not_null())
  return OrderNormalized(id=order.id, customer_id=clean_id(order.customer_id))

Captured:
  Filter(is_not_null(FieldRef(order.id)))
  Project(
    id <- FieldRef(order.id)
    customer_id <- clean_id(FieldRef(order.customer_id))
  )
```

### Symbolic Context

A symbolic context is the active per-subtransform capture state. It records the transform, method, defined scopes, operations, source context, diagnostics, and configuration snapshot.

### Row Proxy

A row proxy represents a symbolic row with a schema and scope identity. Attribute access on a known field returns a field reference. Unknown fields produce error/warning diagnostics.

The current-row proxy is the main subtransform parameter. Joined and constructed row proxies represent later symbolic scopes.

Example:

```python
order.id  # FieldRef(scope="order", field="id")
```

### Input Scope

An input scope represents a declared transform input during symbolic execution. It is accessible through `self.<input>` and is the source for `join_one(...)` in v1.

Input scopes are not DataFrames and do not expose a live DataFrame API.

### Joined Scope

A joined scope is created by a symbolic join. It owns the right-side fields made available by that join and carries stable occurrence metadata for repeated joins of the same input.

Joined scopes are essential for deterministic aliasing, nullability adjustment, diagnostics, and traceability.

Example:

```python
customer = join_one(self.customers, on=order.customer_id == self.customers.id)
customer.tier  # FieldRef(scope="customers#1", field="tier")
```

### Constructed Row

A constructed row is the symbolic result of calling a schema constructor or schema base overlay inside a subtransform.
The final constructed row returned by the method becomes the projection for that step.

Example:

```python
OrderWithCustomer.base(order)(customer_name=customer.name)
```

### Field Reference

A field reference is an expression node pointing at a scoped schema field, such as `order.customer_id`. It records the
owning scope, field path, type, nullability, field origin, and source context when available.

Field references are never raw string column paths in compiler-visible source.

### Operation Capture

Operation capture is the act of recording source events as IR operations. `where(...)` records filters,
`join_one(...)` records joins, and schema construction records projection assignments.

Capture preserves source order. Later passes may combine or render operations only when the observable semantics stay
the same.

### Unsupported Operation

An unsupported operation is source behavior that Structure cannot safely represent in IR. Examples include Python
truthiness on symbolic expressions, Python string methods on symbolic fields, source-level PySpark `Column` objects,
DataFrame methods inside compiled subtransforms, row-wise maps, and implicit UDFs.

Unsupported operations fail with diagnostics instead of falling back to opaque Python execution.

## Intermediate Representation (IR)

The intermediate representation, or IR, is the backend-neutral compiler contract between DSL source semantics and
execution targets.

IR describes what the transform means. It must not contain live Spark sessions, DataFrames, PySpark Columns, runtime
hook return values, file handles, timestamps, memory addresses, or generated source text.

Sketch:

```text
TransformPlan EnrichOrders
  inputs: orders: OrderRaw, customers: Customer
  steps:
    normalize: OrderRaw -> OrderNormalized
      Filter(...)
      Project(...)
    add_customer: OrderNormalized -> OrderWithCustomer
      Join(...)
      Project(...)
  outputs: enriched: OrderEnriched
```

### TransformPlan

`TransformPlan` represents one compiled transform class. It contains ordered inputs, ordered steps, ordered outputs,
validation policy, streaming policy, provenance, dataflow records, capability metadata, and diagnostics.

It is the semantic source of truth for online and generated execution.

### InputPlan

`InputPlan` represents a declared transform input in IR: name, schema, declaration order, source anchor, and stable id.

It does not record the runtime DataFrame.

Example:

```text
InputPlan(name="orders", schema=OrderRaw, ordinal=0)
```

### StepPlan

`StepPlan` represents one compiled subtransform method: source-order position, input and output lanes, schema
boundaries, scopes, operations, hooks, validation policy, and source anchor.

For multi-result subtransforms, one step can share joins and filters before producing ordered result projections.

Example:

```text
StepPlan(name="normalize", input_lane="orders", output_lane="orders")
```

### OutputPlan

`OutputPlan` represents a public transform result: declared name, schema, declaration order, and the source lane that
materializes the result.

### Scope

A scope identifies which row stream owns a field reference. Common scope kinds are input, current, joined, and
projected.

Scopes let Structure distinguish fields with the same name across current rows and joined inputs without relying on
unqualified column strings.

### Operation IR

Operation IR records transform actions such as filter, project, join, hook call, and schema validation.

Operation nodes preserve order, reads, writes, source anchors, streaming classification, and stable ids.

Example:

```text
Filter(predicate=is_not_null(FieldRef(scope="orders", field="id")))
Project(assignments=[id <- FieldRef("orders.id")])
```

### Expression IR

Expression IR records field references, literals, helper calls, binary comparisons, boolean expressions, casts, and
conditionals.

Expression IR is checked and lowered later. It is not backend-specific PySpark syntax.

Example:

```text
CallExpr(function="lower", args=[CallExpr(function="trim", args=[FieldRef("orders.customer_id")])])
```

### Source Anchor

A source anchor connects an IR node back to user code: module, qualified name, project-relative path, line, column, and
display text when available.

Missing source spans should reduce diagnostic precision, not prevent valid compilation.

### Compiler Provenance

Compiler provenance maps source nodes to IR nodes and, when generation runs, to generated nodes. It is compile-time
metadata used for explanation, debugging, and review.

It is not runtime telemetry and does not contain row counts, Spark application ids, cluster details, or timing from a
Spark job.

### Static Dataflow Traceability

Static dataflow traceability records inferred dependencies from IR without running Spark jobs. Projection assignments,
filters, joins, hooks, and validation points produce records at transform, table, and column levels as supported.

Hook boundaries are marked opaque.

### Capability Metadata

Capability metadata records target backend and version information used by backend capability checks. For v1, the main
target is PySpark.

Capability checks happen before online execution or code generation so unsupported operations fail early.

### Determinism

Determinism means identical source, configuration, Structure version, and target capabilities produce identical IR
order, stable ids, aliases, diagnostics, traceability, and generated output.

Determinism is required for `--fail-on-diff`, snapshot tests, provenance, and future incremental compilation.

### Immutability

IR is intended to be immutable or treated as immutable after construction. Later analysis passes should produce
annotated copies or side reports rather than mutating shared plans in place.

This keeps parallel rendering, caching, and deterministic diagnostics tractable.

## Lowering

Lowering is the process of turning a higher-level semantic representation into a lower-level target representation.

In Structure, there are two important lowering steps:

- backend-neutral IR lowers to shared PySpark execution recipes;
- those recipes are either interpreted online or rendered as generated PySpark source.

Lowering must not invent semantics. It must implement checked IR.

Example:

```text
FieldRef("orders.customer_id")
  -> PySparkExpressionRecipe(field_reference=("orders", "customer_id"))
  -> F.col("orders.customer_id")
```

### Target Boundary

The target boundary is where backend-neutral Structure semantics become backend-specific execution choices. PySpark
API spelling, aliases, type strings, join hints, and version-specific choices belong here.

DSL objects and generic IR should not contain PySpark implementation details.

### PySpark Execution Plan

The PySpark execution plan is the shared target-level recipe model consumed by both online PySpark execution and the
generated PySpark emitter.

It decides expression mapping, filter order, projection field order, join aliases, hook order, validation placement,
literal typing, and guardrails once.

Example:

```text
PySparkStepRecipe normalize
  shared_operations: where(...)
  results:
    orders: select(id, customer_id, total)
```

### Online Lowering

Online lowering turns checked IR plus PySpark capabilities into target recipes, then the online runner interprets those
recipes with live PySpark DataFrame and Column APIs at runtime.

Online execution does not write generated files and does not execute generated source text.

Example:

```text
OnlinePySparkRunner interprets PySparkStepRecipe with live DataFrames.
```

### Generated Lowering

Generated lowering renders the shared PySpark recipes as deterministic Python source files under the configured
generated package.

Generated source is optional for ordinary execution, but first-class for review, provenance, debugging, snapshot tests,
and projects configured with `execution_mode = "generated"`.

Example:

```python
orders = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
)
```

### Expression Lowering

Expression lowering turns expression IR into PySpark `Column` expressions. Field references become qualified or
unqualified `F.col(...)` calls according to scope needs. Helper calls become selected PySpark functions.

Unsupported expression kinds must fail before rendering or runtime execution.

Example:

```text
lower(trim(FieldRef("orders.customer_id")))
  -> F.lower(F.trim(F.col("customer_id")))
```

### Filter Lowering

Filter lowering renders `where(...)` operations as PySpark `.where(...)` calls or equivalent online recipe steps.

Filters may be combined only when doing so preserves source semantics and does not cross joins or hooks illegally.

Example:

```text
where(order.id.is_not_null())
  -> orders.where(F.col("id").isNotNull())
```

### Projection Lowering

Projection lowering turns schema construction into explicit DataFrame selection. Every output field is selected in
schema order and aliased to the declared field name.

There is no implicit carry-through in compiled projection. Base overlays expand into explicit assignments.

Example:

```text
OrderNormalized(id=order.id, customer_id=clean_id(order.customer_id))
  -> select(F.col("id").alias("id"), ... )
```

### Join Lowering

Join lowering turns symbolic join IR into PySpark joins with stable aliases, supported join type spelling, optional
hints, ordered key pairs, and right-side field projection.

`join_one(...)` lowering must not silently deduplicate right-side rows.

Example:

```text
join_one(self.customers, on=order.customer_id == self.customers.id, how=Join.LEFT)
  -> orders.alias("order_normalized").join(customers.alias("customers"), ..., "left")
```

### Hook Lowering

Hook lowering emits or executes calls to source transform hook methods. Hooks receive keyword arguments for selected
lanes, optional original input namespace, `spark`, and `ctx`.

The hook body remains opaque. Validation and projection around hook outputs are represented by hook metadata and
validation recipes.

Example:

```python
orders = self._impl.add_quality_columns(orders=orders, spark=self.spark, ctx=self.ctx)
```

### Validation Lowering

Validation lowering places schema checks at the same points in online and generated execution: inputs, intermediate
outputs, hook outputs, projections after `project_output=True`, and final outputs according to policy.

### Backend Capability Check

Backend capability checks verify that the selected target backend and version range can support the checked IR and
target recipes.

For PySpark, this isolates version-specific API choices from DSL and generic compiler phases.

### Performance Guardrail

A performance guardrail is a rule that prevents compiled paths from becoming optimizer-invisible. Compiled lowering
must not introduce Python UDFs, Pandas UDFs, RDD operations, `collect`, `toPandas`, row-wise maps, or hidden local
materialization.

Arbitrary PySpark belongs in explicit hooks, where the opacity is visible.

### Online/Generated Parity

Online/generated parity means online execution and generated execution produce the same observable DataFrame semantics
from the same checked `TransformPlan`.

Parity covers projection order, filters, expressions, joins, hooks, validation placement, schema projection, result
shape, and diagnostics for unsupported cases.

## Related Documents

- [Concepts.md](Concepts.md): concept-test coverage map.
- [DSL.md](../specifications/DSL.md): public DSL contract.
- [SymbolicExecution.md](../specifications/SymbolicExecution.md): symbolic capture contract.
- [IntermediateRepresentation.md](../specifications/IntermediateRepresentation.md): IR shape and invariants.
- [ExecutionSemanticContract.md](../specifications/ExecutionSemanticContract.md): shared online/generated lowering contract.
- [PySparkCodeGeneration.md](../specifications/PySparkCodeGeneration.md): generated PySpark rendering contract.
- [OnlineExecution.md](../specifications/OnlineExecution.md): runtime session and online runner behavior.
