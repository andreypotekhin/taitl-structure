# DSL

## Purpose

The Structure DSL is the public Python API for declaring schemas, transforms, expressions, filters, joins, hooks,
validation policy, and runtime invocation. It must feel like ordinary typed Python while preserving one strict promise:
compiled subtransforms lower to Spark-plan-visible DataFrame and Column operations through backend-neutral IR.

The DSL is not a second PySpark wrapper layer. It is a small authoring surface that captures enough metadata and
symbolic behavior for `structure check`, online execution, optional generated PySpark, compiler provenance, static
dataflow traceability, and streaming compatibility checks to agree.

## Scope

This specification owns the public DSL surface and cross-cutting rules for:

- `@transform`;
- `Transform`;
- `input(...)`;
- public schema-returning subtransform methods;
- `@expr_fn`;
- `where(...)`;
- `@before(...)` and `@after(...)`;
- `@validate_output(...)`;
- `StructureSession`;
- expression helper imports;
- join enum imports;
- hook and schema mode enum imports;
- import-time and symbolic-execution behavior.

Detailed contracts are delegated to narrower specifications:

- schemas and output construction: [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.md);
- schema inheritance: [SchemaInheritance.md](SchemaInheritance.md);
- schema model: [SchemaModel.md](SchemaModel.md);
- assignment, literals, and nullability: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md);
- join behavior: [JoinSemantics.md](JoinSemantics.md);
- online and generated runtime behavior: [OnlineExecution.md](OnlineExecution.md);
- streaming compatibility: [StreamingCompatibility.md](StreamingCompatibility.md);
- version and compatibility policy: [CompatibilityPolicy.md](CompatibilityPolicy.md);
- diagnostic code, registry, and documentation lifecycle: [Diagnostics.md](Diagnostics.md).

When this document and a narrower specification overlap, the narrower specification owns the detailed semantics. This
document owns how those features appear and compose in the public DSL.

## Public Imports

The v1 public DSL must be importable from `structure`:

```python
from structure import (
    Structure,
    field,
    String,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    Array,
    Struct,
    Map,
    Transform,
    transform,
    input,
    output,
    expr_fn,
    where,
    before,
    after,
    validate_output,
    lower,
    upper,
    trim,
    to_decimal,
    when,
    coalesce,
    StructureSession,
    Join,
    JoinHint,
    SchemaMode,
)
```

Rules:

- Public examples must import from `structure`, not from internal modules.
- Internal modules may exist for implementation, but they are not part of the compatibility contract unless exported
  from `structure`.
- Importing `structure` and importing user modules that use the DSL must not import PySpark, start Spark, create a
  Spark session, inspect live DataFrames, or read project data.
- Decorators and declaration helpers must attach metadata only during import.
- Expensive validation, symbolic execution, compileability checks, and backend capability checks happen in compiler or
  runtime phases, not during module import.

## Canonical Source Shape

The canonical v1 source shape is:

```python
@transform
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    published = output(OrderPublished)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = join_one(
            self.customers,
            on=self.customers.id == order.customer_id,
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )

    @after(normalize, lane=orders)
    def remove_negative_totals(self, *, orders, spark, ctx):
        return orders.where(F.col("total") >= 0)

    @after(normalize, lane=orders, pass_inputs=True)
    def compare_to_raw(self, *, orders, inputs, spark, ctx):
        return orders
```

Runtime invocation is:

```python
session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
).run(session)
```

## Transform Classes

`@transform` marks a class as a Structure transform. A transform class must inherit from `Transform`.

Canonical forms:

```python
@transform
class NormalizeOrders(Transform):
    normalized = output(OrderNormalized)
    ...
```

```python
@transform(validate_intermediate=False, streaming_compatible=True)
class NormalizeOrders(Transform):
    normalized = output(OrderNormalized)
    ...
```

`@transform(...)` keyword arguments:

- `validate_intermediate`: optional class-level override for intermediate output validation.
- `streaming_compatible`: optional author promise that the transform must satisfy the streaming compatibility
  specification.

Rules:

- `@transform` without parentheses and `@transform(...)` with keyword arguments are both valid.
- Positional arguments to `@transform(...)` are rejected.
- Unknown keyword arguments are rejected with allowed values.
- `output=` is not a class-level option; it is reserved for method-level lane output binding.
- The decorator must preserve the original class identity enough for IDE navigation, `isinstance`, subclass checks,
  and direct instantiation to behave normally.
- The decorator must record source metadata for discovery, diagnostics, generated class naming, provenance, and
  static dataflow traceability.
- Transform classes should be import-safe. They must not do Spark work in class bodies.
- A class decorated with `@transform` but not inheriting `Transform` is invalid.
- A class inheriting `Transform` but missing `@transform` is not discovered as a compiled transform unless a future
  spec adds an explicit registration mode.

## Transform Invocation

`Transform.__init__(**inputs)` creates a deferred invocation by binding declared input DataFrames.

Rules:

- Transform constructors accept keyword arguments matching declared `input(...)` names.
- Positional arguments are rejected.
- Unknown input names are rejected at construction time when possible.
- Missing declared inputs must fail no later than `run(session)`.
- Construction stores input objects and performs no Spark action.
- Runtime context belongs in `StructureSession(ctx=...)`, not in transform constructors.
- Custom transform construction parameters are out of scope for v1.
- A transform invocation can be run through `transform.run(session)` or `session.run(transform)`.
- `run` is reserved for runtime execution. A public schema-returning subtransform named `run` is invalid.

## Inputs

`input(schema)` declares a named DataFrame input on a transform class:

```python
orders = input(OrderRaw)
customers = input(Customer)
```

Rules:

- `schema` must be a `Structure` subclass.
- Input declaration names are the class attribute names.
- Input declaration order is class body order.
- Duplicate input names after inheritance resolution are invalid.
- Input declarations are metadata objects during import.
- Accessing `self.orders` during symbolic execution returns an input scope, not a DataFrame.
- Accessing `self.orders` during ordinary runtime construction before `run(session)` should not expose a live
  DataFrame API.
- Generated `run(...)` methods use the same input names as keyword-only parameters.
- Hook input namespaces use the same input names as read-only attributes when `pass_inputs=True`.
- Method-level `@transform(input=declared_input)` selects a class input explicitly when the row schema is ambiguous
  or cannot be inferred safely.

Input DataFrame schema validation is governed by the validation configuration and runtime specifications. The DSL only
declares the expected schema.

## Lanes And Outputs

`lane(schema)` declares a named intermediate DataFrame stream on a transform class:

```python
orders = lane(OrderNormalized)
orders_with_product = lane(OrderWithProduct)
```

Lane declarations are not constructor inputs and are not returned from `run(...)`. They name internal funnel streams
that can be produced, consumed, and updated by subtransforms.

`output(schema)` declares a named transform result on a transform class. Every transform must declare at least one:

```python
accepted = output(OrderAccepted)
rejected = output(OrderRejected)
```

Rules:

- `schema` must be a `Structure` subclass.
- Output declaration names are the class attribute names.
- Output declaration order is class body order.
- A transform with no field-declared outputs is invalid.
- A single-output transform does not need an explicit output method binding; the final current lane produces the
  result.
- Final output fields must be materialized by explicit method-level `output=...` or by unique schema matching at the
  end of the funnel.
- Method-level `@transform(input=declared_input_or_lane)` selects an original class input or an already-produced lane.
  If a lane with the same name as an input declaration already exists, the lane shadows the original input.
- Method-level `@transform(output=declared_lane_or_output)` writes a declared lane or final output. If the selected
  name already exists as a lane, the write updates that lane.
- Method-level `input(...)`, `lane(...)`, and `output(...)` can also wrap declarations as role selectors:
  `input(orders)` forces the original runtime input, `lane(orders)` selects or writes the current working lane named
  `orders`, and `output(published)` selects the final output declaration.
- Bare method-level declarations smart-resolve by the schema expected by the subtransform parameter or return. When an
  original input and a latest same-named lane both match, the latest lane wins.
- Method-level `input=[...]` and `output=[...]` bind multiple parameters or returned values in order.
- Method-level `inout=source | target` is shorthand for one explicit source and target; one side may be a list.
- Method-level `inputs=`, `outputs=`, `lane=`, and `lanes=` are retired. Hook decorators still use `lane=` and
  `lanes=`.
- Method-level references use declarations, not strings.

Canonical multi-output form:

```python
@transform
class RouteOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    accepted = output(OrderAccepted)
    rejected = output(OrderRejected)

    @transform(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized.base(order)()

    @transform(output=accepted)
    def accept(self, order: OrderNormalized) -> OrderAccepted:
        where(order.customer_id.is_not_null())
        return OrderAccepted.base(order)(status="accepted")

    def keep_accepted(self, order: OrderAccepted) -> OrderAccepted:
        where(order.status == "accepted")
        return OrderAccepted.base(order)()

    @transform(output=rejected)
    def reject(self, order: OrderNormalized) -> OrderRejected:
        where(order.customer_id.is_null())
        return OrderRejected.base(order)(reason="missing customer")
```

Transform methods execute in source order. The compiler infers sources from parameter schemas when the choice is
unambiguous; decorators are needed when a method names a new lane or output, starts from a non-current input, branches,
or resolves repeated schemas. Output-local `where(...)` filters affect only the lane written by that method, so
`reject(...)` above still reads the normalized lane rather than the filtered `accepted` lane.

## Subtransforms

A compiled subtransform is a public instance method whose return annotation is a `Structure` subclass.

Canonical form:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Rules:

- Public instance methods are methods whose names do not start with `_`.
- A public method with a `Structure` return annotation is a compiled subtransform.
- A compiled subtransform has one or more parameters after `self`; every parameter annotation must be a `Structure`
  subclass.
- The first parameter is the driving row. Later parameters are symbolic relations that must be joined before their
  fields are used in filters or projections.
- The return annotation is either one `Structure` subclass or a fixed tuple such as `tuple[Accepted, Audited]`.
- `input=[...]` binds input or lane declarations to parameters in order when inference is ambiguous.
- `output=[...]` binds lane or output declarations to returned values in order when tuple results cannot be inferred.
- `input=` and `output=` also accept a single declaration.
- `input=`, `output=`, and `inout=` accept optional role selectors around declarations. Selectors are required when
  source-order shadowing would otherwise hide an original input or when an input declaration name is intentionally used
  as a working lane.
- The compiler infers bindings only when every schema has one unambiguous available declaration.
- Subtransforms execute in source order.
- Source-order lane flow must be valid. Undecorated methods consume and update the uniquely inferred lane.
  `@transform(output=target)` writes a named lane or output.
  `@transform(input=source, output=target)` selects both sides explicitly.
- If more than one declared input has the first subtransform's input schema, the compiler must require an unambiguous
  mapping such as `@transform(input=orders_external)` or emit a diagnostic.
- A multi-result subtransform executes its joins and `where(...)` filters once, then projects every returned schema
  from that shared row set.
- Private helper methods are allowed and are not compiled as subtransforms.
- Public helper methods without a `Structure` return annotation are ignored by the subtransform collector, but should
  not be used for compileable expression reuse. Use `@expr_fn` instead.
- Async subtransforms, generator subtransforms, classmethods, and staticmethods are out of scope for v1 compiled DSL.

The body of a compiled subtransform is symbolically executed. It must return a symbolic schema construction expression:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
)
```

or a schema base overlay:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
)
```

Output construction details are owned by [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.md).

## Symbolic Execution

The compiler builds a `TransformPlan` by invoking compiled subtransforms with symbolic row proxies.

During symbolic execution:

- field access produces `FieldRef` expressions;
- Python literals in expression positions produce typed literal expressions;
- expression helpers produce expression IR;
- `where(...)` records filter operations in the active subtransform context;
- `join_one(...)` records join operations;
- schema constructors record projection operations;
- hooks are not executed;
- live Spark objects are not created.

Rules:

- Symbolic execution must be deterministic for the same source and configuration.
- User code outside compiled subtransform bodies must not be symbolically executed except expression helpers called
  from those bodies.
- Unsupported operations must fail with structured compile errors. Structure must not silently lower unsupported
  Python code to UDFs, RDD operations, Pandas conversion, row-wise callbacks, or opaque generated code.
- Symbolic execution should avoid AST parsing except where needed for source spans, expression text, or diagnostics.
- If symbolic execution invokes user code and that user code performs side effects, Structure is not required to undo
  them. Diagnostics should still guide developers toward pure compiled subtransforms and explicit hooks.

## Expressions

Compiled expressions are symbolic objects with type, nullability, scope, source metadata, and lowering behavior.

The v1 expression surface includes:

- field references such as `order.customer_id`;
- Python literals described by `NullabilityAndTypeCoercion.md`;
- comparisons such as `==`, `!=`, `<`, `<=`, `>`, and `>=` when supported by the expression type;
- boolean combination with `&`, `|`, and `~`;
- null checks such as `expr.is_null()` and `expr.is_not_null()`;
- null-safe equality when provided by expression objects;
- helper calls such as `lower(...)`, `upper(...)`, `trim(...)`, `to_decimal(...)`, `coalesce(...)`, and `when(...)`.

Rules:

- Python `and`, `or`, and `not` are not valid for symbolic boolean expressions because Python evaluates truthiness
  instead of building expression trees. Diagnostics should suggest `&`, `|`, and `~`.
- Symbolic expressions must not be truthy or falsey in Python. `if order.id:` must fail with a diagnostic.
- Python string methods such as `order.customer_id.strip().lower()` are not compileable. Diagnostics should suggest
  direct DSL helpers such as `lower(trim(order.customer_id))`.
- Expression helpers must carry enough metadata for type checking, nullability checking, streaming compatibility, IR,
  online lowering, generated lowering, and diagnostics.
- Backend-specific lowering belongs in target layers, not in public expression objects.

Detailed type, literal, and nullability behavior is specified by [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md).

## Expression Helpers

`@expr_fn` declares a reusable compileable expression helper.

Module-level form:

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Class-local form:

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))

def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(customer_id=self.clean_id(order.customer_id))
```

Rules:

- `@expr_fn` functions are ordinary Python callables at import time.
- `@expr_fn` attaches metadata and wraps calls so symbolic arguments produce symbolic expressions.
- An expression helper must return a symbolic expression or a Python literal accepted as a source expression.
- A helper returning `None`, a DataFrame, an RDD, a Python collection of rows, or another unsupported object is invalid
  when called from a compiled subtransform.
- Class-local `@expr_fn` helpers do not take `self`, but may be called through `self` for IDE discoverability.
- Module-level helpers and class-local helpers use the same expression semantics.
- Helpers should be pure and deterministic. Non-deterministic helpers require an explicit future contract.
- Helpers must not import or require PySpark during compiler phases.
- Recursive expression helpers are invalid in v1 unless a future spec defines recursion limits and expansion behavior.

When a helper call is unsupported, diagnostics should show the helper name and the call site, not only the expanded
expression internals.

## Filtering

`where(predicate)` records a filter in the active subtransform context:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(to_decimal(order.total, precision=12, scale=2) >= 0)

    return OrderNormalized(...)
```

Rules:

- `where(...)` is valid only during symbolic execution of a compiled subtransform.
- `predicate` must be a non-nullable or nullable boolean expression accepted by the expression checker.
- Multiple `where(...)` calls are combined with logical AND while preserving source order.
- A `where(...)` call before a join can reference only scopes available before that join.
- A `where(...)` call after a join may reference the joined scope.
- Filter placement in IR must preserve source semantics. Emitters may optimize only when observable semantics remain
  the same.
- `where(...)` narrows simple `is_not_null()` field references according to
  [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md).
- Calling `where(...)` outside an active subtransform is invalid and should mention that filters belong inside
  compiled subtransform methods.

## Joins

The v1 DSL exposes lookup joins through the free-standing `join_one(relation, ...)` function:

```python
customer = join_one(
    self.customers,
    on=self.customers.id == order.customer_id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

Public enum values required for v1:

```text
Join.LEFT
Join.INNER
JoinHint.BROADCAST
```

Rules:

- `join_one(relation, *, on, how, hint=None)` is the canonical v1 join function.
- `on` and `how` are required.
- `hint` is optional.
- Join calls are valid only during symbolic execution of a compiled subtransform.
- Member joins such as `self.customers.join_one(...)` are rejected with migration guidance.
- `join_one(...)` returns a joined symbolic scope.
- Field access on the joined scope is scoped and must not rely on unqualified string column names.
- Join calls execute in source order.
- Repeated joins of the same input must produce deterministic aliases.
- `join_many(...)` and row-multiplying joins are deferred to v2.

Detailed join condition, null, aliasing, cardinality, projection, and diagnostics behavior is specified by
[JoinSemantics.md](JoinSemantics.md).

## Hooks

Hooks are explicit PySpark escape hatches attached to a concrete subtransform.

Canonical forms:

```python
@before(normalize, lane=orders)
def prepare(self, *, orders, spark, ctx):
    return orders
```

```python
@after(normalize, lane=orders, pass_inputs=True)
def compare_to_raw(self, *, orders, inputs, spark, ctx):
    return orders
```

```python
@after(publish, lane=published, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, published, spark, ctx):
    return published
```

Hook decorator keyword arguments:

- `pass_inputs`: whether the hook receives a read-only namespace of original named input DataFrames.
- `schema_mode`: output schema validation mode after the hook.
- `project_output`: whether extra hook-produced columns should be projected away after validation.
- `streaming_safe`: author promise used by streaming compatibility checks.

Rules:

- `@before(method, lane=lane)` runs before the compiled operations for `method`.
- `@after(method, lane=lane)` runs after the compiled operations for `method`.
- `@before(method, lane=lane)` selects the lane consumed by the target method.
- `@after(method, lane=lane)` selects the lane produced by the target method.
- The target must be a compiled subtransform method on the same transform class.
- Hook order for the same target and timing is source order.
- Hooks are not symbolically executed and are opaque to the compiler except for metadata, signature, declared options,
  provenance, and streaming compatibility classification.
- A hook without `pass_inputs=True` must have signature `def hook(self, *, selected_lane_name, spark, ctx)`.
- A hook with `pass_inputs=True` must have signature `def hook(self, *, selected_lane_name, inputs, spark, ctx)`.
- `inputs` is a read-only namespace containing the original DataFrames bound to the transform invocation. It does not
  contain intermediate DataFrames unless they were also declared original inputs.
- Hooks must return a DataFrame at runtime.
- Generated code and online execution call hooks on the source transform instance so hook behavior remains transparent.
- Hooks may import and use PySpark because they execute at runtime, not during compiler phases.
- Hook metadata must be present in IR so generated code can call hooks and traceability can mark opaque boundaries.

`SchemaMode` must include at least the strict default mode and `SchemaMode.ALLOW_EXTRA_COLUMNS`. The exact enum names
for the default strict mode may be implementation-defined in v1, but public documentation should use the default by
omitting `schema_mode`.

## Validation Policy

`@validate_output(enabled)` overrides validation for one subtransform output:

```python
@validate_output(False)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Rules:

- `enabled` must be a boolean.
- `@validate_output(...)` applies to the decorated compiled subtransform only.
- Method-level validation settings override class-level `@transform(validate_intermediate=...)`.
- Class-level settings override project defaults.
- Unknown validation decorator arguments are invalid.
- Validation policy must be recorded on `StepPlan`.
- Runtime validation placement must be identical for online and generated execution.

Project-level validation configuration and runtime validation behavior are outside this DSL spec. This document only
defines the public source hooks for validation policy.

## Execution Session

`StructureSession` is the public runtime session:

```python
session = StructureSession(spark=spark, ctx=ctx)
result = session.run(EnrichOrders(orders=orders_df, customers=customers_df))
```

Rules:

- `spark` is supplied by the caller.
- `ctx` is optional and passed to hooks.
- The session owns resolved configuration, execution mode, target backend, runner selection, and optional plan cache.
- The session must not start Spark, stop Spark, mutate Spark configuration silently, own streaming lifecycle, or manage
  orchestration concerns.
- The default execution mode is online.
- Generated execution remains available through configuration.

Detailed runtime behavior is specified by [OnlineExecution.md](OnlineExecution.md).

## Discovery and Metadata

The DSL must produce metadata sufficient for discovery and compilation:

```text
TransformDef
  source class
  declared inputs
  subtransforms
  expression helpers
  hooks
  validation policy
  streaming policy
  source locations when available
```

Rules:

- Discovery finds classes marked by `@transform` under configured source roots.
- Metadata should preserve source order for input declarations, subtransforms, hooks, fields, filters, joins, and
  projections.
- Metadata should be immutable or treated as immutable after discovery.
- Source locations should be captured when practical, but lack of source spans must not prevent compilation when the
  source object is otherwise valid.
- Metadata extraction must not require PySpark, Java, Spark startup, a Spark cluster, or live DataFrames.

## IR Contract

The DSL frontend must build backend-neutral IR.

Minimum transform IR:

```text
TransformPlan
  transform name
  source class
  generated class identity
  inputs
  steps
  validation policy
  streaming policy
  provenance
  static dataflow
```

Minimum step IR:

```text
StepPlan
  name
  input schema
  output schema
  operations
  hooks_before
  hooks_after
  validate_output
```

Minimum operation kinds:

```text
Filter
Join
Project
HookCall
ValidateSchema
```

Minimum expression kinds:

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

- Public DSL objects must not expose backend-specific PySpark implementation details as their semantic model.
- IR should contain enough source context for actionable diagnostics and provenance.
- IR must preserve deterministic operation order.
- IR must be consumable by both online PySpark execution and generated PySpark emission.
- Backend capability checks consume IR plus target metadata, not live Spark objects.

## Compileability Checks

The DSL frontend must reject source that cannot be lowered safely.

Required checks include:

- transform decorator usage;
- transform base class;
- input schema validity;
- subtransform signature and source-order flow;
- reserved `run` method misuse;
- expression helper return validity;
- unsupported Python operators and methods;
- `where(...)` predicate type;
- output projection completeness;
- output assignment type and nullability compatibility;
- join condition support;
- `join_one(...)` uniqueness warnings;
- hook target and signature validity;
- validation decorator validity;
- streaming compatibility when enabled.

Checks must run without importing PySpark or starting Spark, except runtime-only checks explicitly owned by
`StructureSession` or a runtime runner.

## Diagnostics

Diagnostic code format, severity names, lifecycle rules, registry requirements, and stable documentation anchors are
owned by [Diagnostics.md](Diagnostics.md). This section defines the DSL-specific context and message content that
DSL diagnostics must supply.

DSL diagnostics must include:

- diagnostic code;
- severity;
- transform class when available;
- subtransform method when available;
- input, hook, field, expression, or decorator when relevant;
- source file and line when available;
- problem;
- why it matters when the issue is not obvious;
- direct DSL fix when one exists;
- `@expr_fn` helper fix when reuse is likely;
- hook workaround when arbitrary PySpark is appropriate;
- configuration workaround only when safe and real;
- link to the most specific specification or public docs page.

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

See docs/specifications/DSL.md
```

Invalid hook example:

```text
CompileError HOOK-E0701: Invalid hook signature

Transform:
  EnrichOrders

Hook:
  compare_to_raw after normalize

Problem:
  Hooks with pass_inputs=True must declare keyword-only inputs.

Use:
  def compare_to_raw(self, *, orders, inputs, spark, ctx):
      return orders

See docs/specifications/DSL.md
```

Invalid transform invocation example:

```text
RuntimeError ONLINE-E1001: Unknown transform input

Transform:
  EnrichOrders

Input:
  customer

Problem:
  The transform declares inputs: orders, customers.

Use:
  EnrichOrders(orders=orders_df, customers=customers_df)

See docs/specifications/DSL.md
```

## Non-Goals

The following are outside v1 DSL scope:

- arbitrary Python control flow as a source of multiple dynamic DataFrame branches;
- subtransform branching and merging;
- custom transform constructor parameters;
- async, generator, classmethod, or staticmethod subtransforms;
- implicit Python UDF generation;
- Pandas UDF generation;
- RDD operations;
- automatic fallback from compiled expressions to hooks;
- automatic deduplication for `join_one(...)`;
- row-multiplying `join_many(...)`;
- aggregations, windows, deduplication, and higher-order collection transforms;
- streaming source, sink, trigger, checkpoint, and query lifecycle DSL;
- Spark Connect-specific public syntax;
- non-PySpark backends in v1.

## Implementation Checklist

1. Export the public DSL symbols from `structure`.
2. Implement import-safe `@transform` metadata for bare and keyword forms.
3. Implement `Transform.__init__(**inputs)` and `Transform.run(session)` deferred invocation.
4. Implement `input(Structure)` declaration metadata and symbolic input scopes.
5. Discover compiled subtransforms from public instance methods with `Structure` return annotations.
6. Reject reserved `run` subtransform names.
7. Preserve source order for inputs, subtransforms, hooks, filters, joins, and projections.
8. Implement symbolic row proxies and scoped field references.
9. Implement expression objects with type, nullability, scope, and source metadata.
10. Implement public expression helpers and helper metadata.
11. Implement `@expr_fn` for module-level and class-local helpers without `self`.
12. Implement `where(...)` context capture and boolean predicate checking.
13. Implement `join_one(...)` input-scope capture and enum validation.
14. Implement `@before(...)` and `@after(...)` metadata, options, ordering, and signature checks.
15. Implement `@validate_output(...)` method-level metadata.
16. Build `TransformPlan` and `StepPlan` IR from symbolic execution.
17. Run compileability checks against IR without PySpark imports.
18. Ensure online and generated execution consume the same IR semantics.
19. Add diagnostics with direct DSL fixes, helper fixes, hook workarounds, and spec links.
20. Add tests for import safety, metadata, symbolic execution, diagnostics, and runtime invocation.
21. Update public docs and model fixtures to use only canonical DSL syntax.

## Acceptance Criteria

The implementation is complete when tests prove:

- `from structure import ...` exposes every public symbol listed in this specification.
- Importing a module containing schemas, transforms, expression helpers, and hooks does not import PySpark or create a
  Spark session.
- `@transform` and `@transform(...)` both mark transform classes.
- `@transform` rejects unknown keyword arguments and classes not inheriting `Transform`.
- `orders = input(OrderRaw)` records the input name, schema, order, and source metadata.
- `published = output(OrderPublished)` records a named public result contract.
- A transform with no `output(...)` declaration fails before symbolic execution.
- Class-level `to=` is rejected; output fields are the only transform result declaration.
- Transform construction accepts declared keyword inputs and performs no Spark action.
- Transform construction rejects positional arguments and unknown input names.
- Missing declared inputs fail no later than `run(session)`.
- `Transform.run(session)` delegates to `StructureSession`, and `session.run(transform)` works equivalently.
- Public schema-returning instance methods are discovered as subtransforms in source order.
- Private helper methods are not discovered as subtransforms.
- A public schema-returning method named `run` fails with a reserved-name diagnostic.
- Subtransform parameter and return annotations are validated.
- Source-order schema flow is validated.
- Symbolic field access produces scoped field references.
- Schema constructors and `SchemaClass.base(...)` produce projection IR.
- Module-level `@expr_fn` helpers compile.
- Class-local `@expr_fn` helpers without `self` compile when called through `self`.
- An `@expr_fn` returning a non-expression value fails with an actionable diagnostic.
- Direct DSL helpers such as `lower(trim(order.customer_id))` compile to expression IR.
- Unsupported Python string methods fail with a direct DSL replacement suggestion.
- Python boolean `and`, `or`, and `not` fail with suggestions for `&`, `|`, and `~`.
- `where(...)` records filters in source order.
- Multiple `where(...)` calls combine with logical AND.
- `where(...)` outside a compiled subtransform fails clearly.
- `where(expr.is_not_null())` participates in nullability narrowing.
- `join_one(..., how=Join.LEFT)` and `join_one(..., how=Join.INNER)` record join IR.
- Unsupported join enum values fail or warn according to `JoinSemantics.md`.
- `@before(...)` and `@after(...)` hooks bind to subtransforms declared in the same class.
- Hook order for the same target and timing is source order.
- Hook signatures are validated for both `pass_inputs=False` and `pass_inputs=True`.
- Hook metadata records `schema_mode`, `project_output`, `pass_inputs`, and `streaming_safe`.
- `@validate_output(False)` overrides validation for one subtransform.
- DSL diagnostics include transform, subtransform, source expression or decorator, suggested fix, and documentation
  link.
- `structure check` and `structure compile` can validate DSL source without PySpark, Java, SparkSession, Spark
  startup, or a Spark cluster.
- Online and generated execution preserve the same DSL semantics for projection, filtering, expression helpers, joins,
  hooks, validation policy, and hook input namespaces.
