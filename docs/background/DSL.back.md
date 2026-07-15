# DSL

The Structure DSL is the public Python API for declaring schemas, transforms, expressions, filters, joins, hooks,
validation policy, and runtime invocation. It reads like ordinary typed Python while preserving one strict promise:
compiled step methods stay visible to Structure and Spark.

The DSL is not a second PySpark wrapper layer. It is a small authoring surface that keeps checks, execution,
generated PySpark, explain output, traceability, and streaming compatibility aligned.

For exhaustive supported API names, PySpark parity, examples, and differences, see the
[API reference](../reference/API.ref.md) for [schemas](../api/Schemas.api.md),
[transforms](../api/Transforms.api.md), [expressions](../api/Expressions.api.md),
[joins](../api/Joins.api.md), [aggregations](../api/Aggregations.api.md),
[windows](../api/Windows.api.md), [collections](../api/Collections.api.md), and
[streaming](../api/Streaming.api.md).

## Scope

This reference covers the public DSL surface and cross-cutting rules for:

- `@transform`;
- `Transform`;
- `input(...)`;
- public schema-returning step methods;
- `@special(type="expr")`;
- `where(...)`;
- `@raw`;
- `@validate_output(...)`;
- `StructureSession`;
- expression helper imports;
- join enum imports;
- hook and schema mode enum imports;
- import-time and symbolic-execution behavior.

Detailed behavior is covered by narrower references:

- schemas and output construction: [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.back.md));
- schema inheritance: [SchemaInheritance.md](SchemaInheritance.back.md));
- schema model: [SchemaModel.md](SchemaModel.back.md));
- assignment, literals, and nullability: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md));
- join behavior: [JoinSemantics.md](JoinSemantics.back.md));
- advanced aggregation, windows, and collection helpers:
  [AdvancedAnalyticalOperations.md](AdvancedAnalyticalOperations.back.md));
- execution and generated-code runtime behavior: [Execution.md](Execution.back.md));
- streaming compatibility: [StreamingCompatibility.md](StreamingCompatibility.back.md));
- version and compatibility policy: [CompatibilityPolicy.md](CompatibilityPolicy.back.md));
- diagnostic code, registry, and documentation lifecycle: [Diagnostics.md](Diagnostics.back.md)).

When this document and a narrower reference overlap, the narrower reference owns the detailed semantics. This
document owns how those features appear and compose in the public DSL.

## Public Imports

The public DSL is importable from `structure`:

```python
from structure import *
```

## Canonical Source Shape

The canonical v1 source shape is:

```python
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    published = output(OrderPublished)

    @special(type="expr")
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
        left_join(
            on=order.customer_id == customer.id,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )

    @raw(inout=lane(orders) | lane(orders))
    def remove_negative_totals(self, *, orders, spark, ctx):
        return orders.where(F.col("total") >= 0)

    @raw(inout=input(orders) | lane(orders))
    def compare_to_raw(self, *, orders, spark, ctx):
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

Inheriting from `Transform` declares a Structure transform class. `@transform` is optional and records class-level
options when those options are needed.

Canonical forms:

```python
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
  reference.

Rules:

- A concrete class inheriting `Transform` may be compiled without a class-level decorator.
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
- Project discovery compiles concrete `Transform` entrypoints: classes that declare final outputs or a class-field
  pipeline. Reusable lane-only base classes remain support code.
- A direct or indirect parent class inheriting `Transform` may contribute reusable inputs, lanes, outputs, hooks,
  helpers, and step methods to a child even when the parent is not decorated with `@transform`.
- Inherited parent step methods run before child step methods. Multiple direct parents run left to right in the
  Python class declaration, and shared diamond ancestors contribute once.
- A child step method with the same method name overrides the inherited scheduled step. Sibling parents that define
  the same step method name are ambiguous unless the child overrides that name.
- An overriding step method may explicitly schedule the overridden parent implementation with `super().method(row)`,
  `Base.method(self, row)`, or `super(Base, self).method(row)`. The parent implementation runs as a separate scheduled
  step before the child step and returns a symbolic row for the parent output.

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
- `run` is reserved for runtime execution. A public schema-returning step method named `run` is invalid.
- Transform invocations can be composed with `.to(...)` when complete transform outputs should feed later transform
  inputs. Composition behavior is specified in [TransformComposition.md](TransformComposition.back.md)).

## Transform Composition

Runtime composition chains transform invocations:

```python
result = (
    NormalizeOrders(orders=orders_df)
    .to(AddProduct(products=products_df))
    .to(PublishOrders())
    .run(session)
)
```

Rules:

- `.to(...)` accepts transform invocations, not transform classes.
- `a.to(b, c)` and `a.to(b).to(c)` compile to the same flattened pipeline.
- `Transform.to(a, b, c)` starts a pipeline without a receiver.
- A downstream input is supplied by its constructor binding or by one matching upstream output.
- Constructor binding plus upstream output match is ambiguous and must fail.
- Matching uses exact schema identity, with same-name outputs preferred when several upstream outputs share a schema.
- `lane(...)` declarations are internal and cannot be used as composition bindings.
- The composed result exposes the final stage's declared outputs.
- Hook-bearing stages are rejected until hook ownership for composed stages is specified.

Generated-capable composition uses a wrapper transform with one pipeline field:

```python
class OrderPipeline(Transform):
    orders = input(OrderRaw)
    products = input(Product)

    pipeline = Transform.to(
        NormalizeOrders(orders=orders),
        AddProduct(products=products),
        PublishOrders(),
    )
```

See [TransformComposition.md](TransformComposition.back.md)) for inheritance and composition examples.

## Inputs

`input(schema)` declares a named DataFrame input on a transform class:

```python
orders = input(OrderRaw)
customers = input(Customer)
```

Rules:

- `schema` must be a `Schema` class.
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

Input DataFrame schema validation is governed by the validation configuration and runtime references. The DSL only
declares the expected schema.

## Lanes And Outputs

`lane(schema)` declares a named intermediate DataFrame stream on a transform class:

```python
orders = lane(OrderNormalized)
orders_with_product = lane(OrderWithProduct)
```

Lane declarations are not constructor inputs and are not returned from `run(...)`. They name internal funnel streams
that can be produced, consumed, and updated by step methods.

`output(schema)` declares a named transform result on a transform class. Every transform must declare at least one:

```python
accepted = output(OrderAccepted)
rejected = output(OrderRejected)
```

Rules:

- `schema` must be a `Schema` class.
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
- Bare method-level declarations smart-resolve by the schema expected by the step-method parameter or return. When an
  original input and a latest same-named lane both match, the latest lane wins.
- Method-level `input=[...]` and `output=[...]` bind multiple parameters or returned values in order.
- Method-level `inout=source | target` is shorthand for one explicit source and target; one side may be a list.
- Method-level `cache=...` records an explicit v2 cache directive for the step method. It is intentionally part of
  `@transform(...)` rather than a separate public decorator so user projects can keep their own `@cache` helpers.
- Method-level `inputs=`, `outputs=`, `lane=`, and `lanes=` are retired. Hook decorators still use `lane=` and
  `lanes=`.
- Method-level references use declarations, not strings.

Canonical multi-output form:

```python
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

## Step methods

A compiled step method is a public instance method whose return annotation is a `Schema` class.

Canonical form:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Rules:

- Public instance methods are methods whose names do not start with `_`.
- A public method with a `Structure` return annotation is a compiled step method.
- Public schema-returning methods inherited from `Transform` ancestors are compiled as parent step methods before
  local child step methods.
- A compiled step method has one or more parameters after `self`; every parameter annotation must be a `Structure`
  subclass.
- The first parameter is the driving row. Later parameters are symbolic relations that must be joined before their
  fields are used in filters or projections.
- The return annotation is either one `Schema` class or a fixed tuple such as `tuple[Accepted, Audited]`.
- `input=[...]` binds input or lane declarations to parameters in order when inference is ambiguous.
- `output=[...]` binds lane or output declarations to returned values in order when tuple results cannot be inferred.
- `input=` and `output=` also accept a single declaration.
- `input=`, `output=`, and `inout=` accept optional role selectors around declarations. Selectors are required when
  source-order shadowing would otherwise hide an original input or when an input declaration name is intentionally used
  as a working lane.
- Without `input=`, the compiler infers parameter bindings from available input or lane schemas. If several sources have
  the same schema, it prefers a source named after the parameter or its simple plural form. Plural inference adds a
  trailing `s` before any non-alpha suffix, so `order` matches `orders` and `order1` matches `orders1`; irregular
  English plurals are not inferred.
- Once `input=` is present, it supplies all parameter bindings for that step method; parameter-name inference is not
  mixed with partial explicit bindings.
- Once `output=` is present on a tuple-returning step method, it supplies all result bindings for that step method.
- The compiler infers bindings only when every schema has one unambiguous available declaration after the name rule is
  applied.
- Step methods execute in source order.
- Inherited step methods execute in effective source order: parent classes first, direct parents left to right, then
  child class methods. Diamond ancestors are visited once.
- Overriding an inherited step method without calling the parent replaces the inherited step position.
- Calling an overridden parent step method from the override schedules the parent as its own DataFrame step immediately
  before the child override. Parent hooks, validation, lane writes, and traceability belong to the parent step.
- Other direct calls from one compiled step method to another are invalid. Step methods are pipeline steps scheduled
  by source order, lane binding, inheritance, or `Transform.to(...)`; reusable inline logic belongs in private helpers
  or `@special(type="expr")` helpers.
- Source-order lane flow must be valid. Undecorated methods consume and update the uniquely inferred lane.
  `@transform(output=target)` writes a named lane or output.
  `@transform(input=source, output=target)` selects both sides explicitly.
- If more than one declared input has the first step method's input schema, the compiler must require an unambiguous
  mapping such as `@transform(input=orders_external)` or emit a diagnostic.
- A multi-result step method executes its joins and `where(...)` filters once, then projects every returned schema
  from that shared row set.
- Private helper methods are allowed and are not compiled as step methods.
- Public helper methods without a `Structure` return annotation are ignored by the step method collector, but should
  not be used for compileable expression reuse. Use `@special(type="expr")` instead.
- Async step methods, generator step methods, classmethods, and staticmethods are out of scope for v1 compiled DSL.

The body of a compiled step method is symbolically executed. It must return a symbolic schema construction expression:

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

When the output copies same-name fields from a source row, prefer schema-method projection:

```python
return OrderPublished.project(order)
```

`project(source, TargetSchema)` and source-less `project(TargetSchema)` remain supported compatibility forms inside
compiled step methods. Prefer `TargetSchema.project(source)` in public examples because the source row remains visible.

Output construction details are owned by [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.back.md)).

## Symbolic Execution

The compiler builds a `TransformPlan` by invoking compiled step methods with symbolic row proxies.

During symbolic execution:

- field access produces `FieldRef` expressions;
- Python literals in expression positions produce typed literal expressions;
- expression helpers produce expression IR;
- `where(...)` records filter operations in the active step-method context;
- `lookup_join(...)` records join operations in source order;
- schema constructors record projection operations;
- hooks are not executed;
- live Spark objects are not created.

Rules:

- Symbolic execution must be deterministic for the same source and configuration.
- User code outside compiled step method bodies must not be symbolically executed except expression helpers called
  from those bodies.
- Unsupported operations must fail with structured compile errors. Structure must not silently lower unsupported
  Python code to UDFs, RDD operations, Pandas conversion, row-wise callbacks, or opaque generated code.
- Symbolic execution should avoid AST parsing except where needed for source spans, expression text, or diagnostics.
- If symbolic execution invokes user code and that user code performs side effects, Structure is not required to undo
  them. Diagnostics should still guide developers toward pure compiled step methods and explicit hooks.

## Expressions

Compiled expressions are symbolic objects with type, nullability, scope, source metadata, and lowering behavior.

The v1 expression surface includes:

- field references such as `order.customer_id`;
- Python literals described by `NullabilityAndTypeCoercion.md`;
- comparisons such as `==`, `!=`, `<`, `<=`, `>`, and `>=` when supported by the expression type;
- membership predicates such as `expr.isin(...)`;
- inclusive range predicates such as `expr.between(lower, upper)`;
- string predicates such as `expr.contains(value)`, `expr.like(pattern)`, `expr.ilike(pattern)`, and
  `expr.rlike(pattern)`;
- collection indexing such as `array_expr[index]` and `map_expr[key]`;
- Struct field access with `struct_expr.get_field(name)`;
- scalar casts such as `expr.cast(Integer())`, `expr.astype(String())`, and `expr.try_cast(Integer())`;
- string helpers `substring(...)`, `split(...)`, `regexp_replace(...)`, `regexp_extract(...)`, `length(...)`, and
  `concat_ws(...)`, `initcap(...)`, `reverse(...)`, `translate(...)`, `instr(...)`, and `levenshtein(...)`;
- temporal helpers `date_add(...)`, `datediff(...)`, and `date_trunc(...)`;
- numeric helpers `abs(...)`, `round(...)`, `ceil(...)`, and `floor(...)`;
- predicate helpers `isnull(...)`, `isnotnull(...)`, and `isnan(...)`;
- boolean combination with `&`, `|`, and `~`;
- null checks such as `expr.is_null()` and `expr.is_not_null()`;
- null-safe equality when provided by expression objects;
- helper calls such as `lower(...)`, `upper(...)`, `trim(...)`, `to_decimal(...)`, `coalesce(...)`, and `when(...)`.

String predicates require a typed String expression and a Python string literal. `like(...)` uses Spark SQL `%` and `_`
wildcards, `ilike(...)` is case-insensitive, and `rlike(...)` accepts a Java regular-expression pattern. They preserve
the source expression's nullability and render as visible PySpark `Column` calls.

Collection indexing requires an Array with an integral index or a Map with a key of the declared key type. It infers the
array element or map value type. Lookup results are nullable because an array index can be absent and a map key may be
missing.

`get_field(name)` reads a declared Struct field by its Python name or physical column alias. It is useful when
attribute access is inconvenient, preserves the field's type and nullability, and renders as a visible PySpark
`Column.getField(...)` call.

`cast(...)`, its `astype(...)` alias, and `try_cast(...)` require a Structure scalar type such as `Integer()`,
`String()`, `Date()`, or `Decimal(precision, scale)`. Strict casts preserve source nullability and render as native
PySpark `Column.cast(...)` calls. `try_cast(...)` also returns null when conversion fails, so its result is always
nullable; it is available only with target profile `>=4.0,<4.1`, where it renders as `Column.try_cast(...)`.

Order descriptors are `expr.asc()`, `expr.desc()`, `expr.asc_nulls_first()`, `expr.asc_nulls_last()`,
`expr.desc_nulls_first()`, and `expr.desc_nulls_last()`. Use them as a window `order_by=` value; they render as the
corresponding visible PySpark `Column` ordering call.

`substring(value, start=..., length=...)` uses a one-based positive start and non-negative length. `split(...)` and
`regexp_replace(...)` require explicit Python string patterns, keeping Java regular-expression behavior visible rather
than admitting raw SQL. `split(...)` returns an Array of non-null strings when its input is non-null.
`regexp_extract(value, pattern=..., group=...)` returns the selected Java-regex capture group and requires a
non-negative literal group index; a non-matching group produces an empty string.
`length(value)` counts String characters, including trailing spaces, and returns a nullable Integer only when its
source value is nullable.
`initcap(value)` title-cases words and `reverse(value)` reverses the String. `translate(value, matching=...,
replacement=...)` replaces individual matching characters. `instr(value, substring=...)` returns a one-based position,
or zero when absent. `levenshtein(left, right)` returns the String edit distance. All preserve the nullability of their
String inputs.
`concat_ws(separator, *values)` requires a Python string separator and one or more String expressions or literals;
it skips null values and produces a non-null String expression.

`date_add(value, days=...)` returns a Date, `datediff(end, start)` returns an Integer number of days, and
`date_trunc(value, unit=...)` returns a Timestamp. Each helper accepts typed Date or Timestamp expressions and keeps
the source nullability visible; the truncation unit is an explicit non-empty string literal.

`abs(...)` and `round(..., scale=...)` preserve their numeric input type. `ceil(...)` and `floor(...)` return Long
for non-Decimal inputs; Decimal inputs yield a scale-zero Decimal. Every numeric helper requires a typed integer,
long, float, double, or decimal expression and preserves input nullability.

`isnull(...)` and `isnotnull(...)` are function-style equivalents of the corresponding expression methods.
`isnan(...)` accepts only Float or Double expressions because NaN cannot occur in Structure's other scalar types. All
three return a non-null Boolean.

Rules:

- Python `and`, `or`, and `not` are not valid for symbolic boolean expressions because Python evaluates truthiness
  instead of building expression trees. Diagnostics should suggest `&`, `|`, and `~`.
- Symbolic expressions must not be truthy or falsey in Python. `if order.id:` must fail with a diagnostic.
- Python string methods such as `order.customer_id.strip().lower()` are not compileable. Diagnostics should suggest
  direct DSL helpers such as `lower(trim(order.customer_id))`.
- Expression helpers must carry enough metadata for type checking, nullability checking, streaming compatibility, IR,
  execution lowering, generated lowering, and diagnostics.
- Backend-specific lowering belongs in target layers, not in public expression objects.

Detailed type, literal, and nullability behavior is specified by
[NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md)).

## Expression Helpers

`@special(type="expr")` declares a reusable compileable expression helper.

Module-level form:

```python
@special(type="expr")
def clean_id(value):
    return lower(trim(value))
```

Class-local form:

```python
@special(type="expr")
def clean_id(value):
    return lower(trim(value))

def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(customer_id=self.clean_id(order.customer_id))
```

Rules:

- `@special(type="expr")` functions are ordinary Python callables at import time.
- `@special(type="expr")` attaches metadata and wraps calls so symbolic arguments produce symbolic expressions.
- An expression helper must return a symbolic expression or a Python literal accepted as a source expression.
- A helper returning `None`, a DataFrame, an RDD, a Python collection of rows, or another unsupported object is invalid
  when called from a compiled step method.
- Class-local `@special(type="expr")` helpers do not take `self`, but may be called through `self` for IDE discoverability.
- Module-level helpers and class-local helpers use the same expression semantics.
- Helpers should be pure and deterministic. Non-deterministic helpers require an explicit future contract.
- Helpers must not import or require PySpark during compiler phases.
- Recursive expression helpers are invalid in v1 unless a future spec defines recursion limits and expansion behavior.

When a helper call is unsupported, diagnostics should show the helper name and the call site, not only the expanded
expression internals.

## Filtering

`where(predicate)` records a filter in the active step-method context:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(to_decimal(order.total, precision=12, scale=2) >= 0)

    return OrderNormalized(...)
```

Rules:

- `where(...)` is valid only during symbolic execution of a compiled step method.
- `predicate` must be a non-nullable or nullable boolean expression accepted by the expression checker.
- Adjacent `where(...)` calls may be combined with logical AND while preserving source order.
- A `where(...)` call before a join can reference only scopes available before that join.
- A `where(...)` call after a join may reference the joined scope.
- Filter placement in IR must preserve source semantics. Emitters may optimize only when observable semantics remain
  the same.
- `where(...)` narrows simple `is_not_null()` field references according to
  [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md)).
- Calling `where(...)` outside an active step method is invalid and should mention that filters belong inside
  compiled step methods.

## Selected-Row Dedupe

`latest_by(...)` and `earliest_by(...)` keep one row per partition by explicit order. `dedupe_latest_by(...)` and
`dedupe_earliest_by(...)` are end-user convenience aliases for the same deterministic selected-row behavior when the
source intent is keyed deduplication:

```python
def latest_events(self, event: RawEvent) -> LatestEvent:
    dedupe_latest_by(event.sequence, partition_by=event.account_id)
    return LatestEvent(account_id=event.account_id, event_id=event.event_id, sequence=event.sequence)
```

Rules:

- `partition_by` is required and accepts one expression or a list/tuple of expressions.
- `order_by` is required and defines which row survives within each partition.
- `latest_by(...)` and `dedupe_latest_by(...)` order descending.
- `earliest_by(...)` and `dedupe_earliest_by(...)` order ascending.
- The current public tie policy is `TiePolicy.ERROR`; other policies are rejected until their behavior is specified.
- These helpers are batch-only for streaming compatibility until explicit streaming state and watermark semantics
  exist.

## Window Projection Helpers

Projection-level window helpers return symbolic expressions that render as Spark window expressions:

```python
def rank_events(self, event: RawEvent) -> RankedEvent:
    return RankedEvent(
        account_id=event.account_id,
        row_number=row_number(partition_by=event.account_id, order_by=event.sequence),
        rank=rank(partition_by=event.account_id, order_by=event.sequence, descending=True),
        dense_rank=dense_rank(partition_by=event.account_id, order_by=event.sequence),
        previous_sequence=lag(event.sequence, partition_by=event.account_id, order_by=event.sequence),
        next_sequence=lead(event.sequence, partition_by=event.account_id, order_by=event.sequence),
        rolling_total=rolling_sum(event.amount, partition_by=event.account_id, order_by=event.sequence, preceding=6),
    )
```

Rules:

- `row_number(...)`, `rank(...)`, and `dense_rank(...)` return non-nullable `Long` expressions.
- `lag(value, ...)` and `lead(value, ...)` return the value expression type and are nullable unless a non-null default
  is supplied.
- `partition_by` is required and accepts one expression or a list/tuple of expressions.
- `order_by` is required.
- `descending=True` reverses the order expression.
- `offset` for `lag(...)` and `lead(...)` must be greater than or equal to `1`.
- `rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and `rolling_max(...)` require `preceding=...` and use a
  row frame from `-preceding` through the current row.
- Window helpers are valid in projection expressions and must remain Spark-plan-visible.
- Window helpers are batch-only for streaming compatibility until explicit streaming state and watermark semantics
  exist.

## Joins

The DSL exposes joins through free-standing `*_join(...)` functions. Ordinary left enrichment uses `left_join(...)`.
When the `on` clause names exactly one unjoined relation, the call stays bare:

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    left_join(
        on=order.customer_id == customer.id,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(customer_name=customer.name)
```

Class input scopes may also be joined directly:

```python
left_join(
    on=order.customer_id == self.customers.id,
    hint=JoinHint.BROADCAST,
)
return OrderWithCustomer.base(order)(customer_name=self.customers.name)
```

Documentation uses inferred bare joins as the default style.

Public enum values required for v1:

```text
Join.LEFT
Join.INNER
JoinHint.BROADCAST
```

Rules:

- `left_join(*, on, hint=None, strategy=None)` and `inner_join(*, on, hint=None, strategy=None)` are the canonical
  ordinary rowset join shortcuts when the relation is inferable.
- `lookup_join(*, on, how, hint=None, dedupe=None)` is the strict lookup join function when the right-side key must
  match at most one row or when a deterministic `dedupe` policy is required.
- Legacy explicit-selection overloads remain supported, but they are not the documented style.
- `on` and `how` are required.
- Rowset join helpers also accept `on="key"` or `on=["key1", "key2"]` when the current row and the joined
  relation expose same-named keys. Structure expands this shorthand to typed equality predicates and rejects missing,
  repeated, or incompatible keys before lowering.
- `hint` is optional.
- `dedupe` is optional. When present, it must be a deterministic `JoinDedupe` policy and reduces the right side before
  the lookup join.
- Join calls are valid only during symbolic execution of a compiled step method.
- Member joins such as `self.customers.lookup_join(...)` are rejected with migration guidance.
- `lookup_join(...)` records the same ordered join operation for inferred and legacy explicit-selection forms.
- `lookup_join(...)` returns a relation proxy whose fields read from the joined symbolic scope.
- For relation parameters and cached class input scopes, `lookup_join(...)` also makes later reads from that same proxy
  read from the joined scope.
- Inferred joins are valid only when `on` references exactly one unjoined relation.
- Field access on the joined scope is scoped and must not rely on unqualified string column names.
- Join calls execute in source order.
- Repeated joins of the same input must produce deterministic aliases.
- `inner_join(...)` is the v2 row-multiplying join form. It is valid when the business output is one row per right-side
  match.
- `rowset_join(...)` is the broad v2 rowset join form for right, full, cross, non-equi, and disjunctive joins.
- `left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)` are shortcuts over
  `rowset_join(...)`.
- `cross_join(...)` requires `allow_cartesian=True` and does not accept `on`.
- Rowset shortcuts compile to the canonical `rowset_join` operation with the specific join kind recorded on the join
  plan.

Documentation keeps ordinary joins bare and reads later fields from the joined relation proxy:

```python
left_join(on=order.customer_id == customer.id)
return OrderWithCustomer.base(order)(customer_name=customer.name)
```

Detailed lookup join condition, null, aliasing, cardinality, projection, and diagnostics behavior is specified by
[JoinSemantics.md](JoinSemantics.back.md)). Broad rowset join behavior is specified by
[FullPySparkJoinSupport.md](FullPySparkJoinSupport.back.md)).

## Hooks

Hooks are explicit PySpark escape hatches. A hook is a method decorated with `@raw`, placed in the Transform class
where it should execute.

Canonical forms:

```python
@raw(inout=lane(orders) | lane(orders))
def prepare(self, *, orders, spark, ctx):
    return orders
```

```python
@raw(inout=input(orders) | lane(orders))
def compare_to_raw(self, *, orders, spark, ctx):
    return orders
```

```python
@raw(inout=lane(published) | output(published), schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, published, spark, ctx):
    return published
```

Hook decorator keyword arguments:

- `input=...`, `output=...`, and `inout=sources | targets`: hook DataFrame bindings.
- `schema_mode`: output schema validation mode after the hook.
- `project_output`: whether extra hook-produced columns should be projected away after validation.
- `streaming_safe`: author promise used by streaming compatibility checks.

Rules:

- `@raw` has no step-method argument.
- Hook order is Transform class declaration order.
- A raw method before a step can explicitly select and replace that step's source lane.
- A raw method after a step can implicitly consume and replace the current lane, or explicitly select lanes.
- `input=...`, `output=...`, `inout=...`, `schema_mode`, `project_output`,
  `streaming_safe`, `target_backend`, and `target_platform` define the hook boundary.
- Hooks are not symbolically executed and are opaque to the compiler except for metadata, signature, declared options,
  provenance, and streaming compatibility classification.
- Every hook DataFrame binding, `spark`, and `ctx` must be keyword-only parameters. `input(name)` selects an original
  input, `lane(name)` selects the current lane, and `output(name)` selects a materialized output.
- Hooks must return a DataFrame at runtime.
- Generated code and execution call hooks on the source transform instance so hook behavior remains transparent.
- Generated code normally delegates hooks to the source transform. With `generated_code_options = ["embed_hooks"]`,
  Structure instead copies a standalone hook body into generated PySpark. The source form remains the authority for
  hook metadata and the copied body remains opaque to the compiler.
- Hooks may import and use PySpark because they execute at runtime, not during compiler phases.
- Hook metadata must be present in IR so generated code can call hooks and traceability can mark opaque boundaries.

`SchemaMode` must include at least the strict default mode and `SchemaMode.ALLOW_EXTRA_COLUMNS`. The exact enum names
for the default strict mode may be implementation-defined in v1, but public documentation should use the default by
omitting `schema_mode`.

## Validation Policy

`@validate_output(enabled)` overrides validation for one step-method output:

```python
@validate_output(False)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Rules:

- `enabled` must be a boolean.
- `@validate_output(...)` applies to the decorated compiled step method only.
- Method-level validation settings override class-level `@transform(validate_intermediate=...)`.
- Class-level settings override project defaults.
- Unknown validation decorator arguments are invalid.
- Validation policy must be recorded on `StepPlan`.
- Runtime validation placement must be identical for execution and generated-code execution.

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
- The default `execution_mode` value is `online`, which selects execution.
- Generated-code execution remains available through configuration.

Detailed runtime behavior is specified by [Execution.md](Execution.back.md)).

## Discovery and Metadata

The DSL must produce metadata sufficient for discovery and compilation:

```text
TransformDef
  source class
  declared inputs
  step methods
  expression helpers
  hooks
  validation policy
  streaming policy
  source locations when available
```

Rules:

- Discovery finds concrete `Transform` entrypoint classes under configured source roots.
- Metadata should preserve source order for input declarations, step methods, hooks, fields, filters, joins, and
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

- Public DSL objects keep backend-specific PySpark details out of the semantic model.
- IR should contain enough source context for actionable diagnostics and provenance.
- IR must preserve deterministic operation order.
- IR must be consumable by both PySpark execution and generated PySpark emission.
- Backend capability checks consume IR plus target metadata, not live Spark objects.

## Compileability Checks

The DSL frontend must reject source that cannot be lowered safely.

Required checks include:

- transform decorator usage;
- transform base class;
- input schema validity;
- step-method signature and source-order flow;
- reserved `run` method misuse;
- expression helper return validity;
- unsupported Python operators and methods;
- `where(...)` predicate type;
- output projection completeness;
- output assignment type and nullability compatibility;
- join condition support;
- `lookup_join(...)` uniqueness warnings;
- hook target and signature validity;
- validation decorator validity;
- streaming compatibility when enabled.

Checks must run without importing PySpark or starting Spark, except runtime-only checks explicitly owned by
`StructureSession` or a runtime runner.

## Diagnostics

Diagnostic code format, severity names, lifecycle rules, registry requirements, and stable documentation anchors are
owned by [Diagnostics.md](Diagnostics.back.md)). This section defines the DSL-specific context and message content that
DSL diagnostics must supply.

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
  @raw(lane=orders)
  def clean_customer_id(self, *, orders, spark, ctx):
      return orders.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

See docs/background/DSL.back.md
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

See docs/background/DSL.back.md
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

See docs/background/DSL.back.md
```

## Non-Goals

The following are outside v1 DSL scope:

- arbitrary Python control flow as a source of multiple dynamic DataFrame branches;
- step method branching and merging;
- custom transform constructor parameters;
- async, generator, classmethod, or staticmethod step methods;
- implicit Python UDF generation;
- Pandas UDF generation;
- RDD operations;
- automatic fallback from compiled expressions to hooks;
- automatic deduplication for `lookup_join(...)`;
- implicit or nondeterministic selected-row deduplication;
- advanced grouping sets, rollups, cubes, and rolling window helpers beyond admitted projection and selected-row
  helpers;
- streaming source, sink, trigger, checkpoint, and query lifecycle DSL;
- Spark Connect-specific public syntax;
- non-PySpark backends in v1.
