# Basic Concepts

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

### Session

`StructureSession` is the runtime session. It owns the caller-supplied Spark session, optional hook context, library configuration, execution mode, target backend, runner selection, and optional plan cache.

The session does not own Spark lifecycle, orchestration lifecycle, reads, writes, or streaming query management.

Example:

```python
session = StructureSession(spark=spark, ctx=ctx)
result = session.run(EnrichOrders(orders=orders_df))
enriched_df = result.enriched
```

### Lowering

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

### Execution Plan

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

## Next 

Get started: [GettingStarted.md](GettingStarted.md)

## 
