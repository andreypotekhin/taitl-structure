# Concepts

## DSL

The DSL has a structural public surface in `structure`—schemas, transforms, declarations, decorators, and runtime
invocation—and a plugin surface for symbolic operations. PySpark fields, expressions, filters, joins, and
aggregation helpers come from `structure.plugin.pyspark`.

The DSL is not a general wrapper around PySpark. A feature is compiler-visible only when Structure can capture
it, represent it in IR, check it, and lower it to optimizer-visible target operations.

Example source shape:

```python
from structure import *
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string(nullable=True)


class OrderNormalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = decimal(12, 2, nullable=False)


class Customer(Schema):
    id = string(nullable=False)
    name = string(nullable=False)


class OrderEnriched(OrderNormalized):
    customer_name = string(nullable=True)


class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    normalized = lane(OrderNormalized)
    enriched = output(OrderEnriched)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null(), order.customer_id.is_not_null())
        return OrderNormalized.project(order)(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )

    def publish(self, order: OrderNormalized, customer: Customer) -> OrderEnriched:
        lookup_join(on=customer.id == order.customer_id, how="left")
        return OrderEnriched.base(order)(customer_name=customer.name)
```

### Schema

A schema is a `Schema` class that describes a row contract: field names, order, types, nullability,
inheritance, aliases, metadata, and descriptions.

Schemas define input rows, intermediate rows, and output rows.

A schema is not a Spark DataFrame and does not contain data.

Example:

```python
from structure import *
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=True)
    total = decimal(12, 2, nullable=True)
```

### Field

A field is one named column in a schema.

Field metadata drives type checks, nullability, field order, generated Spark schema shape, projection checks,
and many diagnostics.

For example, `customer_id` is a non-null string field. The field can be referenced symbolically in a step,
where its declared type and nullability remain available to compiler checks:

```python
class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = decimal(12, 2, nullable=True)


class ValidateOrders(Transform):
    orders = input(OrderRaw)
    valid = output(OrderRaw)

    def valid_order(self, order: OrderRaw) -> OrderRaw:
        where(order.customer_id.is_not_null(), order.total >= 0)
        return OrderRaw.project(order)(
            id=order.id,
            customer_id=order.customer_id,
            total=order.total,
        )
```

`order.customer_id` is a field expression, not a value read from a particular row. Structure can therefore
check the filter and projection before a DataFrame is executed.

### Transform

A transform is a `Transform` subclass. It declares the pipeline surface: input DataFrames, output results, lanes,
step methods, expression helpers, and hooks. `@transform(streaming=True)` is optional and records class-level options
such as streaming compatibility.

A transform instance created with `EnrichOrders(orders=orders_df, customers=customers_df)` is a deferred invocation
that stores runtime inputs until `.run(session)` is called. The class below shows the declarations, two steps, a
lookup join, and the named result in one complete example.

Example:

```python
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    normalized = lane(OrderNormalized)
    enriched = output(OrderEnriched)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null(), order.customer_id.is_not_null())
        return OrderNormalized.project(order)(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )

    def publish(self, order: OrderNormalized, customer: Customer) -> OrderEnriched:
        lookup_join(on=customer.id == order.customer_id, how="left")
        return OrderEnriched.base(order)(customer_name=customer.name)


session = StructureSession(spark=spark, target="pyspark")
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
enriched_df = result.enriched
```

### Input

An input is a class-level `input(Schema)` declaration. Its attribute name becomes the runtime input name,
generated `run` parameter name, hook binding parameter name, and source scope name.

During symbolic execution, `self.orders` resolves to a symbolic input scope. During runtime, the invocation
stores the actual DataFrame under the same declared name.

Example:

```python
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)


invocation = EnrichOrders(orders=orders_df, customers=customers_df)
```

### Output

An output is a class-level `output(Schema)` declaration. Outputs are the public result names returned from
`TransformResult`, such as `result.enriched` or `result["enriched"]`.

Output declarations are part of the public transform contract.

Example:

```python
class OrderAccepted(OrderRaw):
    status = string(nullable=False)


class OrderRejected(OrderRaw):
    reason = string(nullable=False)


class ReviewOrders(Transform):
    orders = input(OrderRaw)
    accepted = output(OrderAccepted)
    rejected = output(OrderRejected)

    @step(input=orders, output=[accepted, rejected])
    def review(self, order: OrderRaw) -> tuple[OrderAccepted, OrderRejected]:
        return (
            OrderAccepted.base(order)(status="accepted"),
            OrderRejected.base(order)(reason="needs_review"),
        )


result = ReviewOrders(orders=orders_df).run(session)
accepted_df = result.accepted
rejected_df = result["rejected"]
```

### Lane

A lane is an intermediate DataFrame stream inside a transform. Lanes let a transform identify internal funnel
states, branch outputs, or disambiguate repeated schemas.

Lanes are neither constructor inputs nor public transform results. They are named internal flow states used by
method-level binding, hooks, IR, execution, and generated code.

Example:

```python
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    enriched = output(OrderEnriched)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized.project(order)(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )

    def publish(self, order: OrderNormalized) -> OrderEnriched:
        return OrderEnriched.base(order)(customer_name="unknown")
```

### Step method

A step method is a public instance method on a transform class that returns a `Structure` or schema tuple.
The compiler symbolically executes step methods in source order.

The first schema parameter is the driving row. Additional schema parameters are symbolic relations that must
be joined before their fields are used in filters or projections.

Example:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null(), order.customer_id.is_not_null())
        return OrderNormalized.project(order)(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )
```

The method receives a symbolic `order` scope during compilation. It does not run once for each row; the returned
projection becomes part of the transform plan.

### Binding of Inputs and Outputs

Method-level `@step` bindings with `input`, `output`, and `inout` select which declared input, lane, or output a
step method consumes or writes.

Binding is optional. Most single-lane transforms rely on inference. Explicit binding handles repeated schemas,
branches, funnel lanes, and lane names that intentionally shadow original inputs.

Example:

```python
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    enriched = output(OrderEnriched)

    @step(input=lane(normalized), output=enriched)
    def publish(self, order: OrderNormalized) -> OrderEnriched:
        return OrderEnriched.base(order)(customer_name="unknown")
```

Here `input=lane(normalized)` selects the intermediate lane rather than the original `orders` input. The explicit
`output=enriched` binding makes the public result clear even if another output uses the same schema.

### Expressions

An expression is a compiler-visible symbolic value: a field reference, literal, comparison, boolean
expression, cast, conditional, or helper call.

For PySpark, expressions carry type, nullability, referenced scopes, and source context without containing live
PySpark `Column` objects. Their target-specific representation belongs to the PySpark plugin and remains opaque to
Core.

Example:

```python
class ClassifyOrders(Transform):
    orders = input(OrderNormalized)
    classified = output(OrderEnriched)

    def classify(self, order: OrderNormalized) -> OrderEnriched:
        where(lower(trim(order.customer_id)) == "c-001")
        return OrderEnriched.base(order)(
            customer_name=when(order.total >= 1000, "priority").otherwise("standard"),
        )
```

The expression surface supports field references, Python literals, comparisons, arithmetic, Boolean `&`, `|`, and
`~`, null checks, null-safe equality, string predicates, casts, conditionals, collection indexing, and the
following helper families:

- String: `contains`, `like`, `ilike`, `rlike`, `lower`, `upper`, `trim`, `substring`, `split`,
  `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, and
  `levenshtein`.
- Struct and collection: `get_field`, `array[index]`, `map[key]`, `coalesce`, and `to_decimal`.
- Temporal: `date_add`, `datediff`, and `date_trunc`.
- Numeric and predicates: `abs`, `round`, `ceil`, `floor`, `isnull`, `isnotnull`, and `isnan`.

`cast`, `astype`, and `try_cast` are available for explicit type conversion; `try_cast` requires the PySpark 4
profile. `when` expressions must finish with `otherwise` before they are returned or used in another expression.

### Expression Helper

An expression helper function is a reusable compiler-visible function whose body returns Structure expressions.
Reachable ordinary helpers are compiled by default; `@special(type="expr")` is optional metadata for explicit intent
or named helper capture. When called with symbolic arguments, the helper expands as expression IR.

Expression helpers are Structure's preferred way to use reusable expression logic while keeping it visible to
compiler checks, traceability, execution, and generated code.

Ordinary helper (the default):

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def clean_id(self, value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null(), order.customer_id.is_not_null())
        return OrderNormalized.project(order)(
            id=self.clean_id(order.id),
            customer_id=self.clean_id(order.customer_id),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )
```

Use `@special(type="expr")` when explicit metadata or named helper rendering is useful:

```python
@special(type="expr")
def normalized_email(value):
    return lower(trim(value))


class NormalizeCustomers(Transform):
    customers = input(Customer)
    normalized = output(Customer)

    def normalize(self, customer: Customer) -> Customer:
        return Customer.project(customer)(
            id=normalized_email(customer.id),
            name=trim(customer.name),
        )
```

### Filter

A filter is recorded with `where(predicate)` inside a compiled step method. Multiple filters preserve source
order and combine with logical AND where legal.

Filters are operations in IR, not immediate DataFrame calls. A filter can reference only scopes available at
the point where it is recorded.

Example:

```python
class ReviewOrders(Transform):
    orders = input(OrderRaw)
    reviewed = output(OrderNormalized)

    def review(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(to_decimal(order.total, precision=12, scale=2) >= 0)
        return OrderNormalized.project(order)(
            id=order.id,
            customer_id=order.customer_id,
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )
```

### Join

A join is a symbolic relationship between the current row and a declared input. `lookup_join` is the main form:
a lookup-style join that selects at most one matching right-side row.

A join creates a joined scope. Fields from that scope can be used in later filters or in the returned output
schema.

Example:

```python
class EnrichOrders(Transform):
    orders = input(OrderNormalized)
    customers = input(Customer)
    enriched = output(OrderEnriched)

    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderEnriched:
        lookup_join(
            on=order.customer_id == customer.id,
            how="left",
            hint="broadcast",
        )
        return OrderEnriched.base(order)(customer_name=customer.name)
```

### Hook

A hook is an explicit PySpark escape hatch for arbitrary DataFrame code.

Use `@raw` for an explicit PySpark escape hatch. A raw method runs exactly where it appears in the Transform class;
it receives selected DataFrames as keyword-only parameters and returns the replacement frame or ordered tuple.

Hooks are opaque compiler boundaries. Structure validates metadata and signatures, preserves order, records
the boundary in IR and traceability, and calls the hook during execution. It does not inspect the hook body as
compiler-visible logic.

Example:

```python
class FillCustomerNames(Transform):
    orders = input(OrderEnriched)
    published = output(OrderEnriched)

    @raw(inout=orders | published, schema_mode=SchemaMode.STRICT, project_output=True)
    def fill_customer_name(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F

        return orders.withColumn(
            "customer_name",
            F.coalesce(F.col("customer_name"), F.lit("unknown")),
        )
```

### Session

`StructureSession` is the target-neutral runtime session. It owns the caller-supplied runtime, optional hook context,
library configuration, selected plugin target, execution mode, and optional artifact cache. PySpark users supply a
Spark session as that runtime.

The session does not own Spark lifecycle, orchestration lifecycle, reads, writes, or streaming query
management.

Example:

```python
session = StructureSession(
    spark=spark,
    ctx={"job_name": "order-enrichment"},
    target="pyspark",
)
result = session.run(EnrichOrders(orders=orders_df, customers=customers_df))
enriched_df = result.enriched
```

### Lowering

Lowering turns a higher-level semantic representation into a lower-level target representation.

In Structure, Core first establishes a structural transform plan. The selected plugin then lowers its opaque target
body to target recipes. For PySpark, those recipes are either interpreted during execution or rendered as generated
PySpark source.

Lowering must implement checked IR, not invent semantics.

Example:

```text
FieldRef("orders.customer_id")
  -> PySparkExpressionRecipe(field_reference=("orders", "customer_id"))
  -> F.col("orders.customer_id")
```

### Execution Plan

The PySpark execution plan is the shared target-level recipe model consumed by execution and the generated PySpark
emitter.

It decides expression mapping, filter order, projection field order, join aliases, hook order, validation
placement, literal typing, and guardrails once.

Example:

```text
PySparkStepRecipe normalize
  shared_operations: where(order.id.is_not_null())
  results:
    orders: select(id, customer_id, total)
```

## Next

QuickRef: [QuickRef.md](QuickRef.md)
