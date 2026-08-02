# Transform Inheritance And Composition

Structure supports two reuse shapes for transform code:

- inheritance, for a concrete transform that specializes reusable parent steps;
- `.to(...)` composition, for running complete transform invocations one after another.

Use inheritance when the child owns one logical pipeline and needs to override or extend pieces of that pipeline.
Use `.to(...)` when each stage is already a complete transform with declared `input(...)` and `output(...)`
boundaries.

## Transform Inheritance

A direct or indirect parent class inheriting from `Transform` may contribute inputs, lanes, outputs, expression
helpers, hooks, and step methods to a decorated child. Parent classes do not need `@transform` when they are reusable
fragments rather than standalone compiled transforms.

```python
class NormalizeBase(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    @step(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized.project(order)(
            customer_id=lower(trim(order.customer_id)),
        )


class PublishOrders(NormalizeBase):
    published = output(OrderPublished)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return project(order, OrderPublished)
```

Effective step order is parent-first:

```text
normalize
publish
```

Rules:

- inherited parent step methods run before child step methods;
- multiple direct parents run left to right in the Python class declaration;
- shared diamond ancestors contribute once;
- inherited declarations remain available to child methods and child overrides;
- a child method with the same name overrides the inherited scheduled step;
- sibling parents that define the same effective step method name are ambiguous unless the child overrides that name.

An override can call a parent implementation to schedule the parent as a separate step immediately before the child
step:

```python
class StrictPublishOrders(NormalizeBase):
    published = output(OrderPublished)

    @step(output=NormalizeBase.normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        normalized = super().normalize(order)
        where(normalized.customer_id.is_not_null())
        return OrderNormalized.project(normalized)
```

Supported parent-call forms are:

```python
super().normalize(order)
NormalizeBase.normalize(self, order)
super(NormalizeBase, self).normalize(order)
```

The parent step keeps its own hooks, validation boundary, lane writes, and traceability entry.
No other compiled step method may call another step method directly. Use source order and lanes inside one transform,
`Transform.to(...)` between complete transforms, private helpers for inline object construction, and `@special(type="expr")` for
reusable compiler-visible expressions.

## Runtime Composition

Use `.to(...)` to compose transform invocations at runtime:

```python
result = (
    NormalizeOrders(orders=orders_df)
    .to(AddProduct(products=products_df))
    .to(PublishOrders())
    .run(session)
)

published = result.recommended
```

`a.to(b, c)` and `a.to(b).to(c)` are equivalent. `Transform.to(a, b, c)` is the static starter when there is no
natural receiver:

```python
pipeline = Transform.to(
    NormalizeOrders(orders=orders_df),
    AddProduct(products=products_df),
    PublishOrders(),
)
```

Composition wires each downstream declared `input(...)` from exactly one source:

- a constructor argument supplied on that downstream invocation; or
- one output from the immediately preceding composed stage.

Matching uses schema identity. If several upstream outputs have the same schema, a matching output alias wins, then a
same-name output wins; if ambiguity remains, compilation fails. If a downstream input is both explicitly bound and
produced upstream, compilation fails rather than choosing a hidden precedence rule.

Use a transform boundary alias when a reusable transform publishes an implementation-oriented output field name but
downstream stages should consume a stable domain name:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")
```

Here `normalized` remains the declaration name, while `orders` is also accepted for composition and result lookup.
Input declarations may also use `.alias(...)` so constructor calls can use either the canonical input name or the alias.

For a stage graph with several public outputs, schemas can remain grouped with the transform contract while source
assignments are collected in one constructor-style block:

```python
class Merchandising(Transform):
    recommended_products = output(RecommendedProduct)
    recommendation_runs = output(RecommendationRun)

    outputs = output(
        recommended_products=recommended.recommended_products,
        recommendation_runs=recommended.recommendation_runs,
    )
```

The `stage(...)` wrapper is optional for class-body graph composition. A direct transform invocation assignment is
recognized as a stage, so equivalent graphs can omit the wrapper and keep output references readable:

```python
class Merchandising(Transform):
    catalog = input(Catalog)
    recommended = output(RecommendedProduct)

    normalized = NormalizeCatalog(catalog=catalog)
    selected = SelectRecommendations(normalized=normalized.products)
```

Only assignments whose value is a `Transform` invocation are interpreted as stages. Ordinary class assignments such as
`description = "catalog workflow"` remain ordinary Python values. Existing `stage(Transform(...))` declarations keep
their current behavior and can be mixed with implicit stages.

The output declaration order remains the public result order; keyword order only supplies the source mapping. Existing
`output(Schema, stage.output)` declarations remain supported.

When the transform class cannot be changed, use invocation-level `.rename(...)` to expose an output alias for that
stage:

```python
pipeline = NormalizeOrders(orders=orders_df).rename(normalized="orders").to(AddProduct(products=products_df))
```

Transform boundary `.alias(...)` and `.rename(...)` are separate from schema field `alias=...` and Spark DataFrame
`.alias(...)`. Field aliases name Spark columns; DataFrame aliases name Spark relations; transform boundary aliases
name inputs and outputs for Structure composition and result access.

The composed result exposes the final stage's declared outputs. Earlier-stage outputs are intermediate composition
state and are not returned unless a later stage publishes them.

## Generated-Capable Composition

Generated PySpark needs source-time metadata and a stable generated class name. Use a wrapper transform with exactly
one pipeline field:

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

The compiler flattens the stage plans into one generated class. Stage step names are prefixed with the stage name so
generated code and diagnostics remain readable:

```text
normalize_orders.normalize
add_product.add_product
publish_orders.publish
```

Class-field composition may bind stage constructor inputs to wrapper `input(...)` declarations. Runtime DataFrames are
not valid class-body bindings.

## Boundaries

Composition uses declared transform outputs as external boundaries. A stage cannot bind a downstream constructor input
to a `lane(...)` declaration because lanes are internal implementation state.

Hook-bearing stage transforms are not supported in `.to(...)` composition yet. Run hook-bearing transforms separately
until composition hook ownership is specified for both execution and generated-code execution.

Composition also does not interleave wrapper-local step methods with a wrapper pipeline in the first slice. Keep a
wrapper transform focused on the pipeline

## Choosing The Shape

Prefer schema inheritance and `SchemaClass.base(...)` when the output schema extends or combines existing row
contracts:

```python
class OrderPublished(OrderPublication, PublicationFlags):
    pass


return OrderPublished.base(order, flags)
```

Prefer transform inheritance when one transform specializes a parent pipeline:

```python
class RetailPublishOrders(NormalizeBase):
    ...
```

Prefer `.to(...)` composition when stages are independently useful transforms:

```python
NormalizeOrders(orders=orders_df).to(AddProduct(products=products_df), PublishOrders())
```

See also [SchemaInheritance.md](SchemaInheritance.back.md)), [DSL.md](DSL.back.md)), and
[ExecutionSemanticContract.md](ExecutionSemanticContract.back.md)).
