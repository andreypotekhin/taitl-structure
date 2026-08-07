# Transform Reference

A transform declares typed inputs, compiler-visible steps, optional hooks, and named outputs. Use this page when you
need to shape a pipeline, choose a binding form, add a filter or projection, or understand what runs at compile time
and runtime.

The [Transform background](../background/Transform.back.md) explains the source model and composition rules. The
[Transforms API](../api/Transforms.api.md) and the [Relations API](../api/Relations.api.md) provide the complete
operation inventories.

Examples use the `OrderRaw`, `OrderNormalized`, and related schemas introduced in the [Schema reference](Schema.ref.md).
Replace them with the schemas in your own application.

## Declare a transform

Declare a transform when a typed class should connect named inputs, steps, lanes, and outputs.

```python
from structure import *
from structure.plugin.pyspark import *


class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def clean(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized.project(order)(
            id=trim(order.id),
            total=to_decimal(order.total, precision=12, scale=2),
        )
```

`input(...)`, `lane(...)`, and `output(...)` declare named rowset boundaries. A step method returns a declared
`Schema` projection. The method body is captured symbolically; it is not called once per Spark row.

### Declarations

| Declaration | Purpose | Example |
| --- | --- | --- |
| `input(schema)` | Required transform input | `orders = input(OrderRaw)` |
| `input(schema, streaming=True)` | Streaming input declaration | `events = input(Event, streaming=True)` |
| `lane(schema)` | Named intermediate rowset | `clean = lane(OrderClean)` |
| `output(schema)` | Named final result | `published = output(OrderPublished)` |
| `output(schema).alias(name)` | Additional result lookup name | `output(OrderPublished).alias("orders")` |
| `stage(invocation)` | Explicit composed stage boundary | `clean = stage(Normalize(orders=orders))` |

Directly assigning a transform invocation, such as `clean = Normalize(orders=orders)`, is also a stage declaration.
Ordinary Python assignments that are not stages are ignored by the transform graph.

Use keyword arguments when constructing an invocation:

```python
result = NormalizeOrders(orders=orders_df).run(session)
published = result.normalized
```

Unknown or missing input names are errors. The constructor stores DataFrames and does not start a Spark action; see
the [Execution reference](Execution.ref.md) for the runtime boundary.

## Steps and bindings

An undecorated public method with supported schema input and return annotations is a step. Use `@step(...)` when the
graph needs explicit binding or step options.

```python
class PublishOrders(Transform):
    orders = input(OrderNormalized)
    accepted = output(OrderPublished)
    rejected = output(OrderRejected)

    @step(input=orders, output=accepted, cache=True)
    def publish(self, order: OrderNormalized) -> OrderPublished:
        where(order.total > 0)
        return OrderPublished.base(order)(status="accepted")
```

| Step option | Meaning |
| --- | --- |
| `input=` | Original input or existing lane consumed by the step |
| `output=` | Lane or output receiving the step result |
| `inout=` | Hook input/output binding, used with `@raw` |
| `cache=True` | Persist the completed step at Spark's default storage level |
| `cache=StorageLevel...` | Persist with an explicit PySpark storage level |
| `streaming=` | Declare or reject streaming compatibility for the transform |

Method-level `input=` and `output=` accept ordered lists for steps with multiple inputs or results. A lane with the
same name as an original input shadows that input for later inferred bindings.

Operations are applied in source order. Independent lanes remain independent; a step does not silently merge every
DataFrame currently in scope.

## Projections and filters

Use a schema constructor for the output contract. `project(...)` and `Schema.base(...)` preserve only the fields you
choose and make output shape explicit.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.customer_id.is_not_null())
    return OrderNormalized.project(order)(
        id=lower(trim(order.id)),
        customer_id=lower(trim(order.customer_id)),
        total=to_decimal(order.total, precision=12, scale=2),
    )


def add_status(self, order: OrderNormalized) -> OrderPublished:
    return OrderPublished.base(order)(
        status=when(order.total > 0, "paid").otherwise("free"),
    )
```

| Operation | Use |
| --- | --- |
| `where(predicate)` | Keep rows satisfying a symbolic Boolean expression |
| `Schema.project(row)(...)` | Build a complete declared projection |
| `Schema.base(row)(...)` | Retain the base fields and add or replace fields |
| `field.cast(type)` / `to_decimal(...)` | Make a conversion explicit |
| Ordinary helper / class | Reuse compiler-visible expression logic |
| `@special(type="expr")` | Optionally mark expression intent or preserve a named helper |
| `@special(type="udf")` | Opt into a row-local ordinary-PySpark UDF |

Python `and`, `or`, and expression truthiness are not symbolic operations. Use `&`, `|`, and `~` for Boolean
expressions. A `when(...)` expression must end with `.otherwise(...)` before it is used.

## Composition

Use composition when a reusable transform has its own inputs, outputs, or hook boundaries.

```python
class Fulfill(Transform):
    orders = input(OrderNormalized)
    inventory = input(Inventory)
    planned = output(FulfillmentPlan)

    plans = PlanFulfillment(orders=orders, inventory=inventory)
    planned = output(name=plans.plans)
```

Composition preserves the child transform's typed graph. A composed transform must resolve one target and compatible
streaming policy for every participating plugin service. Reuse a stage for a pipeline boundary; use a helper
expression for row-local logic.

## Hooks

`@raw(...)` is the explicit boundary for caller-supplied PySpark code. Structure checks the declaration and schema
policy,
but does not compile the hook body.

```python
class WithAudit(Transform):
    orders = input(OrderNormalized)
    published = output(OrderPublished)

    @raw(inout=orders | published, schema_mode=SchemaMode.STRICT)
    def audit(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F

        return orders.withColumn("audited", F.lit(True))
```

| Hook option | Meaning |
| --- | --- |
| `inout=` | Select the exact input and output boundaries |
| `schema_mode=SchemaMode.STRICT` | Require the hook result to match the declared schema |
| `schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS` | Permit additional hook columns |
| `target_backend=` | Restrict the hook to a target |
| `streaming=True` | Declare that the hook is streaming-safe |

Hooks run at their declared source-order boundary. Their selected DataFrames, target, schema mode, and validation
policy are visible in explain output and diagnostics. Raw SQL, RDDs, local row loops, and lifecycle calls are not
compiler-visible operations; keep them inside an explicitly application-controlled hook when they are necessary.

## Relations and analytical operations

Transforms can use the typed operation families documented in the focused API pages:

- [Joins API](../api/Joins.api.md) for lookup, rowset, existence, temporal, and as-of joins.
- [Aggregations API](../api/Aggregations.api.md) for grouping, metrics, selection, and deduplication.
- [Windows API](../api/Windows.api.md) for reusable and inline windows.
- [Collections API](../api/Collections.api.md) for arrays, maps, callbacks, and typed generators.
- [Expressions API](../api/Expressions.api.md) for fields, predicates, conversions, and SQL functions.
- [Relations API](../api/Relations.api.md) for ordering, set composition, assertions, hierarchy, sampling, and
  bounded scans.

The compiler rejects an operation when its type, nullability, cardinality, target capability, or streaming behavior
cannot be established. It does not silently turn an unsupported operation into a Python callback or UDF.

For example, a step can join a lookup, filter the joined scope, and publish a grouped result without leaving the typed
transform boundary:

```python
def summarize(self, order: Order, customer: Customer) -> CustomerTotal:
    left_join(customer, on=(order.tenant_id == customer.tenant_id) & (order.customer_id == customer.id))
    where(customer.is_active)
    group_by(tenant_id=order.tenant_id, customer_id=order.customer_id)
    return CustomerTotal(order_count=count(), gross_total=sum(order.total))
```

The same source-order rules apply when the step uses a window, collection callback, or relation-shape operation; choose
the focused API reference for the operation's cardinality and target conditions.

## Streaming

Declare streaming compatibility at the transform and input boundaries:

```python
@transform(streaming=True)
class WindowedOrders(Transform):
    events = input(OrderEvent, streaming=True)
    totals = output(OrderWindowTotal)
```

Streaming compatibility is a checked contract, not a query runner. Callers own `readStream`, `writeStream`, output
mode, triggers, checkpoints, sinks, and query lifecycle. Watermarks, event-time windows, bounded deduplication, and
admitted stream joins must meet the conditions in the [Streaming API](../api/Streaming.api.md). Broad analytic
windows, unbounded state, and arbitrary state processors remain outside the current transform contract.

## Compile, explain, and run

Compile for a Spark-free plan check, then run when the application is ready to evaluate the transform.

```python
plan = NormalizeOrders.compile(project_root=".")
result = NormalizeOrders(orders=orders_df).run(session)
```

`compile(...)` performs source discovery, symbolic capture, type and capability checks, and target planning without
binding live DataFrames. `run(session)` executes the checked meaning through the selected runtime. The CLI commands
`structure check`, `structure compile`, and `structure explain` are Spark-free; see the [CLI reference](CLI.ref.md).

Online and generated-code execution must agree on source order, aliases, projections, validation placement, hook order,
result names, and output schemas. A generated file is build output; edit the Structure source or configuration and
regenerate it instead of changing the generated file by hand.

## Common corrections

| Diagnostic situation | Correction |
| --- | --- |
| A step uses Python `and` or `or` | Use symbolic `&` or `\|` and parenthesize each condition |
| A hook result has an unexpected column | Use strict mode, project the hook output, or allow extras |
| A transform cannot compile without Spark | Move Spark startup and runtime-only code out of imported source modules |
| A method consumes the wrong lane | Declare `input=` and `output=` explicitly |
| A generated run cannot import its class | Run `structure compile` or switch to `execution_mode = "online"` |
| An operation is rejected as unsupported | Use the linked API page for its target/streaming boundary or use an
  explicit raw hook |

## Detailed binding rules

The first schema parameter is the driving relation. Additional typed relation parameters are scopes that must be joined
before their fields are used in a filter or projection. Bind them explicitly when more than one declaration has the
same schema or when a branch is easier to understand by name.

| Situation | Recommended binding |
| --- | --- |
| One unambiguous input and output | Annotations and ordinary step discovery |
| Repeated input schemas | `@step(input=[left, right], output=result)` |
| One input and multiple results | `@step(input=source, output=[accepted, rejected])` |
| Hook input and output | `@raw(inout=source \| target)` |
| Same-named input and lane | Role selectors such as `input(source)` or `lane(source)` |

```python
class EnrichOrders(Transform):
    orders = input(OrderNormalized)
    customers = input(Customer)
    enriched = output(OrderWithCustomer)

    @step(input=[orders, customers], output=enriched)
    def add_customer(
        self, order: OrderNormalized, customer: Customer
    ) -> OrderWithCustomer:
        left_join(customer, on=customer.id == order.customer_id)
        return OrderWithCustomer.base(order)(customer_name=customer.name)
```

Step methods cannot call other step methods directly. Use source order and lanes for one logical pipeline, an
invocation-level `.to(...)` for independent transforms, or ordinary reachable helpers for reusable scalar logic. Async,
generator, classmethod, and staticmethod forms are not compiled step forms.

### Multiple outputs and branches

Use multiple outputs when one step intentionally publishes separate typed branches from the same input.

```python
class SplitOrders(Transform):
    orders = input(OrderRaw)
    accepted = output(OrderAccepted)
    rejected = output(OrderRejected)

    @step(input=orders, output=[accepted, rejected])
    def split(self, order: OrderRaw) -> tuple[OrderAccepted, OrderRejected]:
        return (
            OrderAccepted.base(order)(status="accepted"),
            OrderRejected.base(order)(reason="review"),
        )
```

The result tuple order is the declaration order in `output=[...]`. Filters and projections belong to the branch that
receives them. A rejected branch reading an unfiltered lane does not see filters applied to another output branch.

`output(schema).alias(...)` adds a lookup or composition name; it does not create another output key. Canonical output
names remain stable for iteration, generated schemas, and traceability.

## Composition and inheritance details

Composition matches exact schema identity. If several upstream outputs match, an output alias wins, then a same-name
output wins; unresolved ambiguity is an error. Internal lanes cannot satisfy a composition binding. Cycles, unresolved
outputs, cross-target pipelines, and wrapper-local interleaving without an explicit contract are rejected before
runtime.

Use `.rename(...)` only for invocation-result lookup or composition aliases. Keep these namespaces distinct:

| Name | Changes |
| --- | --- |
| Schema field alias | Physical Spark column name for a schema field |
| Transform boundary alias | Additional input/output boundary name |
| Invocation result alias | Lookup name on one composed invocation |
| DataFrame alias | Spark scope name used by a join |

Transform inheritance is parent-first: direct parents are processed left to right, shared diamond ancestors once, then
local declarations. A child method with the same scheduled name overrides the inherited step. If sibling parents define
the same effective method name, the child must resolve it explicitly. A supported `super()` call preserves the parent
step's hook, validation, lane, and traceability boundary when the parent implementation is scheduled.

Use inheritance for one logical pipeline with a stable step flow. Use `.to(...)` for independent complete transforms.
Do not use inheritance to dispatch dynamically on a runtime schema subclass; schema identity and source-order planning
remain static.

```python
class NormalizedOrders(BaseOrders):
    def clean(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized.project(order)(id=trim(order.id))


published = PublishOrders.to(NormalizedOrders)
```

Inheritance keeps one scheduled graph. The `.to(...)` form composes two complete transforms and keeps their input and
output boundaries separate.

## Relation operation families

| Family | Representative operations | Main contract |
| --- | --- | --- |
| Projection/filter | `Schema.project`, `Schema.base`, `where` | Declared fields and Boolean predicates |
| Joins | `lookup_join`, `left_join`, `exists`, `temporal_one` | Cardinality, aliases, nullability, and tie policy |
| Aggregation | `group_by`, `sum`, `count`, `having` | New output grain and metric result types |
| Windows | `window`, `row_number`, `lag`, `rolling_sum` | Partition, order, and frame |
| Collections | `arr_transform`, `map_filter`, `explode_struct` | Symbolic callbacks and typed cardinality |
| Relation shape | `union_all`, `order_by`, `limit`, assertions | Duplicates, ordering, bounds, and failure policy |

Set operations make no ordering promise; an ordered `limit(...)` or `offset(...)` needs a valid explicit
`order_by(...)` first. Relation operations do not perform driver collection, RDD conversion, hidden actions, or
implicit UDF conversion. Use the focused API pages for signatures and target parity.

```python
ordered = order_by(order.created_at.desc(), order.id.asc())
limited = ordered.limit(100)
```

The explicit order makes the limit reproducible; applying `limit(...)` without an order does not define which rows are
retained.

### Explicit caching

`cache=True` is a step directive, not an optimizer guess:

```python
@step(output=normalized, cache=StorageLevel.MEMORY_AND_DISK)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized.project(order)
```

The directive is retained in direct and generated execution. It does not transfer DataFrame control to Structure and
does not make an operation admissible when the selected target cannot honor the requested storage level.

## Source-module safety and diagnostics

Compiler-imported modules should declare classes, schemas, constants, and pure helpers only. Do not start Spark, read or
write storage, contact a service, start a streaming query, or perform an action at import time. Put runtime-only work
in the caller or an explicit hook. This rule keeps `structure check`, `structure compile`, and `structure explain`
Spark-free and makes discovery deterministic.

Transform diagnostics should identify the transform, input or output, step method, operation, source location, and
shortest valid correction. Typical failures include unknown input names, missing outputs, ambiguous composition wiring,
unsupported operations, invalid hook signatures, incompatible assignments, and backend capability gaps.

```python
# Safe to import during `structure check`: declarations and pure helpers only.
class Normalize(Transform):
    orders = input(OrderRaw)
    cleaned = output(OrderClean)

    def clean(self, order: OrderRaw) -> OrderClean:
        return OrderClean.project(order)(id=trim(order.id))

# Create Spark sessions, read inputs, and start queries in the application entry point.
```

```text
CompileError DSL-E0402: Invalid transform structure

Step:
  add_customer(order: OrderNormalized, customer: Customer)

Problem:
  More than one declared input or lane matches Customer.

Use:
  Add @step(input=[orders, customers], output=enriched) or rename the declarations.
```

```text
CompileError HOOK-E0701: Invalid hook signature

Hook:
  PrepareOrders.remove_negative_totals

Use:
  def remove_negative_totals(self, *, prepared, spark, ctx):
      return prepared
```

## Before running a transform

- Every input, lane, and output has the intended Schema identity.
- Repeated schemas use explicit `input=` and `output=` bindings.
- Every joined scope is joined before it is projected.
- Output projections state field order, aliases, and conversions explicitly.
- Row-shaping operations have a declared cardinality and ordering policy.
- Hooks declare their exact boundary, target, streaming promise, and schema mode.
- Source modules remain safe to import without Spark.
- Streaming inputs have a compatible operation path and caller-controlled lifecycle.
- The same source can execute online or through generated code without changing meaning.

## Reuse decision guide

| Reuse need | Preferred shape |
| --- | --- |
| Same row contract with extra fields | Schema inheritance and `Schema.base(...)` |
| Same logical pipeline with specialized steps | Transform inheritance |
| Independent complete pipelines | Invocation-level `.to(...)` |
| Reusable typed scalar logic | Ordinary helper (optional `@special(type="expr")`) |
| Target-specific arbitrary DataFrame code | `@raw(...)` |

Keep the smallest useful boundary. A scalar helper should not return a DataFrame; a raw hook should not be used to hide
logic that the compiler can type and explain; an inherited transform should not become a runtime schema-dispatch
mechanism.

```python
@special(type="expr")
def normalized_id(value):
    return lower(trim(value))


class CleanOrders(Transform):
    orders = input(OrderRaw)
    cleaned = output(OrderClean)

    def clean(self, order: OrderRaw) -> OrderClean:
        return OrderClean.project(order)(id=normalized_id(order.id))
```

Use a scalar expression helper for row-local reuse; reserve a composed transform for a separate typed relation flow.

## Generated files

Generated PySpark is optional build output. When a project commits it, regenerate after source or configuration changes,
review the diff, and use `structure compile --fail-on-diff` in CI. Generated classes should contain explicit DataFrame
and Column operations, stable lane names, input/intermediate/output validation, and no hidden actions or lifecycle
management. A source-backed hook or UDF remains delegated unless the matching embedding option is selected and its
standalone restrictions are satisfied.

```bash
structure compile
git diff -- generated/
structure compile --fail-on-diff
```

The source transform and configuration remain authoritative; the generated directory is the reviewable artifact.

## See also

- [Execution reference](Execution.ref.md)
- [Schema reference](Schema.ref.md)
- [API reference](API.ref.md)
- [Transform background](../background/Transform.back.md)
