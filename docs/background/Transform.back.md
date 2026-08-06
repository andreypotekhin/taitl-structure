# Transform

Transforms are Structure's compiler-visible units of DataFrame work. A transform declares named schema inputs and
outputs, expresses rowset operations in ordinary Python, and can run directly or produce generated PySpark. The same
checked transform meaning feeds execution, generation, diagnostics, explain output, traceability, and streaming
compatibility analysis.

The [Transforms API](../api/Transforms.api.md) and the related API tables provide the concise callable inventory. This
background gathers the authoring, composition, and compiler-visible rules in the order a reader needs to understand a
transform: declaration, invocation, operations, reuse, and compilation boundaries.
The normative sources are [DSL](../dev/specifications/DSL.md),
[Typed Relation Operations](../dev/specifications/TypedRelationOperations.md),
[Hook Semantics](../dev/specifications/HookSemantics.md), and
[Execution Semantic Contract](../dev/specifications/ExecutionSemanticContract.md).

## The Transform Contract

A transform has four connected parts:

1. named `input(...)` declarations identify the expected input schemas;
2. ordered step methods express compiler-visible DataFrame operations;
3. `lane(...)` and `output(...)` declarations describe internal and public row flows;
4. runtime invocation or generated code materializes the checked plan.

The DSL is not a row-wise Python wrapper and does not generate implicit UDFs. Structure symbolically evaluates compiled
step methods so Spark can retain optimizer-visible DataFrame and `Column` operations. Arbitrary runtime code belongs at
an explicit hook boundary and is outside the compiler-visible portability promise.

### Source-Time And Runtime Responsibilities

Importing a transform module records declarations and metadata only. It must not create a `SparkSession`, inspect a
live DataFrame, read or write storage, contact a service, start a streaming query, or perform a Spark action. Compiler
commands later discover the source, symbolically execute step methods, check capabilities, and build a deterministic
plan. Runtime invocation supplies the caller-owned session and DataFrames to that checked plan.

The same plan is the source of truth for online execution and generated PySpark. The two modes may differ in Python
representation and generated formatting, but not in input binding, step order, projection order, filters, joins, hooks,
validation boundaries, or output schemas.

### Transform Options

`@transform` is optional for an ordinary concrete transform. Use it when class-level policy belongs with the transform:

```python
@transform(validate_intermediate=False, streaming=True)
class EnrichOrders(Transform):
    orders = input(OrderRaw, streaming=True)
    enriched = output(OrderEnriched)

    def enrich(self, order: OrderRaw) -> OrderEnriched:
        return OrderEnriched(
            id=order.id,
            total=to_decimal(order.total, precision=12, scale=2),
        )
```

The decorator accepts keyword options only. `validate_intermediate=False` changes intermediate validation policy;
`streaming=True` makes incompatible or unknown streaming shapes errors when compatibility checks are enabled. The
marker does not start streaming execution. The class must inherit from `Transform`, and its class body must remain
import-safe.


## Canonical Source Shape

```python
from structure import *
from structure.plugin.pyspark import *


@transform
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    @step(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=order.id,
            customer_id=lower(trim(order.customer_id)),
            total=to_decimal(order.total, precision=12, scale=2),
        )
```

Transform constructors bind declared inputs by keyword. Construction is deferred and performs no Spark action. A
`StructureSession` supplies the caller-owned runtime and hook context; it does not belong in transform constructors.

Reusable parent classes may inherit from `Transform` without being decorated or compiled as standalone entrypoints.
Project discovery compiles concrete classes that declare final outputs or a class-field pipeline. This keeps reusable
pipeline fragments available to child transforms without turning every base class into a generated artifact.


## Inputs, Lanes, And Outputs

`input(schema)` declares a named DataFrame input. The schema must be a `Schema` class, declaration names follow class
body order, and duplicate names after inheritance are invalid. During symbolic execution, `self.orders` is an input
scope rather than a live DataFrame. Generated `run(...)` methods retain the same keyword-only input names.

`lane(schema)` names internal transform state. A lane can be written by one step and read by later steps, but it is not
a public composition boundary. `output(schema)` declares a public result and may name the source lane or expression.
Output declarations determine the public result order.

Step methods normally return a schema constructor projection. A method may use `where(...)`, expressions, joins,
windows, aggregates, collections, and other supported operations from the plugin API. Operations are applied in source
order.

### A Multi-Step Funnel

Use lanes for intermediate contracts and outputs for public results. The method decorators make branch destinations
explicit when more than one matching schema is available:

```python
class RouteOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    accepted = output(OrderAccepted)
    rejected = output(OrderRejected)

    @step(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=order.id,
            customer_id=lower(trim(order.customer_id)),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    @step(output=accepted)
    def accept(self, order: OrderNormalized) -> OrderAccepted:
        where(order.customer_id.is_not_null())
        return OrderAccepted.base(order)(status="accepted")

    @step(output=rejected)
    def reject(self, order: OrderNormalized) -> OrderRejected:
        where(order.customer_id.is_null())
        return OrderRejected.base(order)(reason="missing customer")
```

Methods execute in source order. Each method's filter affects only the lane or output it writes. The rejected branch
therefore still reads the unfiltered `normalized` lane. A lane is internal state; it is never returned as a public
output unless an explicit output declaration receives it.

### Explicit Bindings And Multiple Results

When schemas repeat, bind sources and destinations by declaration rather than relying on parameter-name inference:

```python
class SplitOrders(Transform):
    orders = input(OrderRaw)
    accepted = output(OrderAccepted)
    rejected = output(OrderRejected)

    @step(input=orders, output=[accepted, rejected])
    def split(
        self, order: OrderRaw
    ) -> tuple[OrderAccepted, OrderRejected]:
        return (
            OrderAccepted.base(order)(status="accepted"),
            OrderRejected.base(order)(reason="review"),
        )
```

`input=...` and `output=...` select declared inputs, lanes, or outputs. `input=[...]` and `output=[...]` bind fixed
parameter or result tuples in order. `inout=source | target` is the compact one-source/one-target form. Role selectors
such as `input(orders)`, `lane(orders)`, and `output(published)` distinguish the original input from a same-named
working lane after source-order shadowing.

### Step Method Rules

A public instance method whose return annotation is a `Schema` class is a compiled step. A fixed tuple of schema
annotations is also allowed for a multi-result step. The first schema parameter is the driving relation; later relation
parameters are typed scopes that must be joined before their fields are used in filters or projections.

```python
class EnrichOrders(Transform):
    orders = input(OrderNormalized)
    customers = input(Customer)
    enriched = output(OrderWithCustomer)

    @step(input=[orders, customers], output=enriched)
    def add_customer(
        self, order: OrderNormalized, customer: Customer
    ) -> OrderWithCustomer:
        left_join(
            customer,
            on=customer.id == order.customer_id,
        )
        return OrderWithCustomer.base(order)(customer_name=customer.name)
```

Private helper methods and public helpers without schema return annotations are not pipeline steps. When reached from a
compiled step, however, ordinary helper methods and classes are symbolically evaluated like inline compiler logic:
supported Structure expressions compile to optimized PySpark, and unsupported Python fails with a compiler diagnostic.
Use `@special(type="expr")` only when explicit expression intent or named embedded helper rendering is useful. Use
`@special(type="ignore")` to declare code that must remain outside the compiler-visible path. Async, generator,
classmethod, and staticmethod step forms are outside the compiled DSL. Step methods cannot call other step methods
directly, because
source order, lanes, and explicit composition own pipeline flow.


## Invocation And Composition

Invoke a transform with declared input names and run it with a session:

```python
result = NormalizeOrders(orders=orders_df).run(session)
```

The session owns runtime configuration and caller context:

```python
session = StructureSession(spark=spark, ctx={"request_id": request_id})
invocation = NormalizeOrders(orders=orders_df)
result = session.run(invocation)
```

`invocation` can be constructed, inspected, composed, and passed between application layers before execution. Neither
constructor form nor `session.run(...)` transfers Spark-session or DataFrame ownership to Structure.

Unknown and positional constructor arguments are rejected. Missing inputs fail no later than `run(session)`. `run` is
reserved for runtime execution, so a public schema-returning step method named `run` is invalid.

Complete transform invocations can be chained with `.to(...)`:

```python
result = (
    NormalizeOrders(orders=orders_df)
    .to(AddProduct(products=products_df))
    .to(PublishOrders())
    .run(session)
)
```

`.to(...)` accepts invocations, not classes. `a.to(b, c)` and `a.to(b).to(c)` flatten to the same pipeline. Use
invocation-level `.to(...)` for dynamic composition without a wrapper class. A downstream input is supplied by its
constructor binding or by one matching upstream output; supplying both is ambiguous and fails.

Composition matches exact schema identity. If several upstream outputs share a schema, a matching output alias wins,
then a same-name output wins; unresolved ambiguity fails. Internal lanes cannot be composition bindings. The composed
result exposes the final stage's declared outputs.

Transform boundary aliases are separate from schema field aliases and DataFrame aliases:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")
```

The declaration name remains `normalized`, while `orders` is also accepted as a composition and result-lookup alias.
Invocation-level `.rename(...)` supplies an alias when the transform class cannot be changed.

Alias resolution is deterministic:

```python
result = (
    NormalizeOrders(orders=orders_df)
    .rename(normalized="clean_orders")
    .run(session)
)
```

Schema field aliases, transform boundary aliases, invocation result aliases, and Spark DataFrame aliases are separate
namespaces. Renaming an invocation result does not rename schema fields or generated Spark columns.

Generated-capable authored composition uses a wrapper with bare transform assignments:

```python
class OrderPipeline(Transform):
    orders = input(OrderRaw)
    products = input(Product)

    normalized = NormalizeOrders(orders=orders)
    enriched = AddProduct(
        normalized=normalized.normalized,
        products=products,
    )
    published = PublishOrders(enriched=enriched.enriched)
    result = output(published=published.published)
```

The compiler flattens stage plans into one generated class and prefixes stage step names so source and diagnostics
remain readable. Hook-bearing stages retain their declaring owner. Online and delegated generated execution create one
private implementation instance per hook-owning stage; embedded generated hooks are copied only when every included hook
meets the standalone embedding rules. Hook order, bindings, validation, traceability, and streaming classification do
not change through composition. Wrapper-local hooks and steps, cross-target hook pipelines, and exposing internal lanes
as composition outputs remain deferred.

An equivalent direct stage graph is also valid when a class-field assignment itself is the stage declaration:

```python
class OrderPipeline(Transform):
    orders = input(OrderRaw)
    products = input(Product)

    normalized = NormalizeOrders(orders=orders)
    enriched = AddProduct(
        orders=normalized.normalized,
        products=products,
    )
    outputs = output(name=enriched.enriched)
```

The compatibility `stage(...)` wrapper remains supported. Ordinary class assignments that are not transform invocations or
declared output mappings remain ordinary Python values and do not become pipeline stages.


## Composition Graph Details

Class-field assignments whose values are transform invocations may form a dependency graph, while ordinary assignments
remain ordinary Python values. Existing explicit stage declarations remain supported. A wrapper may collect stage outputs
in one output mapping, but its public output declaration order remains authoritative. Stage constructor inputs bind to
wrapper inputs, not runtime DataFrames created during class definition. The compiler flattens a valid graph into one
generated artifact, prefixes stage methods, retains hook owners, and rejects cycles, unresolved outputs, cross-target
pipelines, internal-lane bindings, and wrapper-local interleaving that has no explicit contract.

## Transform Inheritance

Use transform inheritance when one logical pipeline builds upon reusable parent steps. Parent classes may contribute
inputs, lanes, outputs, expression helpers, hooks, and step methods without being decorated standalone transforms.

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

Effective step order is parent-first. Direct parents run left to right, shared diamond ancestors contribute once, and
inherited declarations remain available to child methods. A child method with the same name overrides the inherited
scheduled step. Sibling parents defining the same effective method name require a child override.

An override may explicitly schedule the parent implementation immediately before the child step:

```python
class StrictPublishOrders(NormalizeBase):
    published = output(OrderPublished)

    @step(output=NormalizeBase.normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        normalized = super().normalize(order)
        where(normalized.customer_id.is_not_null())
        return OrderNormalized.project(normalized)
```

Supported parent-call forms are `super().method(row)`, `Base.method(self, row)`, and
`super(Base, self).method(row)`. The parent retains its hooks, validation boundary, lane writes, and traceability entry.
Compiled step methods may not call other step methods directly; use source order and lanes, private inline helpers,
invocation-level `.to(...)`, or ordinary reachable helpers for reusable compiler-visible expressions. `@special(type="expr")`
is optional when explicit metadata or named rendering is useful.

The parent implementation remains a separate plan step when it is scheduled explicitly:

```python
class AuditedPublish(StrictPublishOrders):
    audited = output(OrderAudited)

    def publish(self, order: OrderPublished) -> OrderAudited:
        return OrderAudited(
            id=order.id,
            status=order.status,
            audited_at=current_timestamp(),
        )
```

Use inheritance for one logical pipeline with a stable step flow. Use `.to(...)` for independent complete transforms.
Do not use inheritance to dispatch dynamically on a runtime schema subclass; schema identity and source-order planning
remain static.


## Source Modules and Expression Boundaries

Source roots are resolved from CLI overrides, `[tool.structure]`, `structure.toml`, then the `src` default or project
root fallback. A source-root-relative path becomes its import path; duplicate paths across roots are errors, and
generated paths mirror that import identity. Discovery preserves class-body order for fields, inputs, steps, helpers,
hooks, and validation decorators. Modules imported during compiler commands must declare objects only: Spark startup,
data reads, writes, network calls, services, actions, and large data parsing belong behind runtime entrypoints or hooks.

Compiled expressions use typed field references, literals, operators, ordinary reachable helpers, and optional
`@special(type="expr")` helpers.
Python truthiness and control flow over symbolic values, Python string methods, raw SQL column paths, arbitrary
callbacks,
and runtime objects are rejected. Use `&`, `|`, `~`, and `when(...)` for symbolic logic; use an explicit hook for target
code that has no Structure-level contract. These boundaries preserve Spark optimizer visibility and compiler speed.

An ordinary helper is compiler-visible when symbolic execution reaches it. Prefer the explicit decorator only when the
intent should be documented or generated code should preserve a named helper:

```python
def clean_customer_id(value):
    return lower(trim(value))


class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=order.id,
            customer_id=clean_customer_id(order.customer_id),
        )
```

This helper is pure, deterministic, and expanded into typed expression IR. A helper that returns a DataFrame, performs
an action, or depends on arbitrary runtime state belongs behind `@raw` instead.

Unsupported Python control flow is not silently converted to a UDF:

```python
# Invalid in a compiled step:
if order.customer_id:
    value = order.customer_id.strip().lower()

# Use symbolic operations instead:
value = lower(trim(order.customer_id))
```

Use `&`, `|`, and `~` for symbolic boolean logic. Python `and`, `or`, `not`, symbolic truthiness, raw SQL strings,
row-wise callbacks, RDD conversion, and local collection are outside the compiler-visible DSL.


## Typed Relation Operations

Whole-relation operations consume only declared inputs, lanes, or prior compiler-visible relation results. Each
operation
has an immutable recipe, capability name, provenance, cardinality, null/empty behavior, duplicate and ordering behavior,
and batch/streaming/Spark Connect classification. Relation operations cannot occur inside scalar lambdas, aggregate
assignments, or window expressions unless a narrower specification admits that composition.

The admitted families include:

- `posexplode_struct(...)` for a typed row-expanding `array<struct>` relation, with ordinal fields and zero rows for
  null or empty arrays in the inner form;
- `union_all(...)`, `union_by_name(...)`, `intersect(...)`, `intersect_all(...)`, `subtract(...)`, and `except_all(...)`
  with explicit duplicate and schema contracts;
- `relation_alias(...)` for a typed self-join occurrence;
- `order_by(...)`, deterministic `limit(...)`, `offset(...)`, and seeded `sample(...)`;
- `exactly_one(...)`, `require_unique(...)`, `require_all(...)`, and `require_reference(...)` relation assertions;
- bounded `require_parent_hierarchy(...)` and `hierarchy_closure(...)` catalog operations;
- first-qualified priority selection and branchable typed union where the owning specification admits the shape.

### Typed Array-Of-Struct Expansion

Generators declare the shape of their generated row scope. An inner generator emits zero rows for a null or empty
array, and a positional generator adds a zero-based ordinal:

```python
class OrderItem(Schema):
    sku = string(nullable=False)
    quantity = integer(nullable=False)


class PositionedItem(Schema):
    ordinal = long(nullable=False)
    sku = string(nullable=False)
    quantity = integer(nullable=False)


def expand_items(self, order: OrderWithItems) -> ExpandedItem:
    item = posexplode_struct(order.items, as_=PositionedItem)
    return ExpandedItem(
        order_id=order.id,
        ordinal=item.ordinal,
        sku=item.sku,
        quantity=item.quantity,
    )
```

`explode_struct(...)`, `posexplode_struct(...)`, and the outer or inline variants remain typed relation operations. The
array element schema, nullability, cardinality, source provenance, and streaming classification are part of the plan.

### Set Composition And Branches

Set operations act on the active relation, not on a scalar expression. Exact-schema operations preserve duplicates or
apply the named set semantics, and make no ordering promise:

```python
def add_fallbacks(self, primary: Signal, fallback: Signal) -> Signal:
    union_all(fallback)
    return Signal(
        key=primary.key,
        score=primary.score,
    )
```

Use `union_by_name(...)` when physical field names are the intended alignment. `allow_missing_columns=True` is a
batch-only schema-evolution form and may fill only fields whose nullability or explicit default contract permits it.
`intersect(...)`, `intersect_all(...)`, `subtract(...)`, and `except_all(...)` retain their distinct or duplicate
behavior rather than collapsing into one generic set operation.

### Ordered Bounds And Assertions

Bounds are deterministic only after an explicit order, and relation assertions preserve the typed relation on success:

```python
def top_events(self, event: Event) -> Event:
    order_by(event.account_id, event.sequence)
    limit(100)
    return Event(
        account_id=event.account_id,
        event_id=event.event_id,
        sequence=event.sequence,
    )


def use_policy(self, policy: Policy) -> Policy:
    exactly_one(policy)
    return Policy(
        name=policy.name,
        threshold=policy.threshold,
    )
```

`offset(...)` and `sample(...)` have the same explicit-bound and reproducibility requirements. `require_unique(...)`,
`require_all(...)`, and `require_reference(...)` report registered relation diagnostics at Spark evaluation; they do not
collect rows to the driver or turn failures into silent filtering.

Online and generated execution use the same public PySpark recipes. Relation operations must not hide actions, RDD or
Pandas conversion, raw SQL, implicit UDFs, or driver collection. Set operations preserve or explicitly document
duplicates and make no ordering promise; ordering-dependent `limit(...)` and `offset(...)` require a preceding valid
`order_by(...)` that no later row-shaping operation has invalidated.


## Compiler-Visible Operations

The compiler understands schema constructors, field references, Python literals, reachable helper logic, `where(...)`,
joins, aggregation and window operations admitted by the API tables, hooks at explicit boundaries, and validation
declarations.
Each operation remains a DataFrame or `Column` operation after lowering.

Common operations include:

- projections using schema constructors and `Schema.base(...)`;
- row-local expressions and explicit conversions;
- filters using compiler-visible boolean expressions;
- joins with declared semantics, aliases, and cardinality rules;
- aggregations and admitted window or selected-row helpers;
- collection and higher-order functions supported by the selected target profile.

### Explicit Step Caching

Caching is a step directive, not an implicit compiler optimization:

```python
class ReuseOrders(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)
    published = output(OrderPublished)

    @step(output=normalized, cache=True)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized.project(order)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return OrderPublished.project(order)
```

`cache=True` uses the target's default persistence level. A PySpark `StorageLevel` may be supplied for an explicit
level. The directive is recorded in the plan and preserved in online and generated execution; it does not make a step
cacheable when the target profile cannot honor that contract.

### Join And Aggregate Example

Step parameters identify the driving relation and the typed relation to join. The join predicate is symbolic and the
result projection remains explicit:

```python
class EnrichAndSummarize(Transform):
    orders = input(OrderNormalized)
    customers = input(Customer)
    summary = output(CustomerOrderSummary)

    def summarize(
        self, order: OrderNormalized, customer: Customer
    ) -> CustomerOrderSummary:
        left_join(
            customer,
            on=(customer.id == order.customer_id),
            hint="broadcast",
        )
        group_by(customer_id=order.customer_id)
        return CustomerOrderSummary(
            customer_id=order.customer_id,
            order_count=count(),
            order_total=sum(order.total),
        )
```

Join type, aliases, nullability, cardinality, deduplication, and hints are compiler-visible plan decisions. Group keys
name the aggregate output fields; aggregate expressions determine their result types and nullability. A lookup join
should declare its uniqueness or deduplication policy when the source relation is not known to be unique.

### Window And Selected-Row Example

Window helpers remain Spark-plan-visible and require explicit partition and order semantics:

```python
class RankedEvent(Schema):
    account_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    rank = long(nullable=False)
    previous_sequence = long(nullable=True)


def rank_events(self, event: Event) -> RankedEvent:
    return RankedEvent(
        account_id=event.account_id,
        event_id=event.event_id,
        sequence=event.sequence,
        rank=rank(
            partition_by=event.account_id,
            order_by=event.sequence,
            descending=True,
        ),
        previous_sequence=lag(
            event.sequence,
            partition_by=event.account_id,
            order_by=event.sequence,
        ),
    )
```

For keyed deduplication, use a selected-row helper with an explicit partition and order:

```python
def latest_events(self, event: Event) -> LatestEvent:
    dedupe_latest_by(event.sequence, partition_by=event.account_id)
    return LatestEvent(
        account_id=event.account_id,
        event_id=event.event_id,
        sequence=event.sequence,
    )
```

Ranking, analytic windows, and selected-row helpers are batch-only until a separate streaming state and watermark
contract admits them.


## Expression, Filter, and Window Surface

Row-local expression families include string normalization and search (`lower`, `upper`, `trim`, `substring`,
`regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, and
`levenshtein`); date and numeric helpers (`date_add`, `datediff`, `date_trunc`, `abs`, `round`, `ceil`, and `floor`);
null and conditional helpers (`is_null`, `is_not_null`, `isnan`, `coalesce`, and `when(...).otherwise(...)`); and
explicit conversions such as `to_decimal`, `to_date`, and `to_timestamp`. The [Expressions
API](../api/Expressions.api.md)
owns the exact inventory and target parity.

Keep semantic conversions visible at the assignment site:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    return OrderNormalized(
        id=order.id,
        customer_id=lower(trim(order.customer_id)),
        total=coalesce(
            to_decimal(order.total, precision=12, scale=2),
            0,
        ),
        ordered_on=to_date(order.ordered_on, format="yyyy-MM-dd"),
        is_large=when(
            to_decimal(order.total, precision=12, scale=2) >= 1000,
            True,
        ).otherwise(False),
    )
```

Field references retain their declared type and nullability. Helpers carry result type and nullability into the plan;
they do not scan data to prove a value safe. A nullable source remains nullable after ordinary null-intolerant helpers
until a visible filter or a null-repairing expression narrows it.

Filters use compiler-visible boolean expressions. Multiple `where(...)` calls retain source order and may narrow a
direct nullable field after `is_not_null()`. Python `and`, `or`, `not`, symbolic truthiness, raw SQL strings, and
untyped callbacks are not equivalent forms. Selected-row helpers (`latest_by`, `earliest_by`, and their dedupe forms),
projection windows (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, and rolling metrics), and collection callbacks
are separate compiler-visible families with their own cardinality, nullability, and streaming classifications.

Multiple filters are ordered and composable:

```python
def keep_billable(self, order: OrderNormalized) -> BillableOrder:
    where(order.customer_id.is_not_null())
    where(order.total.is_not_null() & (order.total > 0))
    return BillableOrder.project(order)
```

The compiler may combine adjacent filters in the target plan only when the observable null, error, and source-order
semantics remain unchanged. A filter before a join may reference only scopes already available; a filter after a join
may reference the joined relation.

The compiler rejects arbitrary dynamic branching, row-wise callbacks, implicit Python UDF generation, RDD operations,
local collection, unsupported raw SQL predicates, and operations outside the selected backend capability profile.


## Symbolic Compilation Flow

Compilation executes a step method with symbolic row proxies rather than real data. It records the operation sequence,
source locations, referenced inputs, output projections, filters, joins, expressions, validation boundaries, and lane
dependencies. The result is a deterministic, backend-neutral transform plan.

```text
source transform
  -> discovery and metadata
  -> symbolic step execution
  -> TransformPlan / StepPlan IR
  -> type and compileability checks
  -> capability checks
  -> direct execution or generated PySpark
```

The compiler never runs the user's pipeline during symbolic execution. Unsupported source behavior fails at compile time
with a structured diagnostic identifying the transform, helper, and schema field when available. Structure never silently
falls back to opaque generated code. Use `@special(type="ignore")` only for code that must remain outside compilation;
calling it from compiled logic remains an error. Use `@special(type="udf")` for intentional scalar Python execution or
an explicit hook for arbitrary DataFrame logic.

The [Compiler background](Compiler.back.md) documents symbolic execution, intermediate representation, and extension
points. This page owns the author-facing
transform contract and uses those documents for deeper implementation context.

The author-facing compiler commands are deliberately separate from runtime execution:

```text
structure check orders.transforms.order.EnrichOrders
structure compile orders.transforms.order.EnrichOrders
structure explain orders.transforms.order.EnrichOrders
```

`check` validates discovery, schemas, step flow, expressions, hooks, capabilities, and streaming classification without
writing generated files. `compile` additionally emits deterministic PySpark artifacts. `explain` reports the checked
plan and dependencies without starting a Spark job. All three commands require import-safe source modules.


## Hooks And Validation Boundaries

`@raw` hooks are explicit escape hatches for caller-owned target code. They receive selected DataFrames as keyword-only
parameters and may use the caller's Spark session and context. Because Structure cannot safely inspect arbitrary hook
bodies, hooks are opaque for portability and streaming analysis unless explicitly marked with the relevant target or
streaming contract.

Validation policy is configured at input, intermediate, and output phases. Schema-only validation can remain inside the
compiler/runtime contract; value-level constraints and arbitrary hooks can add Spark work and must be explicit.

### Hook Example

Use a hook when the target-specific DataFrame operation is intentionally outside the compiler-visible DSL:

```python
class PrepareOrders(Transform):
    orders = input(OrderRaw)
    prepared = output(OrderPrepared)

    def normalize(self, order: OrderRaw) -> OrderPrepared:
        return OrderPrepared.project(order)

    @raw(lane=prepared, target_backend="pyspark")
    def remove_negative_totals(self, *, prepared, spark, ctx):
        from pyspark.sql import functions as F

        return prepared.where(F.col("total") >= 0)
```

The hook receives and returns a DataFrame through keyword-only parameters. The compiler validates its decorator,
source lane, target scope, signature, and output boundary, but does not inspect the hook body as symbolic Structure
logic. Generated and online execution call the same hook on the transform implementation instance.

### Hook Access To Original Inputs

`pass_inputs=True` supplies a read-only namespace of original declared inputs in addition to the selected lane:

```python
class CompareOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized.project(order)

    @raw(lane=normalized, pass_inputs=True)
    def retain_source_ids(self, *, normalized, inputs, spark, ctx):
        return normalized.join(
            inputs.orders.select("id"),
            on="id",
            how="left_semi",
        )
```

`inputs.orders` is the original DataFrame, not an intermediate lane. Hook parameters must be keyword-only and contain
exactly the selected lane, `spark`, `ctx`, and—when requested—`inputs`. Hook bodies may use backend APIs, but they
must return a DataFrame.

### Hook Schema Policy And Ordering

Use schema policy when a hook intentionally adds or removes columns:

```python
@raw(
    lane=prepared,
    schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
    project_output=True,
)
def add_quality_flag(self, *, prepared, spark, ctx):
    from pyspark.sql import functions as F

    return prepared.withColumn("_checked", F.lit(True))
```

The default `SchemaMode.STRICT` requires the returned shape to match the declared target. `ALLOW_EXTRA_COLUMNS` permits
additional columns; `project_output=True` restores the target schema and order at the boundary. Hooks run at their exact
source-order position among compiled steps, and adjacent hooks receive the previous hook's returned lane.

Hooks are opaque for traceability and streaming analysis unless their target and `streaming=True` promise make the
boundary eligible. An arbitrary hook in a streaming transform is rejected or warned according to the configured
compatibility policy.


## Runtime And Generated Parity

Direct execution and generated PySpark consume the same checked plan and schema model. Generated code is deterministic,
reviewable, and free of hidden actions, RDD conversion, or lifecycle ownership. Structure does not load data, write
storage, create Spark sessions, start streaming queries, set checkpoints, or stop caller-owned queries.

Streaming compatibility is a separate analysis of concrete input lineage and operation support. A transform marker such
as `streaming=True` is a compatibility contract, not a switch that starts streaming execution.

### Online And Generated Invocation

Online execution is the default and needs only a caller-owned Spark session:

```python
session = StructureSession(
    spark=spark,
    config=StructureConfig.resolve(execution_mode="online"),
)
result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
).run(session)

published_schema = result.schema.published
published_df = result.published
```

Generated mode selects checked-in generated classes without changing the source transform meaning:

```python
config = StructureConfig.resolve(
    project_root=".",
    execution_mode="generated",
)
session = StructureSession(spark=spark, config=config)
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
```

Generated mode fails with an actionable diagnostic when the selected generated artifact is absent or stale. The
remedies are to run `structure compile`, make the generated source root importable, or switch to
`execution_mode = "online"`. Neither mode starts Spark or owns storage and streaming lifecycle.

### Streaming Ownership Example

A streaming-compatible transform returns a DataFrame plan. The caller owns the source, sink, checkpoint, trigger, and
query lifecycle:

```python
@transform(streaming=True)
class EnrichStream(Transform):
    orders = input(OrderRaw, streaming=True)
    enriched = output(OrderEnriched)

    def enrich(self, order: OrderRaw) -> OrderEnriched:
        where(order.id.is_not_null())
        return OrderEnriched.project(order)


orders = spark.readStream.table("orders")
result = EnrichStream(orders=orders).run(session)
query = (
    result.enriched.writeStream
    .option("checkpointLocation", checkpoint)
    .toTable("orders_enriched")
)
```

`streaming=True` makes known incompatible or unknown operations errors when compatibility checks are enabled. It does
not create `readStream` or `writeStream`, start or stop the query, set checkpoints, select output modes, or provide
recovery logic. Watermarks and event-time bounds must be declared before the stateful operation they support.


## Diagnostics

Transform diagnostics should identify the transform, input or output, step method, operation, source location, and
shortest valid correction. Common failures include unknown input names, missing outputs, ambiguous composition wiring,
unsupported operations, invalid hook signatures, incompatible assignments, and backend capability gaps.

Example for ambiguous input binding:

```text
CompileError DSL-E0402: Invalid transform structure

Transform:
  EnrichOrders

Step:
  add_customer(order: OrderNormalized, customer: Customer)

Problem:
  More than one declared input or lane matches Customer.

Use:
  Add @step(input=[orders, customers], output=enriched) or rename the declarations.

See docs/dev/specifications/DSL.md
```

Example for an unsupported symbolic operation:

```text
CompileError DSL-E0401: Unsupported expression

Step:
  NormalizeOrders.normalize

Problem:
  Symbolic fields cannot be used in Python truthiness or string methods.

Use:
  Replace `if order.customer_id:` with `where(order.customer_id.is_not_null())` and
  replace `.strip().lower()` with `lower(trim(...))`.

See docs/dev/specifications/DSL.md
```

Example for an invalid hook boundary:

```text
CompileError HOOK-E0701: Invalid hook signature

Hook:
  PrepareOrders.remove_negative_totals

Problem:
  Hook parameters must be keyword-only and include the selected lane, spark, and ctx.

Use:
  def remove_negative_totals(self, *, prepared, spark, ctx):
      return prepared

See docs/dev/specifications/HookSemantics.md
```

Diagnostics are emitted before execution or generated-source rendering whenever the compiler can prove the problem.
They should link to the narrowest specification and show the shortest source-level correction rather than only a
backend exception.


## Appendix: Choosing A Reuse Shape

Prefer schema inheritance and `Schema.base(...)` when a row contract extends or combines existing schemas. Prefer
transform inheritance when one transform specializes one logical pipeline. Prefer `.to(...)` when independent complete
transforms should be chained behind declared input and output boundaries.

As a compact decision guide:

```text
same row contract, more fields       -> Schema inheritance and Schema.base(...)
same logical pipeline, reusable flow  -> Transform inheritance
independent complete pipelines        -> invocation.to(...)
target-specific arbitrary DataFrame   -> @raw hook
reusable typed scalar expression      -> ordinary helper (optional @special(type="expr"))
```

The [API reference](../reference/API.ref.md) remains the source for supported operation names and target parity. The
[Execution background](Execution.back.md), [Hook semantics](HookSemantics.back.md), and
[Streaming](Streaming.back.md) background covers runtime topics intentionally kept separate from the authoring contract.
The focused inventories are [Relations API](../api/Relations.api.md), [Aggregations API](../api/Aggregations.api.md),
[Windows API](../api/Windows.api.md), [Collections API](../api/Collections.api.md), and
[Expressions API](../api/Expressions.api.md). [Generation](Generation.back.md) documents emitted source and artifact
identity; [Compiler](Compiler.back.md) documents the plan and IR implementation context.
