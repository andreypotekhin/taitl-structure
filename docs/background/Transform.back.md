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


## Invocation And Composition

Invoke a transform with declared input names and run it with a session:

```python
result = NormalizeOrders(orders=orders_df).run(session)
```

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

`.to(...)` accepts invocations, not classes. `a.to(b, c)` and `a.to(b).to(c)` flatten to the same pipeline, and
`Transform.to(a, b, c)` starts a pipeline without a receiver. A downstream input is supplied by its constructor binding
or by one matching upstream output; supplying both is ambiguous and fails.

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

Generated-capable composition uses a wrapper with one pipeline field:

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

The compiler flattens stage plans into one generated class and prefixes stage step names so source and diagnostics
remain readable. Hook-bearing stages retain their declaring owner. Online and delegated generated execution create one
private implementation instance per hook-owning stage; embedded generated hooks are copied only when every included hook
meets the standalone embedding rules. Hook order, bindings, validation, traceability, and streaming classification do
not change through composition. Wrapper-local hooks and steps, cross-target hook pipelines, and exposing internal lanes
as composition outputs remain deferred.


## Composition Graph Details

Class-field assignments whose values are transform invocations may form a dependency graph, while ordinary assignments
remain ordinary Python values. Existing `stage(...)` declarations remain supported. A wrapper may collect stage outputs
in one output mapping, but its public output declaration order remains authoritative. Stage constructor inputs bind to
wrapper inputs, not runtime DataFrames created during class definition. The compiler flattens a valid graph into one
generated artifact, prefixes stage methods, retains hook owners, and rejects cycles, unresolved outputs, cross-target
pipelines, internal-lane bindings, and wrapper-local interleaving that has no explicit contract.

## Transform Inheritance

Use transform inheritance when one logical pipeline specializes reusable parent steps. Parent classes may contribute
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
`Transform.to(...)`, or `@special(type="expr")` for reusable compiler-visible expressions.


## Source Modules and Expression Boundaries

Source roots are resolved from CLI overrides, `[tool.structure]`, `structure.toml`, then the `src` default or project
root fallback. A source-root-relative path becomes its import path; duplicate paths across roots are errors, and
generated paths mirror that import identity. Discovery preserves class-body order for fields, inputs, steps, helpers,
hooks, and validation decorators. Modules imported during compiler commands must declare objects only: Spark startup,
data reads, writes, network calls, services, actions, and large data parsing belong behind runtime entrypoints or hooks.

Compiled expressions use typed field references, literals, operators, public helpers, and `@special(type="expr")`.
Python truthiness and control flow over symbolic values, Python string methods, raw SQL column paths, arbitrary
callbacks,
and runtime objects are rejected. Use `&`, `|`, `~`, and `when(...)` for symbolic logic; use an explicit hook for target
code that has no Structure-level contract. These boundaries preserve Spark optimizer visibility and compiler speed.


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

Online and generated execution use the same public PySpark recipes. Relation operations must not hide actions, RDD or
Pandas conversion, raw SQL, implicit UDFs, or driver collection. Set operations preserve or explicitly document
duplicates and make no ordering promise; ordering-dependent `limit(...)` and `offset(...)` require a preceding valid
`order_by(...)` that no later row-shaping operation has invalidated.


## Compiler-Visible Operations

The compiler understands schema constructors, field references, Python literals, expression helpers, `where(...)`,
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


## Expression, Filter, and Window Surface

Row-local expression families include string normalization and search (`lower`, `upper`, `trim`, `substring`,
`regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, and
`levenshtein`); date and numeric helpers (`date_add`, `datediff`, `date_trunc`, `abs`, `round`, `ceil`, and `floor`);
null and conditional helpers (`is_null`, `is_not_null`, `isnan`, `coalesce`, and `when(...).otherwise(...)`); and
explicit conversions such as `to_decimal`, `to_date`, and `to_timestamp`. The [Expressions
API](../api/Expressions.api.md)
owns the exact inventory and target parity.

Filters use compiler-visible boolean expressions. Multiple `where(...)` calls retain source order and may narrow a
direct nullable field after `is_not_null()`. Python `and`, `or`, `not`, symbolic truthiness, raw SQL strings, and
untyped callbacks are not equivalent forms. Selected-row helpers (`latest_by`, `earliest_by`, and their dedupe forms),
projection windows (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, and rolling metrics), and collection callbacks
are separate compiler-visible families with their own cardinality, nullability, and streaming classifications.

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
with a structured diagnostic instead of silently falling back to opaque generated code.

The [Compiler background](Compiler.back.md) documents symbolic execution, intermediate representation, and extension
points. This page owns the author-facing
transform contract and uses those documents for deeper implementation context.


## Hooks And Validation Boundaries

`@raw` hooks are explicit escape hatches for caller-owned target code. They receive selected DataFrames as keyword-only
parameters and may use the caller's Spark session and context. Because Structure cannot safely inspect arbitrary hook
bodies, hooks are opaque for portability and streaming analysis unless explicitly marked with the relevant target or
streaming contract.

Validation policy is configured at input, intermediate, and output phases. Schema-only validation can remain inside the
compiler/runtime contract; value-level constraints and arbitrary hooks can add Spark work and must be explicit.


## Runtime And Generated Parity

Direct execution and generated PySpark consume the same checked plan and schema model. Generated code is deterministic,
reviewable, and free of hidden actions, RDD conversion, or lifecycle ownership. Structure does not load data, write
storage, create Spark sessions, start streaming queries, set checkpoints, or stop caller-owned queries.

Streaming compatibility is a separate analysis of concrete input lineage and operation support. A transform marker such
as `streaming=True` is a compatibility contract, not a switch that starts streaming execution.


## Diagnostics

Transform diagnostics should identify the transform, input or output, step method, operation, source location, and
shortest valid correction. Common failures include unknown input names, missing outputs, ambiguous composition wiring,
unsupported operations, invalid hook signatures, incompatible assignments, and backend capability gaps.


## Appendix: Choosing A Reuse Shape

Prefer schema inheritance and `Schema.base(...)` when a row contract extends or combines existing schemas. Prefer
transform inheritance when one transform specializes one logical pipeline. Prefer `.to(...)` when independent complete
transforms should be chained behind declared input and output boundaries.

The [API reference](../reference/API.ref.md) remains the source for supported operation names and target parity. The
[Execution background](Execution.back.md), [Hook semantics](HookSemantics.back.md), and
[Streaming](Streaming.back.md) background covers runtime topics intentionally kept
separate from the authoring contract.
