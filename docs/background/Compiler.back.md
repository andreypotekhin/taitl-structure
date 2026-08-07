# Compiler

The compiler turns compiler-visible Structure source into a checked, backend-neutral representation. Symbolic execution
captures what a transform means; the intermediate representation records that meaning; validation, capability checks,
execution, generation, provenance, traceability, and diagnostics consume the same model.

This background combines symbolic execution, intermediate representation, and cross-stage invariants. It is
implementation oriented; authoring behavior remains in [Transform](Transform.back.md), [Schema](Schema.back.md), and
[Join](Join.back.md). The normative sources are [Intermediate
Representation](../dev/specifications/IntermediateRepresentation.spec.md),
[Symbolic Execution](../dev/specifications/SymbolicExecution.spec.md), [Compileability
Checker](../dev/design/CompileabilityChecker.design.md),
[Symbolic Execution Engine](../dev/design/SymbolicExecutionEngine.design.md), and
[Compiler Performance](../dev/specifications/CompilerPerformanceTargets.spec.md).

## Compiler Flow

```text
source discovery
  -> transform and schema metadata
  -> symbolic execution
  -> TransformPlan IR
  -> structural, type, and capability checks
  -> target lowering
  -> online execution or generation
```

Compiler commands are Spark-free. The compiler does not run the user's pipeline, inspect live data, import PySpark,
start Java, or create a Spark session. Unsupported source behavior fails with a structured diagnostic rather than
silently falling back to a UDF, row loop, RDD operation, or opaque generated code.


## Symbolic Execution

Symbolic execution evaluates a compiled step method with symbolic row proxies instead of real data. It records field
references, literals, expressions, filters, joins, schema projections, hook boundaries, lane dependencies, source order,
and source locations.

The purpose is to preserve ordinary Python authoring while keeping operations visible as DataFrame and Column semantics.
It is not a runtime execution mode and never evaluates a pipeline against rows.

Symbolic context tracks the active transform, step method, source path and line, current input scopes, joined scopes,
lanes, outputs, validation policy, target metadata, and diagnostics. A row proxy exposes typed field references; a field
reference carries schema identity, field identity, alias, type, nullability, and provenance.

Supported source forms include:

- schema constructors and Schema.base overlays;
- compiler-visible expression helpers and special expression functions;
- typed Python literals;
- where predicates;
- declared lookup, analytical, and rowset joins;
- ordered lane reads and writes;
- explicit hook and validation declarations.

Dynamic Python branching, arbitrary callbacks, string SQL predicates, local data collection, RDD operations, and
unsupported method calls fail during symbolic capture.

### Worked Symbolic Capture

Consider a step that normalizes a nullable amount and filters invalid rows:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized(
            id=order.id,
            total=coalesce(
                to_decimal(order.total, precision=12, scale=2),
                0,
            ),
        )
```

Symbolic execution does not evaluate `order.id` or `order.total`. It records a field reference, a null-check
predicate, a conversion, a typed literal, a coalesce expression, and an ordered output projection. The generated or
online PySpark lowerer later turns those records into DataFrame and `Column` operations.

The same distinction applies to relation operations:

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    left_join(customer, on=customer.id == order.customer_id, hint="broadcast")
    return OrderWithCustomer.base(order)(customer_name=customer.name)
```

The join is captured as an operation with a right scope, predicate, hint, source order, and output effect. The compiler
does not read either relation or infer whether `customer.id` is unique from data.


## Symbolic Capture Rules

Symbolic execution runs one isolated context per step and clears it after success or failure. It creates typed row and
relation proxies, captures literals and compiler-visible helpers, records filters and joins in source order, and
captures
one schema projection or an ordered result tuple. Python truthiness (`if expr`, `and`, `or`, and `not`), arbitrary
callbacks, local collection, RDD operations, raw SQL strings, and runtime calls are rejected rather than represented as
unverifiable IR. `@special(type="expr")` helpers may expand against symbolic placeholders; their outer identity remains
available for diagnostics and provenance.

The compiler records field paths separately from rendered Spark names so aliases containing dots remain one field. A
joined field is unavailable until its join is captured, and left-join fields become nullable. Schema constructors retain
field assignments in schema order; `Schema.base(...)` overlays are expanded before type and nullability checks.

Capture is isolated from ordinary Python state. A compiled method should calculate only symbolic values and should not
mutate module globals, create files, call services, or depend on a live session:

```python
# Compiler-visible and deterministic:
value = lower(trim(order.customer_id))

# Invalid compiler-time dependency:
value = fetch_customer_name(order.customer_id)  # network call
```

When target-specific code is intentional, move it to an explicit `@raw` hook. Reusable typed scalar expression logic can
remain an ordinary compiler-visible helper; use `@special(type="expr")` when explicit metadata or named helper rendering
is useful.


## Intermediate Representation

The IR is immutable, deterministic, and backend-neutral. Its core shape is:

```text
TransformPlan
  transform identity
  inputs
  steps
  outputs
  hooks
  validation
  provenance
  dataflow
  capabilities

StepPlan
  method identity
  input scopes
  operations
  output projections
  lane dependencies
  source anchors
```

InputPlan records declared input name, schema identity, aliases, streaming mode, and source metadata. OutputPlan records
public result name, schema identity, source lane or projection, and aliases. Scopes identify the current relation,
joined occurrences, lanes, and output boundaries without relying on Python local variable names.

### Stable IR Identity

Every transform, input, step, operation, expression, scope, and validation point should have a deterministic IR id:

```text
transform:orders.transforms.order.EnrichOrders
input:orders.transforms.order.EnrichOrders.orders
step:orders.transforms.order.EnrichOrders.normalize
op:orders.transforms.order.EnrichOrders.normalize.003.filter
expr:orders.transforms.order.EnrichOrders.normalize.003.filter.predicate
```

Ids must not contain timestamps, memory addresses, object ids, or absolute workspace paths. Operation ids include
source-order ordinals. Expression ids may be structural or path-based but must remain stable enough for diagnostics and
snapshot tests. Ids are an internal compatibility surface, not public DSL API.

### Source Anchors

IR nodes retain best-effort source anchors:

```text
SourceAnchor
  module
  qualified_name
  path
  line
  column
  end_line
  end_column
  display
```

Paths should be project-relative in deterministic artifacts. Missing spans must not prevent compilation when semantic
information remains valid. Diagnostics use anchors when available; provenance maps them to generated nodes. `display`
may contain a compact deterministic source expression.

### Transform Plan Rules

`TransformPlan` preserves class-body input and output order and source-order compiled steps. Undecorated steps consume
and update the uniquely inferred lane. Method decorators may select one or more input schemas, lanes, or outputs, and
`inout=` normalizes to input and output declaration tuples. Role selectors distinguish original inputs from logical
lanes,
even when a lane shadows an input name. A sole output schema may be exposed by a compatibility accessor; a multi-output
transform must fail clearly when that accessor is requested. A transform with no compiled steps is invalid unless a
future passthrough specification admits it.


## IR Node Ledger

The core plan consists of ordered `TransformPlan`, `InputPlan`, `StepPlan`, and `OutputPlan` records. A step records its
driving input or lane, ordered parameter bindings, output projections, hooks, validation points, operations, and source
anchor. Operation metadata records kind, ordinal, reads, writes, cardinality, streaming support, capability, and
documentation identity. Join records preserve joined scope occurrence, type, hints, key pairs, equality kind, right
projection, and cardinality policy. Hook records preserve source order, selected inputs, output lanes, schema mode,
projection, target scope, and streaming declaration.

Expression nodes include `FieldRef`, `Literal`, `CallExpr`, `BinaryExpr`, `BooleanExpr`, `CastExpr`, and `WhenExpr`.
Each carries type, static nullability, referenced scopes, and source metadata. Validation metadata records phase, mode,
strictness, projection, and constraint boundary. Streaming metadata classifies operations as `compatible`, `batch_only`,
or `unknown`. Provenance maps source nodes to IR and lowered/generated nodes; dataflow records transform, schema, field,
and artifact dependencies.

For the normalization example, a plan may be represented conceptually as:

```text
TransformPlan NormalizeOrders
  InputPlan orders: OrderRaw
  StepPlan normalize
    read orders.id
    filter is_not_null(orders.id)
    read orders.total
    call to_decimal(precision=12, scale=2)
    call coalesce(default=0)
    project OrderNormalized(id, total)
  OutputPlan normalized: OrderNormalized
```

This is explanatory notation, not a serialized public API. Actual IR records retain immutable metadata, stable IDs,
source anchors, capabilities, and provenance required by execution, generation, explain, and diagnostics.


## Operations And Expressions

Operation IR includes project, filter, join, hook call, schema validation, aggregate, window, collection, and other
admitted target-neutral families. Each operation records source order, referenced scopes, output effect, source anchor,
and target capability requirements.

Expression IR includes field references, literals, calls, binary and boolean operators, casts, and conditional
expressions. Expressions carry Structure type and static nullability metadata. Literal typing and assignment follow
[Schema](Schema.back.md#nullability-and-assignment); join semantics follow [Join](Join.back.md).

The IR must preserve enough information for a target lowerer to produce equivalent online and generated behavior without
re-reading user source. It must not store live DataFrames, Spark sessions, open files, or other runtime resources.

### Operation Admission Example

A new compiler-visible operation is admitted only after its source behavior, IR, target recipe, parity, and guardrails
exist:

```text
source DSL       -> where(predicate)
symbolic capture -> Filter(predicate, source_anchor)
IR validation    -> Boolean type and scope checks
target lowering  -> DataFrame.where(column)
online runner    -> execute the recipe with live DataFrame objects
generator        -> render the same recipe as PySpark source
parity tests     -> compare rows, schema, order, and diagnostics
```

If any stage is missing, the operation remains unsupported or belongs behind an explicit hook. A one-to-one wrapper
around a PySpark function is not by itself a Structure semantic feature.


## Validation, Streaming, And Capabilities

Validation metadata records phase, mode, strictness, projection policy, and constraint boundary. Schema-only validation
is distinguishable from value-level validation because the latter can trigger Spark work.

Streaming metadata records operation support classification, current versus side-input lineage, watermarks, state
requirements, join bounds, hook streaming declarations, and unknown opaque boundaries. The
[Streaming](Streaming.back.md) checker consumes this metadata.

Capability metadata records requirement group, feature name, mode, source anchor, documentation link, selected target,
profile, and decision. Unsupported or unknown required capabilities fail before execution or generation.


## Provenance And Dataflow

Provenance maps source transform, method, expression, field, and decorator locations to IR nodes and lowered/generated
nodes. Static dataflow traceability records dependencies between transforms, source inputs, derived relations, schemas,
fields, and generated artifacts.

Provenance is diagnostic metadata. Stable semantic identity comes from declared transform and schema identities, not
from line numbers or Python local names. Traceability must remain deterministic and must never imply that an unexecuted
or skipped lane was validated.


## Invariants

The compiler maintains these invariants across stages:

- schema field order and effective inheritance order are stable;
- transform steps and operations retain source order;
- every field reference resolves to one schema and field identity;
- every output field is supplied, copied, or rejected explicitly;
- aliases are deterministic and collision-safe;
- IR nodes are immutable after validation;
- online and generated target consumers receive equivalent lowered semantics;
- unsupported operations fail before runtime;
- no compiler phase requires Spark runtime imports;
- diagnostics identify the same source operation regardless of target consumer.


## Performance and Serialization

Compiler commands are designed for linear work in discovered files, declarations, captured operations, and emitted text;
diagnostic sorting may be `O(n log n)`. Caches may store source fingerprints, discovered metadata, schemas, transforms,
IR, and generated hashes, but deletion or invalidation must never change correctness or suppress diagnostics. Serialized
IR and debug output contain importable identities, project-relative source anchors, stable IDs, and no live Spark
objects,
timestamps, memory addresses, absolute paths, or formatter state.


## Determinism, Immutability, And Concurrency

Identical source, configuration, target profile, plugin version, and Structure version produce identical IR
serialization, diagnostics, traceability, and lowered artifact inputs. Collection order is explicit; sets are not used
where output order
is observable.

IR objects and schema models are immutable after construction. Compilation caches may share immutable plans between
threads. Mutable runtime context, live DataFrames, hook context, and target session objects remain outside the IR and
are
owned by the execution boundary.

Cache keys include every input that can affect semantic output. Cache invalidation is conservative; a cache hit must not
suppress diagnostics or change source locations.

### Compiler Commands

The public compiler commands expose progressively more output without taking ownership of runtime execution:

```text
structure check orders.transforms.order.EnrichOrders
structure compile orders.transforms.order.EnrichOrders
structure explain orders.transforms.order.EnrichOrders
```

`check` discovers and validates source without writing generated files. `compile` lowers the checked plan for the
selected target and writes or updates artifacts according to configuration. `explain` renders the plan, dependencies,
capabilities, and warnings without starting Spark. All three commands require import-safe modules.


## Diagnostics And Extensions

Compiler diagnostics should identify phase, transform, step, operation, source location, problem, shortest fix, and
documentation link. Unsupported operations, ambiguous scopes, invalid schema construction, invalid hook signatures,
capability gaps, and nondeterministic choices must fail early.

The IR is extensible through new operation and expression kinds, target capability requirements, provenance metadata,
and serialization fields. Extensions must preserve immutability, deterministic identity, Spark-free compilation, and
backward-compatible handling of additive metadata. A new feature is not complete until symbolic capture, IR validation,
target lowering, execution/generation parity, diagnostics, and tests agree.


## Appendix: Compiler Non-Goals

The compiler does not execute data, own Spark sessions, perform cost-based optimization, invent business keys, guarantee
runtime data quality without explicit validation, or replace caller-owned orchestration and streaming lifecycle.
