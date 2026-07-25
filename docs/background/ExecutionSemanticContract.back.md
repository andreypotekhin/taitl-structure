# Execution Semantic Contract

Execution and generated-code execution are two ways to run the same Structure transform. They differ in output form:
execution uses live PySpark objects at runtime, while generated-code execution imports checked-in PySpark source.
They share the same transform meaning.

This reference defines the shared semantic contract between checked `TransformPlan` IR, PySpark execution,
and generated PySpark emission. The contract exists to prevent two independent lowerers from drifting apart on
projection order, filter order, join aliasing, hook order, validation placement, schema projection, literal typing, or
performance guardrails.

## Ownership Boundary

The target-neutral Core structural plan and the PySpark-owned lowered plan are now separated by the selected Plugin
API. Core establishes transform routing, bindings, source order, outputs, and hook placement; the PySpark authoring
and compiler facets validate and lower PySpark bodies. The PySpark executor and generator consume that lowered
contract. References below to a Core-owned `TransformPlan` should therefore be read as the structural Core portion plus
an opaque PySpark body, never as permission for Core to inspect PySpark expressions or recipes.

## Scope

This reference covers:

- the shared PySpark semantic lowering layer;
- parity requirements for execution and generated-code execution;
- deterministic operation recipes consumed by online runners and generated emitters;
- the boundary between semantic concerns and source-text concerns;
- compiled-path performance guardrails.

Related references own narrower behavior:

- backend-neutral IR shape: [IntermediateRepresentation.md](IntermediateRepresentation.back.md));
- direct runtime selection and session behavior: [Execution.md](Execution.back.md));
- generated source text shape: [PySparkCodeGeneration.md](PySparkCodeGeneration.back.md));
- symbolic capture: [SymbolicExecution.md](SymbolicExecution.back.md));
- type and literal compatibility: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.back.md));
- join semantics: [JoinSemantics.md](JoinSemantics.back.md));
- streaming classification: [StreamingCompatibility.md](StreamingCompatibility.back.md)).

When this document overlaps those references, this document owns how already-checked semantics are shared by online
and generated PySpark consumers. The narrower reference still owns the feature's source-level behavior.

## Core Rule

`TransformPlan` is the backend-neutral source of truth. A target-specific PySpark lowering pass turns checked
`TransformPlan` IR plus `PySparkCapabilities` into deterministic PySpark execution recipes.

```text
TransformPlan
  + PySparkCapabilities
  -> PySparkExecutionPlan
       -> OnlinePySparkRunner interprets recipes with live PySpark objects
       -> PySparkCodeGenerator renders recipes as source text
```

The generated code emitter does not re-decide transform semantics while rendering source text. The online runner does
not execute generated Python source text. Both consume the same PySpark execution recipes.

## Shared Target Plan

The shared target plan is not a public end-user API. Conceptually, it contains:

```text
PySparkExecutionPlan
  transform
  capabilities
  inputs
  steps
  final_validation
  guardrails

PySparkStepRecipe
  step
  inputs
  before_hooks
  shared_operations
  results

PySparkStepResultRecipe
  output_lane
  projection
  after_hooks
  validations

PySparkExpressionRecipe
  expression
  type
  nullable
  function
  arguments
  literal
  field_reference

PySparkJoinRecipe
  joined_input
  left_alias
  right_alias
  join_type
  hint
  key_pairs
  right_fields

PySparkValidationRecipe
  target
  schema
  mode
  projection
  reason

PySparkHookRecipe
  name
  timing
  inputs
  schema_mode
  project_output
```

Implementations may rename these records when a local naming pattern is clearer. The observable requirement is that a
single target plan carries the semantic choices consumed by both execution and generated-code execution.

The shared target plan must not contain:

- live Spark sessions;
- live PySpark DataFrames;
- live PySpark Columns;
- generated source text;
- formatter state;
- import collectors;
- file paths for generated output;
- runtime hook return values.

## Semantic Invariants

The shared PySpark execution plan must decide these items once:

- input validation order and mode;
- step order;
- ordered schema-parameter binding and the driving relation;
- source-ordered hook boundaries;
- filter order and legal filter combination;
- expression function mapping;
- literal typing, casts, and null literal handling;
- projection field order;
- projection aliases;
- one shared join/filter prefix followed by ordered result projections for multi-result steps;
- schema base overlay expansion;
- join type spelling;
- join alias names;
- repeated join occurrence suffixes;
- broadcast and other supported hints;
- right-side join field projection;
- validation placement and validation mode;
- hook binding shape;
- hook `schema_mode` and `project_output` behavior;
- final schema projection and validation;
- compiled-path performance guardrails.

Execution and generated-code execution may differ only in representation details that do not change observable DataFrame
semantics. Allowed differences include:

- Python imports and generated file headers;
- formatting and line wrapping;
- temporary local variable layout when the same operations occur in the same semantic order;
- whether PySpark calls are made directly or represented as rendered source;
- runtime object identity for live DataFrames and Columns;
- source comments in generated files.

## Operation Admission Rule

A compiled operation should be admitted as a Structure semantic feature, not as a one-to-one clone of a PySpark
function. New feature families such as aggregations, windows, arrays, maps, and higher-order expressions must define
source semantics, IR, target recipes, parity tests, and guardrails before they become compiler-visible. If an operation
is too rare, backend-specific, or arbitrary to justify that contract, it belongs in an explicit hook.

A new compiled operation is not supported until all of these are true:

1. The source DSL behavior is specified.
2. The backend-neutral IR shape is specified.
3. The PySpark execution recipe is specified.
4. The direct runtime runner can consume the recipe or the feature is explicitly unsupported for execution.
5. The generated emitter can render the recipe or the feature is explicitly unsupported for generated mode.
6. A parity test proves execution and generated-code execution behavior match for the operation when both modes support it.
7. Guardrail tests prove compiled paths do not use prohibited PySpark escape hatches.

Unsupported operations must fail through diagnostics before execution or generated source rendering.

## Parity Matrix

The parity matrix is cumulative. Each row must have at least one small deterministic Spark fixture before the operation
is considered supported.

```text
projection-only
projection with typed literals
input validation
output validation
filter before projection
multiple filters
expression helpers
class-local @special(type="expr") helpers
schema base overlays
left lookup_join
inner lookup_join
composite join keys
null-safe join keys
broadcast hints
exists left-semi joins
not_exists left-anti joins
repeated joins of the same input
grouped aggregates
selected-row latest/earliest helpers
exact duplicate-row removal
array higher-order helpers
map higher-order helpers
raw hooks
raw hooks
explicit hook input bindings
schema_mode raw hooks
project_output raw hooks
intermediate validation
final validation
streaming-compatible supported operations
```

Parity tests should compare:

- output column order;
- output data types where Spark exposes them reliably;
- output nullability where Spark exposes it reliably;
- row contents;
- presence or absence of extra columns;
- expected diagnostics for unsupported cases.

Generated-code snapshots are useful, but they are not a substitute for runtime parity tests.

## Guardrails

Compiled recipes must not introduce:

- Python UDFs;
- Pandas UDFs;
- RDD operations;
- `collect`;
- `toPandas`;
- row-wise maps;
- hidden Python loops over DataFrame rows.

Hooks remain explicit escape hatches. Hook internals are opaque to the compiler, but hook boundaries must remain visible
in recipes, generated code, traceability, and diagnostics.

## Determinism

For identical source, configuration, Structure version, and PySpark capabilities, the shared PySpark execution plan must
be deterministic.

Rules:

- Preserve source order where order changes semantics.
- Sort independent collections before recipe serialization.
- Use stable aliases derived from IR scopes and occurrence numbers.
- Do not include timestamps, memory addresses, object ids, or absolute workspace paths.
- Emit diagnostics in deterministic order.

Determinism is required for generated-code review, snapshot tests, parity tests, compiler provenance, and future
incremental compilation.

## Diagnostics

Example:

```text
CompileError BACKEND-E0802: Operation is not supported by the PySpark target plan

Transform:
  orders.transforms.order.EnrichOrders

Step method:
  summarize

Operation:
  WindowProject

Problem:
  Broad WindowProject forms have no PySpark execution recipe, so execution and generated-code execution could drift.

Use:
  Use latest_by(...) or earliest_by(...) for admitted selected-row windows, move broader logic into an explicit hook,
  or wait for the broader v2 windowing reference.

See docs/background/Execution.back.md
```
