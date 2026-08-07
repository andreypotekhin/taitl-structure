# Execution Semantic Contract

## Purpose

Execution and generated-code execution are two ways to run the same Structure transform. They differ in output form:
execution uses live PySpark objects at runtime, while generated-code execution imports checked-in PySpark source.
They must not differ in transform meaning.

This specification defines the shared semantic contract between checked `TransformPlan` IR, PySpark execution,
and generated PySpark emission. The contract exists to prevent two independent lowerers from drifting apart on
projection order, filter order, join aliasing, hook order, validation placement, schema projection, literal typing, or
performance guardrails.

## Scope

This specification owns:

- the shared PySpark semantic lowering layer;
- parity requirements for execution and generated-code execution;
- deterministic operation recipes consumed by online runners and generated emitters;
- the boundary between semantic concerns and source-text concerns;
- operation-by-operation parity test requirements;
- compiled-path performance guardrails.

Related specifications own narrower behavior:

- backend-neutral IR shape: [IntermediateRepresentation.spec.md](IntermediateRepresentation.spec.md);
- direct runtime selection and session behavior: [Execution.spec.md](Execution.spec.md);
- generated source text shape: [PySparkCodeGeneration.spec.md](PySparkCodeGeneration.spec.md);
- symbolic capture: [SymbolicExecution.spec.md](SymbolicExecution.spec.md);
- type and literal compatibility: [NullabilityAndTypeCoercion.spec.md](NullabilityAndTypeCoercion.spec.md);
- join semantics: [JoinSemantics.spec.md](JoinSemantics.spec.md);
- streaming classification: [StreamingCompatibility.spec.md](StreamingCompatibility.spec.md).

When this document overlaps those specifications, this document owns how already-checked semantics are shared by online
and generated PySpark consumers. The narrower specification still owns the feature's source-level behavior.

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

The generated code emitter must not re-decide transform semantics while rendering source text. The online runner must
not execute generated Python source text. Both consume the same PySpark execution recipes.

A checked compiled artifact is the runtime unit that holds the plan and recipes. Execution interprets that
artifact; generation renders it. Generated modules carry the artifact semantic fingerprint and generated-code execution
must reject a module whose fingerprint differs from the artifact selected by the session.

## Shared Target Plan

The shared target plan is internal implementation detail, not a public end-user API. The required conceptual records
are:

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
  pass_inputs
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
- before-hook and after-hook order;
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
- hook input namespace shape;
- hook `schema_mode` and `project_output` behavior;
- final schema projection and validation;
- watermark placement on the current frame and on joined inputs before their join;
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
8. The feature family has a representative live public concept-parity scenario. It runs through the supported classic
   PySpark Compose lanes and through Spark Connect only when the capability profile claims Connect support.

Unsupported operations must fail through diagnostics before execution or generated source rendering.

## Parity Matrix

The parity matrix is cumulative. Each row must have at least one small deterministic Spark fixture before the operation
is considered supported.

The compact, user-visible representatives live in `tests/concepts/live_pyspark`; the detailed rows and edge combinations
remain in specification, user-story, differential, and integration suites. A local skip is useful developer feedback,
but only a passing Compose lane is release evidence.

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
pass_inputs=True hook namespace
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

Diagnostics from the shared lowering layer must include:

- diagnostic code;
- transform class;
- target backend;
- target PySpark range;
- step, operation, field, hook, join, or expression when relevant;
- problem;
- why it matters when not obvious;
- suggested fix;
- documentation link.

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
  or wait for the broader v2 windowing specification.

See docs/dev/specifications/ExecutionSemanticContract.spec.md
```

## Acceptance Criteria

The contract is implemented when tests prove:

- execution and generated-code execution consume the same checked `TransformPlan`;
- a shared PySpark target plan exists for projection-only execution;
- projection field order is identical for execution and generated-code execution;
- input, intermediate, hook, and final validation placement is identical for execution and generated-code execution;
- literal typing and casts are identical for execution and generated-code execution;
- filters preserve source order for execution and generated-code execution;
- join aliases and repeated join occurrence names are deterministic for execution and generated-code execution;
- hook calls and `HookInputs` shape are identical for execution and generated-code execution;
- `schema_mode` and `project_output` behavior is identical for execution and generated-code execution;
- generated source snapshots are secondary to runtime parity tests;
- unsupported operations fail before either execution or generated rendering;
- compiled recipes contain no prohibited UDF, RDD, collection, or row-wise behavior;
- compiler commands remain Spark-free.
