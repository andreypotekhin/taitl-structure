# Execution Semantic Contract

## Purpose

Online execution and generated execution are two ways to run the same Structure transform. They differ in output form:
online execution uses live PySpark objects at runtime, while generated execution imports checked-in PySpark source.
They must not differ in transform meaning.

This specification defines the shared semantic contract between checked `TransformPlan` IR, online PySpark execution,
and generated PySpark emission. The contract exists to prevent two independent lowerers from drifting apart on
projection order, filter order, join aliasing, hook order, validation placement, schema projection, literal typing, or
performance guardrails.

## Scope

This specification owns:

- the shared PySpark semantic lowering layer;
- parity requirements for online and generated PySpark execution;
- deterministic operation recipes consumed by online runners and generated emitters;
- the boundary between semantic concerns and source-text concerns;
- operation-by-operation parity test requirements;
- compiled-path performance guardrails.

Related specifications own narrower behavior:

- backend-neutral IR shape: `docs/specifications/IntermediateRepresentation.md`;
- online runtime selection and session behavior: `docs/specifications/OnlineExecution.md`;
- generated source text shape: `docs/specifications/PySparkCodeGeneration.md`;
- symbolic capture: `docs/specifications/SymbolicExecution.md`;
- type and literal compatibility: `docs/specifications/NullabilityAndTypeCoercion.md`;
- join semantics: `docs/specifications/JoinSemantics.md`;
- streaming classification: `docs/specifications/StreamingCompatibility.md`.

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
  before_hooks
  operations
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
single target plan carries the semantic choices consumed by both online and generated execution.

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
- before-hook and after-hook order;
- filter order and legal filter combination;
- expression function mapping;
- literal typing, casts, and null literal handling;
- projection field order;
- projection aliases;
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
- compiled-path performance guardrails.

Online and generated execution may differ only in representation details that do not change observable DataFrame
semantics. Allowed differences include:

- Python imports and generated file headers;
- formatting and line wrapping;
- temporary local variable layout when the same operations occur in the same semantic order;
- whether PySpark calls are made directly or represented as rendered source;
- runtime object identity for live DataFrames and Columns;
- source comments in generated files.

## Operation Admission Rule

A new compiled operation is not supported until all of these are true:

1. The source DSL behavior is specified.
2. The backend-neutral IR shape is specified.
3. The PySpark execution recipe is specified.
4. The online runner can consume the recipe or the feature is explicitly unsupported online.
5. The generated emitter can render the recipe or the feature is explicitly unsupported for generated mode.
6. A parity test proves online and generated behavior match for the operation when both modes support it.
7. Guardrail tests prove compiled paths do not use prohibited PySpark escape hatches.

Unsupported operations must fail through diagnostics before online execution or generated source rendering.

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
class-local @expr_fn helpers
schema base overlays
left join_one
inner join_one
composite join keys
null-safe join keys
broadcast hints
repeated joins of the same input
before hooks
after hooks
pass_inputs=True hook namespace
schema_mode after hooks
project_output after hooks
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

Subtransform:
  summarize

Operation:
  WindowProject

Problem:
  WindowProject has no v1 PySpark execution recipe, so online and generated execution could drift.

Use:
  Move the logic into an explicit hook for now, or wait for the v2 windowing specification.

See docs/specifications/ExecutionSemanticContract.md
```

## Acceptance Criteria

The contract is implemented when tests prove:

- online and generated execution consume the same checked `TransformPlan`;
- a shared PySpark target plan exists for projection-only execution;
- projection field order is identical online and generated;
- input, intermediate, hook, and final validation placement is identical online and generated;
- literal typing and casts are identical online and generated;
- filters preserve source order online and generated;
- join aliases and repeated join occurrence names are deterministic online and generated;
- hook calls and `HookInputs` shape are identical online and generated;
- `schema_mode` and `project_output` behavior is identical online and generated;
- generated source snapshots are secondary to runtime parity tests;
- unsupported operations fail before either online execution or generated rendering;
- compiled recipes contain no prohibited UDF, RDD, collection, or row-wise behavior;
- compiler commands remain Spark-free.
