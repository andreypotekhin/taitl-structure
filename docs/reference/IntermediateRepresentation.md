# Intermediate Representation

The Structure intermediate representation is the compiler contract between source DSL semantics and execution targets.
The DSL frontend produces IR. Compileability checkers, online PySpark execution, PySpark code generation, streaming
compatibility checks, compiler provenance, and static dataflow traceability consume IR.

The IR is backend-neutral. It describes what the transform means, not how PySpark source text happens to spell
that meaning.

## Scope

This reference covers:

- `TransformPlan`;
- `InputPlan`;
- `StepPlan`;
- operation IR;
- expression IR;
- hook call IR;
- schema validation placement IR;
- source provenance anchors;
- static dataflow dependency records;
- IR construction, validation, determinism, and immutability rules;
- extension points for v2, v3, and v4 roadmap features.

This specification does not own the public authoring API or backend rendering details. Those are owned by narrower
specifications:

- public DSL and symbolic execution entrypoints: [DSL.md](DSL.md);
- schema model: [SchemaModel.md](SchemaModel.md);
- schema declaration syntax: [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.md);
- schema inheritance: [SchemaInheritance.md](SchemaInheritance.md);
- type compatibility and nullability: [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md);
- join semantics: [JoinSemantics.md](JoinSemantics.md);
- online runtime behavior: [OnlineExecution.md](OnlineExecution.md);
- generated PySpark rendering: [PySparkCodeGeneration.md](PySparkCodeGeneration.md);
- streaming compatibility classification: [StreamingCompatibility.md](StreamingCompatibility.md);
- CLI behavior and diff checks: [CLI.md](CLI.md);
- compatibility policy: [CompatibilityPolicy.md](CompatibilityPolicy.md);
- diagnostic code, registry, and documentation lifecycle: [Diagnostics.md](Diagnostics.md).

When this document overlaps another specification, this document owns the IR shape and invariants. The narrower
semantic specification owns feature behavior.

## Design Principles

The IR is:

- backend-neutral;
- deterministic for identical source, configuration, and Structure version;
- immutable or treated as immutable after construction;
- explicit about operation order;
- explicit about scopes, aliases, and schema boundaries;
- rich enough for diagnostics, provenance, traceability, online execution, generated output, and streaming checks;
- compact enough to build and inspect quickly during `structure check`;
- serializable for debugging, review workflows, and later incremental compile fingerprints.

IR nodes do not contain live Spark objects, PySpark `Column` objects, PySpark `DataFrame` objects, file handles,
runtime hook return values, or mutable compiler state.

## Compiler Flow

```text
source import
  -> DSL metadata discovery
  -> symbolic execution
  -> TransformPlan IR
  -> IR validation and compileability checks
  -> compiler provenance
  -> static dataflow traceability
  -> online runner or generated PySpark emitter
```

`structure check`, `structure compile`, `structure compile --fail-on-diff`, and `structure explain` can
construct and validate IR without importing PySpark, starting Java, creating a `SparkSession`, or contacting a Spark
cluster.

Online execution is the runtime exception: `OnlinePySparkRunner` consumes the already checked IR and lowers it to live
PySpark DataFrame and Column operations at run time.

## Core Model

Minimum model:

```text
TransformPlan
  id
  name
  qualified_name
  source_class
  generated_class
  inputs
  steps
  outputs
  validation_policy
  streaming_policy
  provenance
  dataflow
  capabilities
  diagnostics

InputPlan
  id
  name
  schema
  ordinal
  source

StepPlan
  id
  name
  ordinal
  source
  input_lane
  output_lane
  input_schema
  output_schema
  input_scope
  output_scope
  operations
  hooks_before
  hooks_after
  validate_output
  source

Operation
  Filter
  Project
  Join
  HookCall
  ValidateSchema

OutputPlan
  id
  name
  ordinal
  schema
  source
  source_scope
```

Minimum expression model:

```text
Expr
  FieldRef
  Literal
  CallExpr
  BinaryExpr
  BooleanExpr
  CastExpr
  WhenExpr
```

Structure may split these records into smaller classes or sealed variants. The observable contract is the data
and invariants, not exact class names.

## Node Identity

Every transform, input, step, operation, expression, scope, and validation point should have a stable IR id.

Recommended id shape:

```text
transform:orders.transforms.order.EnrichOrders
input:orders.transforms.order.EnrichOrders.orders
step:orders.transforms.order.EnrichOrders.normalize
op:orders.transforms.order.EnrichOrders.normalize.003.filter
expr:orders.transforms.order.EnrichOrders.normalize.003.filter.predicate
```

Rules:

- Ids is deterministic.
- Ids does not include timestamps, memory addresses, object ids, or absolute workspace paths.
- Operation ids include source-order ordinals within the owning step.
- Expression ids may be structural or path-based. They is stable enough for diagnostics and review workflows.
- Generated PySpark and traceability metadata may refer to IR ids.
- Ids are internal compatibility surface, not public DSL API.

If a source edit reorders operations, operation ids may change. If only unrelated source text changes, ids for unchanged
IR nodes should remain stable when practical.

## Source Anchors

Source anchors connect IR nodes back to user code.

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

Rules:

- `module` and `qualified_name` are import-oriented identities.
- `path` should be project-relative when available.
- Absolute paths does not be written to deterministic generated artifacts.
- Line and column fields are optional because some Python objects may lack reliable source spans.
- Missing source spans does not prevent compilation when the semantic information is otherwise valid.
- Diagnostics should use source anchors whenever available.
- Provenance metadata should use source anchors to map source nodes to IR nodes and generated nodes.

`display` may hold a compact source expression string for diagnostics. It must be best-effort and deterministic.

## TransformPlan

`TransformPlan` represents one compiled transform class.

Fields:

- `id`: stable transform IR id.
- `name`: source class name, such as `EnrichOrders`.
- `qualified_name`: importable source class identity.
- `source_class`: metadata reference to the source transform class.
- `generated_class`: backend-neutral generated class identity, such as `EnrichOrdersGenerated`.
- `inputs`: ordered `InputPlan` list.
- `steps`: ordered `StepPlan` list.
- `outputs`: ordered `OutputPlan` list.
- `validation_policy`: effective transform-level validation policy.
- `streaming_policy`: transform streaming marker and effective check severity.
- `provenance`: source-to-IR provenance records.
- `dataflow`: static dependency records inferred from IR.
- `capabilities`: target backend capability selection inputs or results.
- `diagnostics`: warnings attached to the plan, if the compiler represents warnings in-plan.

Rules:

- `inputs` preserve class-body input declaration order.
- `steps` preserve source-order compiled subtransform order.
- Undecorated steps consume and update the uniquely inferred lane.
- Method-level `@transform(output=target_lane)` writes a named lane or final output while the source is inferred.
- Method-level `@transform(input=source, output=target_lane)` selects an original input or existing lane and writes the
  target lane.
- Method-level `input=[...]` and `output=[...]` bind multiple schema parameters or returned values in order.
- Method-level `inout=source | target` is normalized to the same input and output declaration tuples.
- If an input declaration name already exists as a lane, that lane shadows the original input for method-level
  `input=`.
- Role selectors preserve raw input, lane, and final output intent. A source key such as `input:orders` identifies the
  original runtime input even after the logical lane `orders` has been updated.
- `outputs` preserve class-body output declaration order.
- `TransformPlan.output_schema` is a compatibility accessor that returns the sole output schema and fails clearly when
  a transform has multiple outputs.
- A transform with no compiled steps is invalid unless a later specification defines passthrough transforms.
- `TransformPlan` does not contain live input DataFrames.
- `TransformPlan` does not contain source transform instances created for hook execution.

The plan may retain source class metadata for diagnostics and hook resolution, but deterministic serialized forms should
record importable identities rather than object representations.

## InputPlan

`InputPlan` represents one declared transform input.

Fields:

- `id`: stable input IR id.
- `name`: input name from the class attribute.
- `schema`: expected `SchemaDef`.
- `ordinal`: declaration order, starting from zero.
- `source`: source anchor for the declaration when available.

Rules:

- Input names are unique within one transform.
- Input schemas must be discovered and valid before transform IR validation.
- Input order controls generated `run(...)` parameter order and hook input namespace order.
- Input plans do not record runtime DataFrame objects.

If two inputs use the same schema and the first step consumes that schema, the compiler records how the current
pipeline input was selected or emit an ambiguity diagnostic before execution.

## StepPlan

`StepPlan` represents one compiled subtransform method.

Fields:

- `id`: stable step IR id.
- `name`: source method name.
- `ordinal`: source-order step number, starting from zero.
- `source`: input DataFrame frame key for this step.
- `input_lane`: logical lane consumed by this step.
- `output_lane`: logical lane updated by this step.
- `input_schema`: `SchemaDef` expected for the row parameter.
- `output_schema`: `SchemaDef` produced by the return expression.
- `input_scope`: current row scope at step entry.
- `output_scope`: current row scope after projection into the output schema.
- `operations`: ordered compiled operation list.
- `hooks_before`: ordered `HookCall` list for hooks before compiled operations.
- `hooks_after`: ordered `HookCall` list for hooks after compiled operations.
- `validate_output`: effective validation decision for this step.
- `source`: source anchor for the subtransform method.
- `inputs`: ordered parameter bindings; the first is marked as the driving relation.
- `results`: ordered result projections with schema, destination lane, frame name, and result-specific after hooks.

Rules:

- Operations preserve source semantics.
- A step may consume only a lane or declared input frame already available earlier in source order.
- Every parameter annotation must match its ordered input binding.
- Every returned schema must match its ordered result binding.
- Joins and filters are shared by all results; projections and after hooks belong to individual results.
- Before hooks run before compiled operations for the step.
- After hooks run after compiled operations for the step.
- `operations` should contain compiled `HookCall` and `ValidateSchema` operations only when an implementation chooses
  a single stream of operations. If hooks are also stored in `hooks_before` and `hooks_after`, duplication must be
  avoided in execution.
- The last compiled operation before after hooks should establish the step output schema, usually through `Project`.
- Step output validation is represented explicitly enough for online and generated execution to place validation
  identically.

The implementation may choose either of these equivalent shapes:

```text
hooks_before + operations + hooks_after + validate_output fields
```

or:

```text
operations containing HookCall and ValidateSchema in exact execution order
```

If the second shape is used, convenience accessors should still expose before hooks, after hooks, and validation points
for code generation and diagnostics.

## OutputPlan

`OutputPlan` represents one public transform result lane.

Fields:

- `id`: stable output IR id.
- `name`: output name, such as `df`, `accepted`, or `rejected`.
- `ordinal`: result declaration order, starting from zero.
- `schema`: declared output `SchemaDef`.
- `source`: source frame key that currently holds the lane at result construction time.
- `source_scope`: symbolic row scope associated with the source frame.

Rules:

- A field-declared output with no explicit output method may be satisfied by a unique source lane with the same schema.
  Results are exposed by declared output name.
- A transform with no field-declared outputs is invalid.
- Multiple field-declared outputs require explicit output bindings or unique schema matches.
- Result construction returns all field-declared output lanes in declaration order.

## Scopes

Scopes identify which symbolic row stream owns a field reference.

```text
ScopeRef
  id
  kind
  name
  schema
  source_input
  join_occurrence
```

Scope kinds:

- `input`: declared transform input scope;
- `current`: current pipeline row scope;
- `joined`: right-side joined scope;
- `projected`: output scope after schema projection.

Rules:

- Every `FieldRef` must reference a `ScopeRef`.
- Scopes must be unique within a transform plan.
- Joined scopes records the joined input and occurrence number.
- Repeated joins of the same input must receive deterministic occurrence numbers.
- Scope names used for generated aliases must be derived from scope metadata, not Python object identity.
- Field names are resolved through scopes, never through unqualified strings after a join.

Scope metadata is the bridge between join semantics, generated aliasing, diagnostics, and traceability.

## Operation Model

Every operation variant has common metadata:

```text
Operation
  id
  kind
  ordinal
  source
  reads
  writes
  streaming_support
```

Rules:

- `ordinal` is the source-semantic order within the owning step.
- `reads` records referenced scopes and fields when available.
- `writes` records output fields or scopes produced by the operation when available.
- `streaming_support` may be absent before the streaming compatibility pass and filled later.
- Operations is immutable after construction or copied on update by later passes.

Operation kinds outside the supported set fails before online execution or generation.

## Filter Operation

```text
FilterOperation
  predicate
```

Rules:

- `predicate` must be a boolean `Expr`.
- Filters preserve source order relative to joins, hooks, and projections.
- Adjacent filters may be combined by emitters only when semantics remain unchanged.
- Filters before a join may reference only scopes available before that join.
- Filters after a join may reference that joined scope.
- `where(expr.is_not_null())` should retain enough expression metadata for nullability narrowing checks.

The IR should not decide whether generated code spells the operation as `.where(...)` or another backend equivalent.
That choice belongs to the backend emitter.

## Project Operation

```text
ProjectOperation
  output_schema
  assignments
  base_scope

ProjectAssignment
  field
  expression
  source
```

Rules:

- `output_schema` is the step output schema.
- `assignments` must cover every effective field in `output_schema`.
- Assignment order follows `output_schema.fields`.
- Each assignment maps exactly one output field to one expression.
- Duplicate output field assignments are invalid.
- `base_scope` records the schema base overlay source when the user wrote `SchemaClass.base(row)(...)`.
- No implicit carry-through columns exist in IR. Carry-through fields are explicit assignments from `base_scope`.
- Extra columns are not part of a compiled projection unless a hook and schema mode explicitly allow them later.

Projection is the typed schema boundary between steps. It is also the primary source for column-level static dataflow.

## Operation Metadata

Ordered `OperationPlan` records attach compiler-visible metadata to filters, joins, and reserved operations before
target lowering. The metadata gives each operation family one shape for capability checks, cardinality reporting, and
streaming classification.

```text
OperationPlan
  kind
  family
  capability
  cardinality
  streaming
  filter
  join

OperationCapability
  group
  name
  source
  docs
```

`capability` is backend-neutral IR metadata. Target mappers translate it to the target layer's
`CapabilityRequirement` before lowering. The PySpark profile accepts supported operation capabilities and rejects
reserved capabilities with `BACKEND-E2402`.

Allowed cardinality values are:

- `row_preserving`;
- `row_filtering`;
- `row_multiplying`;
- `aggregate`;
- `select_one`;
- `unknown`.

Current filter operations record `row_filtering`. Current `join_one(...)` operations record `select_one` because
the operation expresses lookup intent: one matching right-side row is selected for each left-side row, even when the
compiler emits a warning that uniqueness is not proven. `streaming` reuses the compileability vocabulary
`compatible`, `batch_only`, and `unknown`.

Grouped aggregate operations record `aggregate` cardinality and carry explicit grouping keys plus output assignments.
Supported aggregate assignments are grouped key projection, `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`,
`max(...)`, `avg(...)`, and representative `first(...)` for grouped parent struct fields. Target recipes lower these
assignments through Spark-visible `groupBy(...).agg(...)` operations, not hooks or UDFs.
`sum(...)` and `avg(...)` accept numeric expressions. `min(...)` and `max(...)` accept orderable scalar expressions.
Nullable inputs to null-sensitive aggregates produce nullable aggregate expressions and does not feed non-nullable
output fields without an explicit repair.

Selected-row operations record `select_one` cardinality and carry explicit `partition_by` expressions, one `order_by`
expression, a `latest` or `earliest` direction, and a deterministic tie policy. They are the admitted narrow window
slice for latest/earliest row selection and lower through target recipes to Spark-visible ranking, not hooks or UDFs.
Broad ranking, lag/lead, rolling frames, and keyed duplicate removal remain separate features.

`structure explain` displays each step's ordered operations as `kind(cardinality)`. Aggregate explain output also names
grouping keys and aggregate metric functions, and selected-row output names the helper direction and partition count.
This is an anchor for later v2 explain output, not a full v2 lineage or optimizer report.

## Join Operation

```text
JoinOperation
  method
  joined_input
  joined_scope
  join_type
  hint
  key_pairs
  cardinality
  right_fields

JoinKeyPair
  left
  right
  equality
  ordinal
```

Supported values:

- `method`: `join_one`;
- `join_type`: `left`, `inner`;
- `hint`: none or `broadcast`;
- `equality`: normal or null-safe.

Rules:

- Key-pair order follows source order after flattening boolean AND.
- Each key pair includes one expression from the joined input scope and one expression from the current or already
  available scope.
- Composite keys must reference the same joined input for one join operation.
- `joined_scope` records occurrence and alias metadata.
- `cardinality` records whether `join_one(...)` uniqueness is proven, unproven, or explicitly unchecked.
- `right_fields` records right-side fields needed by downstream filters, projections, diagnostics, or traceability.
- IR does not silently deduplicate right-side rows.

Detailed join behavior is owned by [JoinSemantics.md](JoinSemantics.md).

## HookCall Operation

```text
HookCall
  name
  timing
  target_step
  pass_inputs
  schema_mode
  project_output
  streaming_safe
  source
```

Values:

- `timing`: `before` or `after`;
- `schema_mode`: strict default or `allow_extra_columns` at minimum.

Rules:

- Hooks are opaque runtime boundaries.
- Hook calls preserve source order for the same timing and target step.
- `target_step` references a `StepPlan`.
- `pass_inputs` records whether online and generated execution passes the original named inputs namespace.
- Hook calls does not contain the runtime DataFrame returned by the hook.
- Hook calls does not contain generated PySpark source text.
- `schema_mode` and `project_output` is present for after-hook validation and projection decisions.
- `streaming_safe` records the author promise used by streaming compatibility checks.

If hooks are stored outside `operations`, the compiler must still expose them as IR nodes with stable ids and
provenance.

## ValidateSchema Operation

```text
ValidateSchemaOperation
  target
  schema
  mode
  projection
  reason
```

Fields:

- `target`: input scope, current DataFrame, step output, hook output, or final output.
- `schema`: expected `SchemaDef`.
- `mode`: strict default or `allow_extra_columns` at minimum.
- `projection`: whether validation is followed by projection to the schema field order.
- `reason`: input, intermediate, hook, final, or explicit user validation.

Rules:

- Input validation points occur before step execution.
- Intermediate validation points occur after compiled step output and after after hooks when policy enables them.
- Hook validation points follow hook metadata.
- When `project_output=True`, IR must represent validate, project, then strict validate.
- Disabled validation should omit the validation point rather than represent it as a no-op mode.
- Online and generated execution must consume the same validation placement model.

The validation runtime behavior is backend-specific. The IR records only the intent and placement.

## Expression Model

Every expression variant has common metadata:

```text
Expr
  id
  kind
  type
  nullable
  source
  reads
```

Rules:

- `type` is a Structure type, not a Spark `DataType`.
- `nullable` is the expression-level nullability after known narrowing.
- `reads` records referenced fields and scopes when available.
- Expressions must be side-effect-free symbolic values.
- Expressions does not store Python call frames, live Spark objects, or backend-specific rendered code.
- Unsupported expression kinds fails before online execution or generation.

Backends lower expression IR to their own expression model. In the only backend is PySpark.

## FieldRef Expression

```text
FieldRef
  scope
  field
```

Rules:

- `scope` is a `ScopeRef`.
- `field` is a `FieldDef` from the scope schema.
- Field references are always scoped.
- A field reference to an unknown field is invalid.
- Joined-field nullability follows join semantics before assignment compatibility checks run.

Generated code may render some single-scope field references as unqualified columns, but the IR itself remains scoped.

## Literal Expression

```text
Literal
  value
  type
  nullable
```

Rules:

- Literal typing follows [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md).
- `None` is represented explicitly and is nullable.
- Decimal, date, timestamp, string, numeric, and boolean literal metadata is deterministic.
- Literal values must be serializable or have a deterministic diagnostic rendering.
- Large binary literals and unsupported Python objects are out of scope for .

The IR does not decide whether a literal is rendered with `F.lit(...)`. That is a backend lowering decision.

## CallExpr

```text
CallExpr
  function
  args
  kwargs
```

`function` should identify a compileable Structure expression helper, not an arbitrary Python callable object.

Rules:

- Function identity is deterministic and import-oriented when possible.
- Argument order is preserved.
- Keyword arguments are preserved in source order or sorted stable order where semantics allow.
- Calls must be pure symbolic expression calls.
- Backend-specific function selection happens in the target layer.
- Unsupported Python methods, arbitrary lambdas, and runtime callables are invalid in compiled expressions.

Examples include `lower`, `upper`, `trim`, `to_decimal`, `coalesce`, and helper functions declared with `@expr_fn`.

## BinaryExpr

```text
BinaryExpr
  operator
  left
  right
```

Supported operator families:

- equality and inequality comparisons;
- ordering comparisons where the operand types support ordering;
- arithmetic only where admitted by the type compatibility specification;
- null-safe equality when exposed by expression objects.

Rules:

- Operand types must be checked before execution or generation.
- Nullability and type coercion follow [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md).
- Join key extraction may normalize equality operand order, but the original expression tree should remain available
  for diagnostics when practical.

## BooleanExpr

```text
BooleanExpr
  operator
  operands
```

Supported operators:

- `and`;
- `or`;
- `not`.

Rules:

- `and` and `or` operands preserve source order.
- Boolean IR represents symbolic `&`, `|`, and `~`, not Python `and`, `or`, and `not`.
- Python truthiness of symbolic expressions is invalid and should fail before IR construction completes.
- Join conditions in accept AND-combined equality pairs only. Other boolean shapes may still be valid for filters.

## CastExpr

```text
CastExpr
  expression
  target_type
  mode
```

Rules:

- `target_type` is a Structure type.
- `mode` records whether the cast is explicit, helper-driven, or assignment-driven.
- Cast validity follows type compatibility rules.
- Backend-specific cast spelling belongs to the target layer.

Examples include decimal conversion through `to_decimal(...)` and explicit nullable literal casts.

## WhenExpr

```text
WhenExpr
  branches
  otherwise

WhenBranch
  condition
  value
```

Rules:

- Branch order follows source order.
- Conditions must be boolean expressions.
- Branch value types has a common assignable type.
- Nullability is derived from branch values and the presence or absence of `otherwise`.
- Backend-specific `when(...).otherwise(...)` rendering belongs to the target layer.

## Type and Nullability Metadata

IR nodes must carry enough type and nullability information for:

- output assignment compatibility;
- filter predicate validation;
- join key compatibility;
- schema validation placement;
- generated Spark schema import selection;
- helpful diagnostics.

Rules:

- Expression types are Structure types.
- Field types come from `SchemaDef.fields`.
- Joined field nullability is adjusted according to join type.
- Filter-based nullability narrowing is represented in the checker state or in copied expression metadata.
- IR validation rejects assignments that violate type or nullability rules.

The IR may either store final checked type metadata on every expression or store enough raw information for a separate
type checker to derive it deterministically.

## Validation Policy Model

Validation policy combines project configuration, transform decorator settings, and method-level overrides.

```text
ValidationPolicy
  validate_inputs
  input_mode
  validate_intermediate
  intermediate_mode
  validate_outputs
  output_mode
  overrides
```

Rules:

- Method-level `@validate_output(...)` overrides class-level and project-level settings for one step.
- Class-level transform settings override project defaults.
- Input, intermediate, and output validation should be explicit in the executable IR or derivable from policy without
  ambiguity.
- The policy model must distinguish disabled validation from permissive validation.
- Online and generated execution uses the same effective policy.

`input_validation_mode`, `intermediate_validation_mode`, and `output_validation_mode` values are configured outside this
spec. The IR stores the resolved mode used by validation passes.

## Streaming Support Metadata

Streaming checks consume operation IR and may annotate operations or produce a side report.

```text
StreamingSupport
  compatible
  batch_only
  unknown
```

Rules:

- The base IR should expose enough metadata for the checker to classify each operation.
- Hooks default to `unknown` unless `streaming_safe=True`.
- Operations not admitted by `StreamingCompatibility.md` are `batch_only` or `unknown`.
- Transform-level streaming compatibility is derived from operation classifications and policy.
- Streaming metadata should be included in compile reports and traceability when configured.

The checker must be conservative. Unknown is not reported as compatible.

## Provenance Model

Compiler provenance maps source nodes to IR nodes and, when generation runs, to generated nodes.

```text
ProvenanceRecord
  source
  ir_id
  generated_path
  generated_symbol
  generated_line
  kind
```

Rules:

- `source` uses `SourceAnchor`.
- `ir_id` references a stable IR node id.
- Generated fields are optional until code generation runs.
- Provenance records is deterministic.
- Provenance must mark hook boundaries as opaque.
- Provenance does not include runtime row counts, Spark application ids, cluster details, or wall-clock execution
  telemetry.

Compiler provenance is compile-time metadata. Runtime LDJSON traceability is outside through v4 scope unless a future
specification changes that roadmap.

## Static Dataflow Traceability

Static dataflow traceability is inferred from IR without executing Spark jobs.

```text
DataflowRecord
  target
  sources
  operation
  transform
  step
  opaque
```

Examples:

```text
OrderNormalized.customer_id
  <- OrderRaw.customer_id through lower(trim(...))

OrderWithCustomer.customer_name
  <- Customer.name through customers#1 join
```

Rules:

- Projection assignments create column-level dependency records.
- Filters create row-retention dependency records.
- Joins create table and key dependency records.
- Hooks create opaque dependency boundaries.
- Validation creates schema dependency records, not data dependency records.
- Traceability records uses source input names, schema names, field names, step names, and IR ids.
- Traceability is deterministic and compact by default.

Traceability precision may improve over time, but must at least expose transform, input, step, join, projection, hook,
and validation dependencies.

## Capability Metadata

Backend capability checks consume IR plus target configuration.

```text
TargetCapabilities
  backend
  version_range
  features
```

Rules:

- The IR semantic model is not PySpark-specific.
- PySpark capability choices belong to the PySpark target layer.
- Capability diagnostics may attach to IR nodes.
- Unsupported backend operations fails before online execution or generation.
- Online and generated PySpark paths uses the same capability data.

For , `backend` is `pyspark`. Later backends does not require changing public DSL source for existing semantics.

## IR Construction

IR construction happens during symbolic execution.

Rules:

- Symbolic row proxies create scoped `FieldRef` expressions.
- Expression helpers create expression IR.
- `where(...)` records filter operations in the active step context.
- `join_one(...)` records lookup join operations and creates a joined scope.
- `join_many(...)` records row-multiplying join operations and creates a joined scope.
- `exists(...)` and `not_exists(...)` record row-filtering join operations without exposing right-side fields.
- Schema constructors create projection operations.
- Hooks are collected from transform metadata, not executed.
- Validation policy is resolved into explicit plan metadata.
- Source-order semantics are preserved.

IR construction rejects source that cannot be represented safely. It does not create placeholder nodes that later
emitters guess how to interpret.

## IR Validation

Required IR validation checks:

1. Transform has at least one input.
2. Transform has at least one declared output.
3. Transform has at least one compiled step.
4. Input names are unique.
5. Step names are unique.
6. Step source-order schema flow is valid.
7. Every scope reference resolves.
8. Every field reference resolves to a field in the referenced scope schema.
9. Every operation kind is supported for the selected roadmap stage.
10. Every filter predicate is boolean.
11. Every project covers the output schema exactly once and in schema order.
12. Every project assignment is type-compatible and nullability-compatible.
13. Every join condition satisfies `JoinSemantics.md`.
14. Every plain `join_one(...)` records uniqueness proof, warning, or unchecked status; deduped `join_one(...)`
    records the deterministic policy that reduces the right side before lookup.
14. Every hook target resolves to a step.
15. Every hook call has valid timing, schema mode, and `pass_inputs` metadata.
16. Every validation point has a schema, target, mode, and reason.
17. All required source-order boundaries around hooks are preserved.
18. No node contains live Spark objects or runtime DataFrames.
19. Diagnostics have enough context to name the transform and affected step, field, hook, or expression.

Validation should run before any backend lowering. Backend-specific checks may run after generic validation.

## Determinism

For identical source, configuration, Structure version, and target capabilities, the IR is deterministic.

Rules:

- Preserve source order where semantics depend on source order.
- Sort independent collections before serialization or diagnostics.
- Avoid dictionary iteration unless insertion order is deliberate and tested.
- Do not include timestamps.
- Do not include absolute workspace paths in deterministic artifacts.
- Do not include memory addresses or object identities.
- Use stable scope and operation occurrence numbers.
- Emit diagnostics in deterministic order.

Deterministic IR is required for generated-code stability, `--fail-on-diff`, review workflows, provenance, traceability, and
later incremental compilation.

## Immutability and Thread Safety

IR objects should be immutable values after construction.

Rules:

- Prefer frozen dataclasses, tuples, persistent collections, or project-equivalent immutable data types.
- If mutable builders are used, freeze or copy the result before validation and backend consumption.
- Later analysis passes should produce new annotated plans or side reports rather than mutating shared state in place.
- Parallel generation may read the same plan from multiple workers.
- Shared name registries, import collectors, and diagnostics accumulators does not be mutated from parallel workers
  without deterministic merge logic.

Immutability enables caching, safe parallel rendering, and later v2 incremental compile fingerprints.

## Serialization and Debug Output

The implementation should provide a deterministic debug representation for IR.

Rules:

- Debug serialization may be JSON, YAML, or a stable text tree.
- Serialized fields should be sorted or emitted in schema order.
- Source paths should be project-relative.
- Python object reprs are not acceptable for deterministic snapshots.
- Large expression trees may be abbreviated in human CLI output, but full debug dumps should remain available for
  tests and bug reports.
- Serialized IR is a diagnostic/testing aid unless a later compatibility policy makes it a public file format.

`structure explain` may use the same model, but it should present a user-oriented subset rather than a raw object dump.

## Diagnostics

Diagnostic code format, severity names, lifecycle rules, registry requirements, and stable documentation anchors are
owned by [Diagnostics.md](Diagnostics.md). This section defines IR-specific context and message content.

IR diagnostics includes:

- diagnostic code;
- severity;
- transform class;
- step when relevant;
- operation kind when relevant;
- expression, field, input, join, or hook when relevant;
- source location when available;
- problem;
- why it matters when not obvious;
- suggested fix;
- link to this specification or the narrower semantic specification.

Invalid project example:

```text
CompileError IR-E0501: Projection does not assign every output field

Transform:
  orders.transforms.order.EnrichOrders

Subtransform:
  normalize

Output schema:
  OrderNormalized

Problem:
  The projection does not assign OrderNormalized.total.

Use:
  Add total=... to the OrderNormalized(...) constructor, or remove the field from the schema.

See docs/reference/IntermediateRepresentation.md
```

Invalid scope example:

```text
CompileError IR-E0502: Field reference is outside scope

Transform:
  orders.transforms.order.EnrichOrders

Subtransform:
  normalize

Expression:
  customer.name

Problem:
  The customer joined scope is not available before the join that creates it.

Use:
  Move the expression after the join_one(...) call or use a field from the current row scope.

See docs/reference/IntermediateRepresentation.md
```

Backend capability example:

```text
CompileError BACKEND-E0802: Operation is not supported by the target backend

Transform:
  orders.transforms.order.EnrichOrders

Subtransform:
  normalize

Operation:
  WindowProject

Target:
  pyspark >=3.5,<4.1

Problem:
  Broad WindowProject forms are not enabled for the current compiler.

Use:
  Use latest_by(...) or earliest_by(...) for admitted selected-row windows, move broader logic into an explicit hook,
  or wait for the broader v2 windowing specification.

See docs/reference/IntermediateRepresentation.md
```

## Non-Goals

The following are outside IR scope:

- representing arbitrary Python control flow as dynamic DataFrame branches;
- representing implicit Python UDF fallback;
- storing live PySpark `Column` or `DataFrame` objects;
- storing runtime hook output values;
- modeling Airflow DAGs, Spark submit jobs, or orchestration lifecycle;
- modeling `readStream`, `writeStream`, triggers, checkpoints, and query starts;
- modeling grouping sets, broad windows, keyed deduplication, or higher-order forms before their staged specs;
- exposing raw IR classes as public user-facing DSL APIs;
- treating serialized IR as a stable public interchange format.

## Later Extensions

Planned IR variants:

- `GroupingSets`;
- `Rollup`;
- `Cube`;
- `WindowProject`;
- `CacheHint`;
- `JoinStrategyHint`;
- `CardinalityExpandingJoin`;
- `DocumentationModel`;
- `IncrementalCompileFingerprint`.

Rules for adding variants:

- Add the semantic specification first or at the same time.
- Add generic IR validation.
- Add backend capability checks.
- Add online and generated lowering or explicitly mark the feature unsupported for one path.
- Add streaming classification.
- Add provenance and traceability records.
- Add review workflows and parity tests where runtime behavior exists.

`IncrementalCompileFingerprint` should hash stable IR, resolved configuration, relevant source fingerprints, and target
capabilities. It does not hash absolute workspace paths or wall-clock times.

## v3 Extensions

Planned v3 IR variants:

- `ReadStream`;
- `WriteStream`;
- `Watermark`;
- `Trigger`;
- `Checkpoint`;
- `StreamingStatePolicy`.

Rules:

- v3 streaming lifecycle IR must distinguish transform semantics from query orchestration.
- Checkpoints, triggers, watermarks, output modes, and state policies require explicit user-facing semantics before
  they enter IR.
- Runtime telemetry remains separate from compiler traceability unless a later specification merges them deliberately.

## v4 Extensions

Planned v4 IR variants:

- `SparkConnectCapability`;
- `BackendCompatibilityReport`.

Rules:

- Spark Connect support remains behind the backend target boundary.
- Existing transform IR should not change public DSL syntax, generated class construction, `run(...)` signatures,
  or streaming orchestration semantics.
- Backend compatibility reports should explain which operations are supported, unsupported, or degraded for the target.
