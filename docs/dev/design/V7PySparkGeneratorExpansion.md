# V7 Typed PySpark Generator Expansion

## Decision

V7's first broad API slice expands row generators through typed relation-operation records. It does not expose a raw
PySpark generator expression or allow a generator inside an ordinary projection. A generator changes row cardinality
and may create multiple output fields, so it must have its own declared input collection, generated scope schema, and
result relation.

## Why This Slice Comes First

The baseline catalog already covers the practical scalar, collection, relational, join, aggregate, and window surface.
The remaining row-generator variants are common DataFrame transformations and have a precise contract when represented
as relations. They also exercise the deferred v6 decomposition seams without requiring a Binary type, parser-option
model, or new streaming lifecycle policy.

## Public Model

The existing `posexplode_struct(...)` spelling remains unchanged. New helpers use one operation per PySpark semantic
variant rather than an options flag that conceals null/empty behavior:

- `explode_struct(...)` expands non-null `array<struct>` values into a declared struct scope;
- `explode_outer_struct(...)` preserves a row when the array is null or empty and makes generated members nullable;
- `posexplode_struct(...)` remains the non-outer ordinal form;
- `posexplode_outer_struct(...)` adds a nullable ordinal and nullable generated members for outer rows;
- `inline_struct(...)` expands `array<struct>` into declared sibling fields; and
- `inline_outer_struct(...)` preserves an outer row with nullable sibling fields.

The first slice deliberately names the required struct element shape. Scalar-array `explode` and map expansion are
separate follow-ups because their output naming and map key/value schema contracts are different. This keeps the
authoring API clear and permits each later form to reuse the same relation-operation boundary.

## Internal Boundaries

Before implementation, characterize the current `posexplode_struct(...)` behavior and extract only the following
focused delegates from the named v6 oversized components:

- an operation factory and validation delegate from `dsl/operations_api.py`;
- a generated-scope/schema delegate from `dsl/InputScope.py` and symbolic result construction;
- an operation evaluator from `execution/logic/expressions/EvaluatePySparkExpression.py` or the online runner, based
  on where the current operation is actually interpreted;
- a generator renderer from `render/logic/steps/RenderPySparkStep.py`; and
- a generator traceability mapper from `compiler/commands/BuildCompilerTraceability.py` only if the existing generic
  operation mapper cannot express input collection and generated fields without special cases.

Do not split `dsl/expressions.py`, module rendering, or execution orchestration merely to meet a module-count target.
For each, record either the extracted delegate exercised by this slice or the measured reason its current responsibility
is cohesive. Public imports continue to resolve through `structure.plugin.pyspark`.

## Semantics

The generator operation consumes the current relation and produces a new relation with the declared output schema.
Non-outer variants emit zero rows for null or empty arrays. Outer variants emit one row whose generated fields are null
when the array is null or empty. Input fields remain available according to the declared result schema; output field
names come from Structure schema definitions, never from runtime data. The operation invalidates a previous
relation-order claim because expansion changes row multiplicity.

Generators are batch-only for this slice. A transform that opts into streaming compatibility receives a corrective
diagnostic naming the batch-only generator and advising caller-owned PySpark until v7's streaming gate proves a
supported state contract.

## Evidence

Each extraction begins with a characterization test for the existing public helper. Each added variant requires symbolic
capture, invalid-schema diagnostics, recipe, generated source, online execution, traceability, public API snapshot,
and online/generated parity tests. Live PySpark 3.5.x and 4.0.x tests prove null, empty, one-element, multiple-element,
and nested-field behavior. Generated-source assertions prove no Python UDF, action, or lifecycle call is introduced.

The normative behavior is [V7 Typed Generator Expansion](../specifications/V7GeneratorExpansion.md).
