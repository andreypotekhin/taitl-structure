# Hook Semantics

## Purpose

Hooks are Structure's explicit runtime escape hatch. They let a developer attach arbitrary PySpark DataFrame logic to a
specific compiled subtransform without pretending the hook body is compiler-visible.

This specification owns hook decorator behavior, target binding, signatures, ordering, input access, schema handling,
streaming-safety metadata, generated and online invocation, diagnostics, and tests.

## Public API

Canonical hook forms:

```python
@before(normalize, lane=orders)
def prepare(self, *, orders, spark, ctx):
    return orders
```

```python
@after(normalize, lane=orders, pass_inputs=True)
def compare_to_raw(self, *, orders, inputs, spark, ctx):
    return orders
```

```python
@after(publish, lane=published, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, published, spark, ctx):
    return published.withColumn("_checked", F.lit(True))
```

`@before(...)` runs before the compiled operations of the target subtransform. `@after(...)` runs after the compiled
operations of the target subtransform.

## Decorator Arguments

Required positional argument:

- target subtransform method object.

Keyword arguments:

```text
lane=declaration
pass_inputs=False
schema_mode=None
project_output=False
streaming_safe=False
```

Rules:

- Unknown keyword arguments are errors.
- More than one positional argument is an error.
- The target must be a compiled subtransform on the same transform class.
- `@before(...)` must select the target input lane with `lane=...`.
- `@after(...)` must select the target output lane with `lane=...`.
- The target may be referenced inside the class body because earlier methods are present in the class namespace.
- `schema_mode=None` means strict default validation.
- `project_output=True` requires a schema mode and target schema that make projection meaningful.
- `streaming_safe=True` is an author promise, not compiler inspection of the hook body.

## Signatures

Default signature:

```python
def hook(self, *, selected_lane_name, spark, ctx):
    ...
```

Every hook explicitly selects the lane it receives. A before hook selects the target subtransform input lane:

```python
@before(normalize, lane=orders)
def prepare(self, *, orders, spark, ctx):
    ...
```

An after hook selects the target subtransform output lane:

```python
@after(add_product, lane=audited)
def audit(self, *, audited, spark, ctx):
    ...
```

The selected lane is passed through a keyword parameter with the same name. The hook return value replaces only that
lane.

Input-access signature:

```python
def hook(self, *, selected_lane_name, inputs, spark, ctx):
    ...
```

Rules:

- `self` is required.
- Hook runtime parameters must be keyword-only.
- the selected lane parameter, `spark`, and `ctx` are required.
- `inputs` is required only when `pass_inputs=True`.
- `inputs` is invalid when `pass_inputs=False`.
- Extra parameters are invalid in v1.
- Hooks must return a DataFrame at runtime.

Signature validation should happen during compiler checks, not only when a hook is first invoked in production.

## Hook Inputs

When at least one hook declares `pass_inputs=True`, runtime execution creates a read-only namespace of original
transform inputs.

Example:

```python
@after(normalize, lane=orders, pass_inputs=True)
def compare_to_raw(self, *, orders, inputs, spark, ctx):
    return orders.join(inputs.orders.select("id"), "id", "left")
```

Rules:

- `inputs.orders` refers to the original DataFrame bound to the declared `orders = input(...)`.
- The namespace contains original declared inputs only.
- It does not contain intermediate step DataFrames.
- It is read-only; assigning `inputs.orders = ...` is invalid if the namespace can prevent it.
- Missing original inputs are normal transform input binding errors, not hook-specific errors.

## Ordering

Hook order is deterministic:

1. Subtransforms execute in source order.
2. For each subtransform, `@before` hooks run in source order.
3. Compiled operations for the subtransform run.
4. `@after` hooks run in source order.
5. Validation and hook projection follow the shared execution semantic contract.

Multiple hooks of the same timing and target are allowed. A hook can rely on the DataFrame returned by the previous hook
for the same timing and target.

## Opaque Boundary

Hooks are not symbolically executed.

Rules:

- The compiler does not inspect hook internals for expressions, joins, filters, traceability, or performance guardrails.
- Traceability and explain output must show an opaque hook boundary.
- Diagnostics should prefer direct DSL or `@expr_fn` fixes when logic can stay compiler-visible.
- Generated code calls hooks on the source transform implementation instance.
- Online execution calls the same hook methods on the transform invocation.
- Hook internals may import PySpark because they run at runtime.

## Schema Handling

Hooks receive and return DataFrames.

Rules:

- The selected lane parameter has the shape produced by the previous stage at that boundary.
- A hook must return a DataFrame.
- By default, returned shape must match the target schema in strict mode.
- `schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS` permits additional columns at that hook boundary.
- `project_output=True` projects the hook result back to the target schema.
- Hook output validation placement must match online and generated execution.

`SchemaMode` must include at least:

```text
STRICT
ALLOW_EXTRA_COLUMNS
```

Public examples may omit the strict default.

## Streaming Safety

Hooks are batch-only by default for streaming compatibility checks.

Rules:

- A hook in a streaming-compatible transform must declare `streaming_safe=True`.
- `streaming_safe=True` means the author promises the hook uses only Spark operations valid for the runtime streaming
  shape.
- Structure may still reject a streaming-safe hook when its declared schema mode or input access is incompatible with
  the configured backend.
- Hook internals remain opaque, so runtime Spark failures inside a hook are not compiler proof failures.

## IR Contract

Hook metadata recorded in IR:

```text
HookDef
  name
  target_step
  timing
  source_order
  pass_inputs
  schema_mode
  project_output
  streaming_safe
  source_path
  source_line
```

The shared PySpark execution plan lowers each `HookDef` to a deterministic hook call recipe consumed by online and
generated execution.

## Diagnostics

Hook diagnostics must include:

- transform class;
- hook name;
- target subtransform;
- timing;
- source location when available;
- decorator options;
- signature shape;
- problem;
- suggested fix;
- documentation link.

Example:

```text
CompileError HOOK-E0701: Invalid hook signature

Hook:
  EnrichOrders.compare_to_raw after normalize

Problem:
  Hooks with pass_inputs=True must declare keyword-only inputs.

Use:
  def compare_to_raw(self, *, orders, inputs, spark, ctx):
      return orders

See docs/specifications/HookSemantics.md
```

## Implementation Checklist

1. Implement `@before(...)` and `@after(...)` metadata capture.
2. Prove decorator target references work inside class bodies.
3. Preserve hook source order.
4. Validate hook targets against compiled subtransforms on the same class.
5. Validate hook decorator keyword arguments.
6. Validate signatures for default and `pass_inputs=True` modes.
7. Record hook metadata in transform IR.
8. Build hook input namespaces only when needed.
9. Invoke hooks identically in online and generated execution.
10. Implement hook schema mode and projection recipes.
11. Integrate hook boundaries with traceability and explain output.
12. Add streaming-safety checks.
13. Add diagnostics with links to this specification.

## Acceptance Criteria

- `@after(normalize, lane=orders)` binds to a method declared earlier in the same class body.
- `@before(...)` and `@after(...)` preserve source order for the same target.
- A hook targeting a non-subtransform fails.
- A hook targeting a method on another class fails.
- Default hooks require `def hook(self, *, selected_lane_name, spark, ctx)`.
- `pass_inputs=True` hooks require `def hook(self, *, selected_lane_name, inputs, spark, ctx)`.
- Hook input namespaces expose original declared inputs and no intermediate DataFrames.
- Hooks are not symbolically executed during `structure check`.
- Online and generated execution call hooks in the same order.
- Default hook output schema checking is strict.
- `ALLOW_EXTRA_COLUMNS` and `project_output=True` behave the same online and generated.
- Streaming-compatible transforms reject hooks without `streaming_safe=True`.
- Hook diagnostics include target, timing, signature, fix, and docs link.
