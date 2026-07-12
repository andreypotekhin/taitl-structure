# Hook Semantics

## Purpose

Hooks are Structure's explicit runtime escape hatch. A hook is a method decorated with `@raw`; it lets a developer run
arbitrary backend DataFrame logic at a precise point in the transform class without pretending the hook body is
compiler-visible.

This specification owns hook decorator behavior, signatures, source ordering, input access, schema handling,
streaming-safety metadata, generated and online invocation, diagnostics, and tests.

## Public API

Canonical hook forms:

```python
@raw(lane=orders)
def prepare(self, *, orders, spark, ctx):
    return orders
```

```python
@raw(lane=orders, pass_inputs=True)
def compare_to_raw(self, *, orders, inputs, spark, ctx):
    return orders
```

```python
@raw(lane=published, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, published, spark, ctx):
    return published.withColumn("_checked", F.lit(True))
```

`@raw` has no step-method argument. The hook executes exactly where its method appears among the transform's other
public methods.

## Decorator Arguments

Keyword arguments:

```text
input=declaration
inputs=[declaration, ...]
lane=declaration
lanes=[declaration, ...]
output=declaration
outputs=[declaration, ...]
pass_inputs=False
schema_mode=SchemaMode.STRICT
project_output=False
streaming_safe=False
target_backend=None
target_platform=None
```

Rules:

- Unknown keyword arguments are errors.
- A positional step-method target is invalid.
- If no source selector is supplied, the hook consumes and replaces the current source-ordered lane.
- `input(s)=...` and `lane(s)=...` explicitly select one or more current declarations.
- If no output selector is supplied, each selected source is replaced in place.
- `output(s)=...` routes the returned frame or tuple to declared lanes or outputs.
- `schema_mode=SchemaMode.STRICT` is the default validation mode.
- `project_output=True` requires a schema mode and target schema that make projection meaningful.
- `streaming_safe=True` is an author promise, not compiler inspection of the hook body.
- `target_backend=None` means the hook inherits the configured `hook_target_default`.
- `target_platform` narrows the hook to a platform variant of the backend when supported.

## Signatures

Default signature:

```python
def hook(self, *, selected_lane_name, spark, ctx):
    ...
```

An explicit hook source selects the lane it receives:

```python
@raw(lane=orders)
def prepare(self, *, orders, spark, ctx):
    ...
```

An implicit hook source selects the current lane at its declaration position:

```python
@raw
def audit(self, *, normalized, spark, ctx):
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
@raw(lane=orders, pass_inputs=True)
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

1. Transform public methods are scanned in declaration order.
2. A schema-returning method creates a compiled step.
3. A raw method creates a hook at that exact source-order position.
4. Generated and online execution invoke hooks in the same order.
5. Validation and hook projection follow the shared execution semantic contract at the hook boundary.

Multiple adjacent hooks are allowed. A hook can rely on the DataFrame returned by the previous hook for the same lane.

## Opaque Boundary

Hooks are not symbolically executed.

Rules:

- The compiler does not inspect hook internals for expressions, joins, filters, traceability, or performance guardrails.
- Traceability and explain output must show an opaque hook boundary.
- Diagnostics should prefer direct DSL or `@special(type="expr")` fixes when logic can stay compiler-visible.
- Generated code calls hooks on the source transform implementation instance.
- Online execution calls the same hook methods on the transform invocation.
- Hook internals may import backend libraries because they run at runtime.

## Backend Target Scope

Hooks are target-specific opaque code. The compiler-visible Structure source may be portable across backends, but a hook
body can rely on one backend's DataFrame API.

Optional hook target declaration:

```python
@raw(lane=orders, target_backend="pyspark")
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Rules:

- `target_backend` may be a backend id, a list of backend ids, `"configured"`, or `"all"`.
- Use `target_backend="pyspark"` for a single backend.
- Use `target_backend=["pyspark", "polars"]` only when one hook intentionally supports multiple Python-hosted backends.
- Missing `target_backend` resolves from `hook_target_default` in configuration.
- The v.1 compatibility default is `hook_target_default = ["pyspark"]`.
- A future strict mode may use `hook_target_default = "explicit"` to require every hook to declare target backends.
- Runtime execution must not invoke a hook when the active target is outside the hook's effective target set.
- Compatibility checks warn when an unmarked hook inherits a default while checking other targets.
- Compatibility checks warn when a hook appears to import or reference a backend outside its declared target set.

Target scope prevents accidental runtime errors such as calling a PySpark hook with a Polars LazyFrame or DuckDB
relation. It does not make hook internals compiler-visible.

V1 accepts and carries `target_backend` metadata so documented PySpark hook examples are usable now. A hook whose
effective target set excludes `pyspark` must fail during compilation in v.1 because PySpark is the only executable hook
ABI.

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
- `streaming_safe=True` means the author promises the hook uses only backend operations valid for the runtime streaming
  shape.
- Structure may still reject a streaming-safe hook when its declared schema mode or input access is incompatible with
  the configured backend.
- Hook internals remain opaque, so runtime backend failures inside a hook are not compiler proof failures.

## IR Contract

Hook metadata recorded in IR:

```text
HookDef
  name
  source_order
  source_lanes
  output_lanes
  pass_inputs
  schema_mode
  project_output
  streaming_safe
  target_backend
  target_platform
  target_defaulted
  source_path
  source_line
```

The shared PySpark execution plan lowers each `HookDef` to a deterministic hook call recipe consumed by online and
generated execution.

## Diagnostics

Hook diagnostics must include:

- transform class;
- hook name;
- source-order position;
- selected source and output lanes;
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
  EnrichOrders.compare_to_raw

Problem:
  Hooks with pass_inputs=True must declare keyword-only inputs.

Use:
  def compare_to_raw(self, *, orders, inputs, spark, ctx):
      return orders

See docs/dev/specifications/HookSemantics.md
```

## Implementation Checklist

1. Implement `@raw` metadata capture.
2. Reject positional step-method targets.
3. Preserve hook source order.
4. Validate source and output selectors against declared inputs, lanes, and outputs.
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

- `@raw(lane=orders)` selects the declared lane and runs where the method appears.
- Adjacent raw hooks preserve source order.
- A hook with a positional step-method target fails.
- Default hooks require `def hook(self, *, selected_lane_name, spark, ctx)`.
- `pass_inputs=True` hooks require `def hook(self, *, selected_lane_name, inputs, spark, ctx)`.
- Hook input namespaces expose original declared inputs and no intermediate DataFrames.
- Hooks are not symbolically executed during `structure check`.
- Online and generated execution call hooks in the same order.
- Default hook output schema checking is strict.
- `ALLOW_EXTRA_COLUMNS` and `project_output=True` behave the same online and generated.
- Streaming-compatible transforms reject hooks without `streaming_safe=True`.
- Hook diagnostics include source-order context, selectors, signature, fix, and docs link.
