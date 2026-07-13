# Hook Semantics

Hooks are Structure's explicit runtime escape hatch. A hook is a method decorated with `@raw`; it lets a developer run
arbitrary backend DataFrame logic at a precise point in the transform class without pretending the hook body is
compiler-visible.

This reference covers hook decorator behavior, signatures, source ordering, input access, schema handling,
streaming-safety metadata, generated and online invocation, diagnostics, and tests.

## Public API

Canonical hook forms:

```python
@raw(inout=lane(orders) | lane(orders))
def prepare(self, *, orders, spark, ctx):
    return orders
```

```python
@raw(inout=input(orders) | lane(orders))
def restore_raw(self, *, orders, spark, ctx):
    return orders
```

```python
@raw(inout=lane(published) | output(published), schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, published, spark, ctx):
    return published.withColumn("_checked", F.lit(True))
```

`@raw` has no step-method argument. The hook executes exactly where its method appears among the transform's other
public methods.

## Decorator Arguments

Keyword arguments:

```text
input=declaration_or_sequence
output=declaration_or_sequence
inout=sources | targets
schema_mode=SchemaMode.STRICT
project_output=False
streaming_safe=False
target_backend=None
target_platform=None
```

Rules:

- Unknown keyword arguments are errors.
- A positional step-method target is invalid.
- If no binding is supplied, the hook consumes and replaces the current source-ordered lane.
- `input=...` selects the DataFrame arguments and `output=...` selects returned-frame destinations.
- `inout=sources | targets` is the concise form that supplies both sides together.
- A hook receives the ordered, de-duplicated union of source and destination names as keyword-only arguments.
- The hook returns one DataFrame per destination, in right-side order.
- Bare declarations use normal resolution: a current lane shadows an input or output with the same name.
- `input(x)`, `lane(x)`, and `output(x)` force the original input, current lane, or named output binding.
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

An explicit hook binding selects the frames it receives and replaces:

```python
@raw(inout=lane(orders) | lane(orders))
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

Original-input signature:

```python
@raw(inout=input(orders) | lane(orders))
def hook(self, *, orders, spark, ctx):
    ...
```

Rules:

- `self` is required.
- Hook runtime parameters must be keyword-only.
- every distinct binding name, `spark`, and `ctx` are required.
- Extra parameters are invalid in v1.
- Hooks must return a DataFrame at runtime.

Signature validation should happen during compiler checks, not only when a hook is first invoked in production.

## Original Inputs

Select an original transform input explicitly with `input(...)`; no read-only namespace object is created.

Example:

```python
@raw(inout=[lane(orders), input(customers)] | lane(orders))
def compare_to_raw(self, *, orders, customers, spark, ctx):
    return orders.join(customers.select("id"), "id", "left")
```

Rules:

- `input(customers)` refers to the original DataFrame bound to `customers = input(...)`, even when a current lane
  has the same name.
- `lane(customers)` instead selects the current working frame.
- Every selected argument must exist at the hook's source-order position; otherwise compilation reports the missing
  binding and recommends an explicit selector.

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
- The v1 compatibility default is `hook_target_default = ["pyspark"]`.
- A future strict mode may use `hook_target_default = "explicit"` to require every hook to declare target backends.
- Runtime execution must not invoke a hook when the active target is outside the hook's effective target set.
- Compatibility checks warn when an unmarked hook inherits a default while checking other targets.
- Compatibility checks warn when a hook appears to import or reference a backend outside its declared target set.

Target scope prevents accidental runtime errors such as calling a PySpark hook with a Polars LazyFrame or DuckDB
relation. It does not make hook internals compiler-visible.

V1 accepts and carries `target_backend` metadata so documented PySpark hook examples are usable now. A hook whose
effective target set excludes `pyspark` must fail during compilation in v1 because PySpark is the only executable hook
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

See docs/reference/HookSemantics.md
```
