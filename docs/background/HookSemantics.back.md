# Hook Semantics

The normative source is [Hook Semantics](../dev/specifications/HookSemantics.spec.md). Composed hook ownership is defined by
the [Composed Hook Ownership design](../dev/design/ComposedHookOwnership.design.md).

Hooks are Structure's explicit runtime escape hatch. A hook is a method decorated with `@raw`; it lets a developer run
arbitrary backend DataFrame logic at a precise point in the transform class without pretending the hook body is
compiler-visible.

This reference covers hook decorator behavior, signatures, source ordering, input access, schema handling,
streaming-safety metadata, generated and online invocation, diagnostics, and tests.

## Hook Lifecycle At A Glance

A hook crosses the compiler/runtime boundary at a declared lane position:

```text
@raw declaration
  -> binding and signature validation
  -> source-order placement in the transform plan
  -> opaque HookDef in IR
  -> target-scope and streaming checks
  -> online or generated invocation
  -> schema validation and lane replacement
```

The compiler validates the boundary around a hook, not the arbitrary backend code inside it. Read the document in that
order: first decide whether a hook is necessary, then declare its bindings and target, and finally verify its runtime
and generated behavior.

Prefer compiler-visible Structure expressions for logic that can be expressed in the DSL. Reserve `@raw` for a genuine
backend escape hatch, such as a public DataFrame operation that has no admitted Structure equivalent.

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
streaming=False
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
- `streaming=True` is an author promise, not compiler inspection of the hook body.
- `target_backend=None` means the hook inherits the configured `hook_target_default`.
- `target_platform` narrows the hook to a target variant of the backend when supported.

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
- Extra parameters are invalid.
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
4. Generated-code execution and execution invoke hooks in the same order.
5. Validation and hook projection follow the shared execution semantic contract at the hook boundary.

Multiple adjacent hooks are allowed. A hook can rely on the DataFrame returned by the previous hook for the same lane.

For example, the following sequence keeps normalization compiler-visible, inserts a raw quality check, and then
publishes the checked lane:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized.project(order)(id=lower(trim(order.id)))

@raw(inout=lane(normalized) | lane(normalized), schema_mode=SchemaMode.STRICT)
def check_quality(self, *, normalized, spark, ctx):
    return normalized.where(F.col("id").isNotNull())

def publish(self, order: OrderNormalized) -> OrderPublished:
    return OrderPublished.project(order)
```

The hook receives the result of `normalize`, not the original input. If it were declared before `normalize`, its lane
binding would resolve differently and its output would not be the same contract. Moving a raw method is therefore a
semantic change even when its body is unchanged.

## Opaque Boundary

Hooks are not symbolically executed.

Rules:

- The compiler does not inspect hook internals for expressions, joins, filters, traceability, or performance guardrails.
- Traceability and explain output must show an opaque hook boundary.
- Diagnostics should prefer direct DSL or ordinary compiler-visible helper fixes; `@special(type="expr")` remains an
  optional explicit marker when it adds clarity.
- Generated code calls hooks on the source transform implementation instance.
- Execution calls the same hook methods on the transform invocation.
- Hook internals may import backend libraries because they run at runtime.

## Backend Target Scope

Hooks are target-specific opaque code. The compiler-visible Structure source may be portable across backends, but a hook
body can rely on one backend's DataFrame API.

Optional hook target declaration:

```python
@raw(inout=lane(orders) | lane(orders), target_backend="pyspark")
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Rules:

- `target_backend` may be a backend id, a list of backend ids, `"configured"`, or `"all"`.
- Use `target_backend="pyspark"` for a single backend.
- Use `target_backend=["pyspark", "polars"]` only when one hook intentionally supports multiple Python-hosted backends.
- Missing `target_backend` resolves from `hook_target_default` in configuration.
- The compatibility default is `hook_target_default = ["pyspark"]`.
- A future strict mode may use `hook_target_default = "explicit"` to require every hook to declare target backends.
- Runtime execution must not invoke a hook when the active target is outside the hook's effective target set.
- Compatibility checks warn when an unmarked hook inherits a default while checking other targets.
- Compatibility checks warn when a hook appears to import or reference a backend outside its declared target set.

Target scope prevents accidental runtime errors such as calling a PySpark hook with a Polars LazyFrame or DuckDB
relation. It does not make hook internals compiler-visible.

The compiler accepts and carries `target_backend` metadata so documented PySpark hook examples are usable now. A hook
whose effective target set excludes `pyspark` must fail during compilation because PySpark is the only executable hook
ABI.

## Schema Handling

Hooks receive and return DataFrames.

Rules:

- The selected lane parameter has the shape produced by the previous stage at that boundary.
- A hook must return a DataFrame.
- By default, returned shape must match the target schema in strict mode.
- `schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS` permits additional columns at that hook boundary.
- `project_output=True` projects the hook result back to the target schema.
- Hook output validation placement must match execution and generated-code execution.

`SchemaMode` must include at least:

```text
STRICT
ALLOW_EXTRA_COLUMNS
```

Public examples may omit the strict default.

Choose schema behavior according to ownership of the hook output:

| Hook output | Recommended mode | Meaning |
| --- | --- | --- |
| same columns and types | `STRICT` | backend logic preserves the declared lane contract |
| temporary or diagnostic extras | `ALLOW_EXTRA_COLUMNS` | extras may cross this boundary |
| intentional narrowing or shaping | `project_output=True` | project back to the target schema |

Projection is not a substitute for declaring required fields. A hook that drops a required column must still fail the
target schema contract; a hook that adds an auxiliary column should use extra-column mode only when the next stage can
legitimately see it.

## Streaming Safety

Hooks are batch-only by default for streaming compatibility checks.

Rules:

- A hook in a streaming-compatible transform must declare `streaming=True`.
- `streaming=True` means the author promises the hook uses only backend operations valid for the runtime streaming
  shape.
- Structure may still reject a streaming-safe hook when its declared schema mode or input access is incompatible with
  the configured backend.
- Hook internals remain opaque, so runtime backend failures inside a hook are not compiler proof failures.

The streaming declaration applies to the complete hook body, including helper calls and any operation selected by a
backend expression. It does not make an unsupported stateful operation safe. A hook that reads or writes external state,
uses a batch-only action, or depends on unbounded local collection must remain batch-only unless a separate streaming
contract admits that behavior.

## IR Contract

Hook metadata recorded in IR:

```text
HookDef
  name
  source_order
  source_lanes
  output_lanes
  inputs
  schema_mode
  project_output
  streaming
  target_backend
  target_platform
  target_defaulted
  source_path
  source_line
```

For composed transforms, the hook owner remains the declaring stage. A wrapper must retain stage ordinal or graph-stage
name, owner class, hook name, lane bindings, schema mode, validation flags, target scope, and streaming declaration.
Online execution and delegated generated execution create one private implementation instance per hook-owning stage per
pipeline invocation; hooks from separate stages never share an instance. `embed_hooks` copies every eligible raw hook
under a deterministic stage/owner-qualified name, and is all-or-error for the composed artifact. Embedding changes
packaging, not hook order, bindings, validation, traceability, or streaming classification.

Composition follows invocation-level `.to(...)` order or dependency order induced by graph output references. Independent branches
retain local hook order but promise no order between branches until a later stage consumes both. Internal lanes remain
internal to their declaring transform and are not composition boundaries.

The shared PySpark execution plan lowers each `HookDef` to a deterministic hook call recipe consumed by execution and
generated-code execution.

## Diagnostics

Example:

```text
CompileError HOOK-E0701: Invalid hook signature

Hook:
  EnrichOrders.compare_to_raw

Problem:
  Hooks must declare every selected DataFrame as a keyword-only parameter.

Use:
  def compare_to_raw(self, *, orders, customers, spark, ctx):
      return orders

See docs/background/HookSemantics.back.md
```

## Acceptance Contract

Hook support is complete when tests cover decorator metadata, source order, default and `pass_inputs=True` signatures,
read-only original-input namespaces, opaque boundaries, strict and extra-column schema modes, projection, target scope,
streaming safety, online/generated parity, composed owner dispatch, embedded-hook all-or-error behavior, traceability,
and diagnostics with source-order context.
