# Sprint 04: Hooks and Generated Classes

## Sprint Goal

Implement explicit PySpark hook escape hatches while preserving clean generated code for hook-free transforms and
matching online hook behavior.

## Product Outcome

Developers can attach arbitrary PySpark code to a concrete subtransform using `@after(method)` or `@before(method)`.
Online execution and generated code call source hooks at the same lifecycle points.

## Scope

### In Scope

- `@after(method)` hook decorator.
- `@before(method)` hook decorator.
- Hook metadata discovery.
- Hook signature validation.
- Direct source transform import when hooks exist.
- `_impl = SourceTransform()` only when hooks exist.
- Generated hook calls.
- Opt-in original input access with `pass_inputs=True`.
- Hook schema mode options.
- Project output after hook when configured.
- Clean no-hook generated code tests.
- Shared PySpark hook recipes and online/generated parity tests for hook ordering and hook inputs.

### Out of Scope

- Hook source code linting beyond signature and simple strict-performance scanning.
- Passing named input DataFrames to hooks by default.
- Joins.

## Relevant Specification Items

- As a developer, I can attach a hook with `@after(method)`.
- As a developer, I can attach a hook with `@before(method)`.
- As a developer, I can write hook signature `def hook(self, *, df, spark, ctx)`.
- As a developer, I can opt a hook into original input access with `pass_inputs=True`.
- As a developer, generated code directly calls source hooks when hooks exist.
- As a developer, online execution directly calls source hooks when hooks exist.
- As a developer, hook-free generated code remains clean.
- As a developer, hooks are explicit arbitrary PySpark escape hatches.

## Example Source

```python
@after(normalize, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
def add_quality_columns(self, *, df, spark, ctx):
    return df.withColumn("_has_total", F.col("total").isNotNull())
```

```python
@after(normalize, pass_inputs=True)
def compare_to_raw(self, *, df, inputs, spark, ctx):
    return df
```

## Example Generated PySpark

```python
from orders.transforms.order import NormalizeOrders
from structure_generated.runtime.hook_inputs import HookInputs

class NormalizeOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = NormalizeOrders()

    def run(self, *, orders):
        ...
        df = self._impl.add_quality_columns(
            df=df,
            spark=self.spark,
            ctx=self.ctx,
        )

        inputs = HookInputs(orders=orders)
        df = self._impl.compare_to_raw(
            df=df,
            inputs=inputs,
            spark=self.spark,
            ctx=self.ctx,
        )
        ...
```

## Engineering Tasks

1. Implement `after(method)` metadata.
2. Implement `before(method)` metadata.
3. Validate hook references target known subtransform methods.
4. Validate hook signature.
5. Extend IR with `HookCall`.
6. Generate source import only when needed.
7. Generate `_impl` only when needed.
8. Generate direct hook call.
9. Generate original input namespace for `pass_inputs=True` hooks.
10. Implement hook schema mode behavior.
11. Add no-hook cleanliness snapshot tests.
12. Add shared PySpark hook recipes.
13. Add online/generated parity tests for hook ordering and hook inputs.

## Acceptance Criteria

- Hooked transform compiles and runs.
- Hooked transform runs online with the same hook order as generated code.
- Hook-free transform generated code has no source import and no `_impl`.
- Invalid hook signature fails with structured error.
- Hook after a subtransform runs at the correct point.
- Hook lifecycle points come from shared PySpark hook recipes in both runtime modes.
- Hooks with `pass_inputs=True` receive an `inputs` namespace with original named inputs.
- Hook-added columns can be projected away before strict validation.

## Progress

- [x] (2026-06-23) `@before(...)` and `@after(...)` hook metadata, hook recipe lowering, generated hook calls,
  `HookInputs`, hook schema modes, project-output validation, streaming compatibility reporting, and opaque
  traceability boundaries are implemented and tested for v1.
- [x] (2026-06-23) Hook-free generated transforms omit source transform imports and `_impl`; hooked transforms call
  source hooks through shared target recipes.

## Compile-Time Performance Metric

Track discovery overhead for transforms with hooks.

Target:

- Hook metadata should add negligible overhead relative to symbolic execution.

## Risks

- `@after(method)` inside class body must work reliably.
- Hook lifecycle and validation ordering must be clear.
