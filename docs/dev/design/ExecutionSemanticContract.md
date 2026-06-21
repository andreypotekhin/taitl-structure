# Design: Execution Semantic Contract

## Purpose

Online execution and generated PySpark must be two faces of the same compiled transform. The online runner works with
live PySpark DataFrame and Column objects. The generated emitter writes Python source text. Neither should make its own
semantic decisions after IR validation.

This design introduces a shared PySpark semantic lowering layer. It converts checked backend-neutral `TransformPlan` IR
into deterministic PySpark execution recipes that both runtime paths consume.

## Components

```text
TransformPlan
  -> LowerPySparkExecutionPlan
       -> PySparkExecutionPlan
            -> OnlinePySparkRunner
            -> PySparkCodeGenerator
```

`LowerPySparkExecutionPlan` is a target-layer action. It receives:

- checked `TransformPlan` IR;
- resolved validation policy;
- `PySparkCapabilities`;
- deterministic naming and alias rules.

It returns a `PySparkExecutionPlan`, an immutable target plan that contains operation recipes. The plan is still not
generated source and still not live Spark state.

## Recipe Model

The target plan should expose small, immutable records:

- `PySparkExecutionPlan` for the whole transform;
- `PySparkStepRecipe` for each source-ordered step;
- `PySparkExpressionRecipe` for Column expressions and literals;
- `PySparkJoinRecipe` for join shape, aliases, keys, hints, and right-side fields;
- `PySparkValidationRecipe` for validation and projection placement;
- `PySparkHookRecipe` for hook calls and hook input namespace needs.

These names are intentionally concrete. If implementation finds a better local naming pattern, keep the same
separation of responsibilities: one target plan, small recipes, no generated text, and no live PySpark objects.

## Consumer Responsibilities

`OnlinePySparkRunner`:

- asks the compiler frontend for checked `TransformPlan` IR;
- asks `LowerPySparkExecutionPlan` for the target plan;
- interprets recipes against live input DataFrames;
- calls hooks at recipe-defined lifecycle points;
- uses shared runtime helpers for validation and schema projection.

`PySparkCodeGenerator`:

- asks `LowerPySparkExecutionPlan` for the target plan;
- renders recipes to deterministic Python source;
- owns imports, file headers, comments, formatting, and write-if-changed behavior;
- does not re-order semantic operations while rendering.

`GeneratedPySparkRunner`:

- imports generated classes and calls their `run(...)` method;
- does not participate in semantic lowering at runtime except through the generated class already produced from the
  same target plan.

## Boundaries

The shared lowering layer owns:

- PySpark function mapping;
- literal typing and casts;
- stable aliases;
- validation recipe placement;
- hook recipe placement;
- generated and online operation order;
- capability-selected backend spellings;
- guardrail checks for compiled paths.

The generated emitter owns:

- import ordering;
- source text formatting;
- generated file paths;
- headers and comments;
- snapshot readability.

The online runner owns:

- live DataFrame binding;
- live Column construction from expression recipes;
- runtime hook invocation;
- runtime diagnostics with execution context.

The backend-neutral IR owns:

- source semantics;
- scopes;
- operation order;
- field references;
- schema boundaries;
- provenance anchors.

## First Slice

The first implementation slice should support projection-only transforms:

1. Lower input validation into recipes.
2. Lower a single `ProjectOperation` into projection assignment recipes.
3. Lower final output validation into recipes.
4. Interpret the recipes online.
5. Render the recipes into generated code.
6. Add a parity test proving online and generated outputs match.

Do not add filters, joins, or hooks until projection parity is passing.

## Expansion Rule

Every new compiled operation follows the same path:

1. Specify source behavior.
2. Represent it in backend-neutral IR.
3. Add PySpark recipe records.
4. Add online interpretation.
5. Add generated rendering.
6. Add parity tests.
7. Add guardrail checks.

If direct shared lowering is temporarily too expensive, add the parity test first and extract the shared recipe before
the next operation in that family is admitted.

## Testing

Runtime parity tests are the primary proof. Generated-code snapshots are secondary.

Minimum parity assertions:

- same column order;
- same row values;
- same schema shape where Spark exposes it reliably;
- same validation failure placement;
- same hook lifecycle behavior;
- same diagnostics for unsupported operations where both paths reject the same source.

Keep no-Spark compiler tests separate from PySpark runtime parity tests. The shared lowering layer may be tested without
PySpark when it produces pure recipe objects.

## Acceptance

The design is implemented when:

- both online and generated paths consume `PySparkExecutionPlan` or an equivalent shared target plan;
- projection-only parity passes before richer operations are added;
- each new operation has at least one online/generated parity test;
- generator-specific code no longer chooses semantic aliases, validation placement, or expression mapping by itself;
- online-specific code no longer chooses those semantics separately;
- compiler commands still run without PySpark, Java, Spark startup, a SparkSession, or a Spark cluster.
