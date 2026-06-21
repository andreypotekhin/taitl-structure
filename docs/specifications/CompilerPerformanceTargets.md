# Compiler Performance Targets

## Purpose

Structure must feel fast during local development and CI. Compiler commands should validate source, build IR, and check
generated-code freshness without starting Spark. This specification defines performance targets, measurement rules,
architecture constraints, diagnostics, and acceptance tests for compiler speed.

Runtime Spark job performance is outside this document except where compiler choices affect generated or online plan
quality.

## Commands in Scope

Performance targets apply to:

```text
structure check
structure compile
structure compile --fail-on-diff
structure explain <transform>
```

`structure clean` and future cache commands should remain cheap but do not need the same targets.

## Project Sizes

Use these reference project sizes:

```text
Small project:
  10 transforms
  50 schemas

Medium project:
  100 transforms
  300 schemas

Large project:
  500 transforms
  1500 schemas
```

The model fixtures under `tests/model` may grow into synthetic projects for benchmarks. Benchmarks should avoid live
Spark and use deterministic local files.

## Targets

Warm means caches already exist and source fingerprints are unchanged. Cold means no Structure compiler cache exists.

```text
Small project:
  structure check under 2 seconds warm
  structure check under 5 seconds cold

Medium project:
  structure check under 10 seconds warm
  structure check under 30 seconds cold

Single-file edit, v2 incremental compile:
  affected-transform feedback under 2 seconds

Large project, future target:
  structure check under 60 seconds cold
```

These are engineering targets, not release-blocking promises until benchmark infrastructure exists. Architecture should
be chosen so these numbers remain plausible.

## Hard Constraints

Compiler commands must not:

- import PySpark;
- start Java;
- create Spark sessions;
- contact Spark clusters;
- inspect live DataFrames;
- run Spark actions;
- import generated PySpark as part of source checking;
- use network access for ordinary compilation.

Any implementation path that violates these constraints is not a valid compiler optimization.

## Complexity Budgets

Expected complexity:

- source file enumeration: linear in discovered Python files;
- schema extraction: linear in schema classes plus field declarations;
- inheritance resolution: linear after graph validation;
- transform discovery: linear in transform classes and methods;
- symbolic execution: linear in captured operations for supported source;
- generated text emission: linear in emitted files and operations;
- diagnostics sorting: `O(n log n)` in diagnostic count is acceptable.

Avoid algorithms that compare every transform to every schema unless there is an indexed reason. Use maps keyed by
qualified name, schema identity, input name, and source fingerprint.

## Caching

The compiler should be designed around a cache that can be implemented incrementally.

Possible layout:

```text
.structure/cache/
  config_fingerprint.json
  source_hashes.json
  discovered_modules.json
  schemas/
  transforms/
  ir/
  generated_hashes.json
```

Rules:

- Cache keys include Structure version, effective configuration, source root list, generated package, target backend,
  and target version range.
- Cache contents must be deterministic and safe to delete.
- Deleting `.structure/cache` must not break correctness.
- `structure check` may rebuild cache.
- `structure compile --fail-on-diff` must not trust stale generated hashes without verifying source fingerprints.

v1 may use full recomputation. v2 incremental compile should be possible without replacing public APIs.

## Measurement

Benchmark measurements should record:

- command;
- Structure version or git commit;
- project size;
- cold or warm cache;
- platform;
- Python version;
- elapsed wall time;
- cache hit rate when available;
- number of discovered modules, schemas, transforms, and operations.

Benchmarks should run in process where possible, but CLI end-to-end timings are required before public performance
claims.

## Performance Diagnostics

When `strict_performance = true`, unsupported source must fail rather than falling back to slow hidden behavior.

Diagnostics must be emitted for:

- compiler path attempts to import PySpark;
- unsupported Python operations that would require UDF-like fallback;
- hook-heavy transforms when explain output can show opaque boundaries;
- cache corruption or stale cache invalidation.

Example:

```text
CompileError DSL-E0401: Unsupported expression

Problem:
  Python string methods cannot be compiled to Spark Column expressions.

Why this matters:
  Silent fallback would hide work from Spark's optimizer and violate strict_performance.

Use:
  customer_id=lower(trim(order.customer_id))

See docs/specifications/CompilerPerformanceTargets.md
```

## Generated and Online Plan Guardrails

Compiled target plans must not contain:

- Python UDFs;
- Pandas UDFs;
- RDD conversions;
- row-wise Python loops;
- `collect`;
- `toPandas`;
- hidden Spark actions for schema-only validation.

Hooks are excluded from these guardrails because they are explicit opaque runtime boundaries. Explain output and lineage
should make hook boundaries visible.

## Implementation Checklist

1. Keep compiler commands Spark-free.
2. Add a no-PySpark-import test for `structure check` and `structure compile`.
3. Record counts and elapsed time in debug output or benchmark harness.
4. Implement deterministic source fingerprints.
5. Design cache records so they can be deleted and rebuilt safely.
6. Use indexed lookup structures for schemas, transforms, inputs, and generated paths.
7. Add guardrail tests for prohibited compiled-path operations.
8. Add benchmark fixtures for small and medium synthetic projects.
9. Add CI-friendly performance smoke tests with loose thresholds.
10. Document benchmark commands once the CLI exists.

## Acceptance Criteria

- Compiler commands can run in an environment where PySpark is not installed.
- Tests prove compiler checks do not import PySpark through Structure internals.
- A small fixture can run `structure check` inside the target cold and warm budgets once benchmarks exist.
- Unsupported Python operations fail instead of generating UDFs or row-wise callbacks.
- Schema-only validation recipes do not contain Spark actions.
- Cache deletion is safe.
- Generated output freshness checks use source and config fingerprints, not only file timestamps.
- Performance debug output can identify discovered module, schema, transform, operation, and diagnostic counts.
