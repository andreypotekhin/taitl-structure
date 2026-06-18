# Design: Compiler Performance

## Principle

Compile-time performance is a product metric. Users value fast compilers because fast feedback encourages frequent checks and CI enforcement.

## Goals

- `structure check` should be fast enough for local use.
- `structure compile` should avoid rewriting unchanged files.
- Warm incremental compile should be much faster than cold compile.
- The compiler should never require PySpark, Java, SparkSession, a Spark cluster, or Spark startup during normal
  check/compile.

## Metrics

Track:

- config load time
- discovery time
- source inspection time
- symbolic execution time
- IR construction time
- check time
- codegen time
- formatting time
- compiler provenance time
- static dataflow lineage time
- total time
- files considered
- files written
- transforms compiled
- cache hit ratio

## Techniques

- Source fingerprints.
- Transform-level incremental cache.
- Immutable/hashable IR.
- Lazy source snippet extraction.
- Parallel code generation.
- Write-if-changed output.
- Format-if-changed output.
- Avoid PySpark imports and Spark startup.
- Model target PySpark behavior through static emitter capability metadata.
- Avoid heavyweight AST parsing unless diagnostics require it.

## CLI

```bash
structure compile --profile
```

should emit a compact timing report.

## Testing

Add benchmark fixtures for 10, 100, and 1,000 transforms. Track cold and warm timings.
