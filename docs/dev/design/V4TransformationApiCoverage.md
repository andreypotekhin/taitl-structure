# V4 PySpark Transformation API Coverage

## Purpose

V4 makes Structure a more predictable companion to PySpark for data transformation. A developer encountering a
PySpark Column method, SQL function, or DataFrame transformation should be able to tell immediately whether Structure
supports it, which Structure API to use, or why it deliberately remains outside the DSL.

The goal is not a mechanical one-to-one wrapper. Structure continues to admit only operations that are symbolic,
typed, capability checked, explainable, testable, and readable after lowering to PySpark. The goal is to minimize
surprise, not to make those guarantees disappear.

## Release Boundary

V4 covers transformations over caller-supplied DataFrames. It includes row-local expressions, nested values,
row-preserving or row-reducing relational operations, joins, aggregations, windows, and explicitly modeled
row-expanding operations.

V4 excludes loading and storage, table/catalog management, writes, streaming source and sink ownership, triggers,
checkpoints, output-mode application, query lifecycle, Python UDF/UDTF execution, and actions that materialize data
such as `collect()` or `count()`. These APIs either cause side effects, manage execution rather than transform data, or
cannot remain inside Structure's symbolic transformation contract.

The release includes one separately scheduled streaming-transformation slice:
[V4 Caller-Owned Streaming Migration](V4CallerOwnedStreamingMigration.md). It admits only compiler-visible session
aggregation and bounded join shapes over caller-supplied DataFrames. It does not weaken the exclusions above.

Alternative backend expansion and non-batch Spark Connect operations are not v4 objectives. The supported PySpark
batch target remains ordinary PySpark and Spark Connect for the completed compiler-visible feature set.

## Coverage Contract

The v4 first slice creates a versioned transformation coverage catalog. It is the single source of truth for every
relevant public API in the PySpark 3.5.x and 4.0.x target intersection. Each catalog row records:

- the PySpark API and family, including aliases;
- its Structure spelling, when supported;
- a status of `supported`, `scheduled`, `deferred`, or `unsupported`;
- supported target profiles and variants;
- input types, result type, nullability, and cardinality effect where applicable;
- semantic differences from PySpark, the reason for an exclusion, and links to tests and user documentation.

The catalog is checked by tests. A public transformation API cannot silently disappear from consideration, and a public
Structure helper cannot be documented as supported without a target capability, generated-rendering evidence, and
execution/generated-code parity evidence. The catalog itself is generated or validated from a maintained local inventory; no
build step downloads Spark documentation.

The reference baseline is the intersection of PySpark 3.5.x and 4.0.x. An API available only on part of that range may
be supported only with an explicit capability profile and an actionable diagnostic. APIs added after 4.0.x do not enter
the catalog until the compatibility target changes.

## Admission Rules

Each candidate begins as `scheduled` or `deferred`, never implicitly as supported. A feature becomes supported only
when all of the following are true:

1. Its source syntax is small, typed, and does not expose raw SQL strings or arbitrary PySpark objects.
2. Symbolic execution can determine its operands, output type, honest nullability, and cardinality effect.
3. The intermediate representation records the operation without embedding backend objects.
4. Shared PySpark recipes lower it identically for execution and generated-code execution.
5. Target capability checks reject unsupported PySpark versions or Spark Connect variants before execution.
6. Diagnostics state the legal types and a compiler-visible alternative where the operation is rejected.
7. Unit, generated-code, parity, capability, streaming-classification, and public-reference evidence cover the
   operation at the appropriate level.

Schema constructors continue to own output names and aliases. Therefore Column `alias()` and `name()` do not gain
direct parity. Raw `expr`, raw `WindowSpec`, Python UDTF helpers, and arbitrary callback control flow remain explicitly
unsupported rather than becoming partial escape hatches. Scalar `@special(type="udf")` is existing row-local
ordinary-PySpark batch and streaming support with its warning policy.

## Planned Delivery Order

The release works from the high-frequency, low-ambiguity operations outward. A later slice begins only after the prior
slice has complete catalog and parity evidence.

1. **Coverage foundation.** Establish the catalog, inventory tests, public reference, status vocabulary, and a compact
   v4 testing fixture. Reclassify every existing `planned` API gap against the coverage contract.
2. **Scalar and conditional expressions.** Cover the remaining typed Column methods and common SQL function families:
   boolean and null-control functions, bitwise and numeric functions, complete practical string helpers, temporal
   extraction/conversion, hashing, and encoding. Prefer stable cross-version functions over version-specific novelty.
3. **Nested values and parsing.** Add typed struct mutation, array and map construction/mutation/slicing/sorting, and
   JSON/CSV conversion only where a declared Structure schema supplies an exact result contract.
4. **Relational transformations and analytics.** Close practical rowset transformations such as schema-compatible set
   operations, ordering/limiting where their semantics are useful in a transform, nearest as-of matching, exact
   percentiles, and admitted statistical aggregates. Every operation declares whether it preserves, filters,
   aggregates, multiplies, or otherwise changes rows.
5. **Caller-owned streaming migration.** Implement Sprint 18's static-gap session aggregation, bounded stream-stream
   outer and semi joins, and stream-static semi filtering. Require state/output-mode diagnostics and live evidence on
   both target lines before an operation is supported.
6. **Generators behind a cardinality gate.** Design and prove an explicit schema-and-cardinality model for
   `explode`, `posexplode`, and `inline`. Implement them only if that model keeps output schemas, generated code,
   traceability, and streaming classification unambiguous. Otherwise leave them `deferred` with a precise diagnostic.
7. **Release closure.** Complete documentation, the v4 fixture, public API snapshot, Spark target evidence, and the
   full build. Incremental compile/cache diagnostics remain future work outside v4 and require a later reprioritization
   decision.

## Non-Negotiable Semantics

- Structure uses typed schema projections rather than untyped `selectExpr`, Column aliases, or dynamic column-name
  strings.
- Operations that require a dynamic result schema, an unknown number of output columns, or execution-time Python
  behavior are deferred until Structure has an explicit model for that shape.
- Result nullability must be conservative. Missing collection values, parse failures, outer rows, and nullable input
  behavior must never be represented as non-null merely for convenience.
- Ordering, sampling, randomness, and selected-row operations must name their determinism requirements. A PySpark API
  with nondeterministic behavior is not admitted merely because it can be rendered.
- Streaming compatibility stays a classification concern for transformations. V4 does not widen caller-owned lifecycle
  boundaries.

## Success Measure

At release completion, the public catalog has no unclassified transformation APIs in the supported PySpark baseline.
The most common remaining PySpark transformation APIs have direct, documented Structure equivalents, and every
exception tells the user whether to use a named Structure alternative, a `@raw` hook, or a caller-owned PySpark
boundary.
