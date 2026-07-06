# Spark Connect Design

Spark Connect support keeps Structure's PySpark authoring model intact while allowing callers to supply a remote Spark
Connect session instead of an in-process classic PySpark session. The design goal is not a second backend and not a SQL
rewrite. It is a supported PySpark target variant for compiler-visible batch transforms once the project has live
runtime evidence, CI coverage, and diagnostics that prevent classic-only assumptions from leaking into generated or
online execution.

## Design Position

Spark Connect remains inside the PySpark target boundary:

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

The variant uses the same Structure DSL, checked IR, generated class constructor, `run(...)` signature, and
`StructureSession` shape as ordinary PySpark. The caller owns the Spark session. For Connect, that session is a
Spark Connect client session, but Structure does not create a remote server or hide connection management behind a
new runtime abstraction.

Full support in Sprint 09 means supported batch execution for completed v1/v2 compiler-visible features. It does not
mean Structure owns streaming source and sink orchestration, storage writes, or every arbitrary PySpark hook body.

## First Slice Boundary

The first Spark Connect slice already established the configuration variant and static capability boundary. It proved
that Spark Connect is not a peer backend, documented classic-only exclusions, and added parity checks for generated
shape and backend capabilities.

That slice intentionally left out the evidence needed for a support claim:

- live online execution against a Spark Connect session;
- live generated-code execution against a Spark Connect session;
- CI provisioning or documented manual verification for a Connect server;
- runtime session guardrails and setup diagnostics;
- hook compatibility rules beyond the generic PySpark hook boundary;
- generated source scans that prove no classic-only internals are emitted;
- StructureTools behavior when schema extraction uses a Connect session;
- release notes and public compatibility wording that distinguish supported batch Connect from future streaming work.

## Supported Surface

Sprint 09 promotion covers completed compiler-visible batch features only:

- projections, filters, casts, literals, and standard expression helpers;
- v1 joins, completed v2 analytical joins, and the implemented full rowset join pass: right, full, cross, non-equi, and
  disjunctive joins;
- first-slice aggregations plus implemented advanced analytical operations: rollup, cube, grouping metadata helpers,
  additional aggregate metrics, metric-local filters, reusable windows, distribution/value/window aggregate helpers,
  selected-row helpers, ranking, lag/lead, rolling metrics, exact and subset dedupe;
- the implemented compiler-visible array and map higher-order helper set;
- schema materialization, schema-only validation, strict projection, and generated schema imports;
- explicit optimization directives only when the PySpark DataFrame API supports the directive without classic internals;
- online and generated execution through the same PySpark recipe layer.

The design deliberately excludes:

- Structure-owned streaming source, sink, trigger, watermark, and state policy generation;
- storage write orchestration and table lifecycle management;
- RDD access, SparkContext access, direct JVM/Py4J calls, `_jdf`, and private classic PySpark fields;
- automatic fallback to Python UDFs, local collection, row-wise loops, or SQL string rewrites;
- deferred advanced analytical boundaries such as `grouping_sets(...)` and post-aggregate `having(...)`;
- opaque hook bodies unless the hook is explicitly scoped and the user accepts responsibility for Connect-compatible
  PySpark code.

## Runtime Shape

`StructureSession(spark=..., ctx=...)` remains the only runtime dependency container. Structure should use duck typing
for runtime behavior: generated and online code call public DataFrame, Column, and SparkSession methods that exist in
the supported PySpark Connect API. Compiler commands must remain Spark-free and must not import PySpark, start Java,
create a session, or contact a remote server.

Runtime guardrails belong at the edges:

- configuration resolution identifies `target_variant = "spark-connect"` without importing PySpark;
- `structure doctor` or an equivalent setup check may validate a live Connect session only when the user asks for a
  runtime check;
- online and generated parity tests use caller-created sessions and never rely on a local `SparkContext`;
- diagnostics name the classic-only feature, explain why Connect cannot run it, and link to the Spark Connect reference.

## Hook Policy

Hooks are opaque runtime code. Structure cannot prove that arbitrary hook bodies are Connect-compatible. A hook is
Connect-supported only when all of the following are true:

- its effective target set includes the configured PySpark target variant;
- its signature follows the existing hook ABI with selected lane DataFrames, optional `inputs`, `spark`, and `ctx`;
- it uses public PySpark DataFrame, Column, and functions APIs that work with Spark Connect;
- it does not use SparkContext, RDDs, direct JVM/Py4J access, `_jdf`, or private classic fields.

Static checking should reject explicit classic-only declarations. It may warn, rather than reject, for opaque hook
bodies that cannot be inspected deeply. Runtime failures from unsupported hook internals should be wrapped when
practical with a diagnostic that points users to rewrite the hook as compiler-visible Structure DSL or Connect-safe
PySpark.

## Support Evidence

Spark Connect moves from experimental to supported for batch features only after these facts are true:

- the capability profile has explicit supported and unsupported decisions for every completed v1/v2 batch feature;
- recently implemented full rowset joins and advanced analytical helpers are included in the Connect capability and
  generated-source guardrail matrix;
- online and generated parity tests pass against a real Spark Connect session;
- generated source snapshots are identical in public API shape and do not emit classic-only internals;
- `structure check`, `compile`, and `explain` remain Spark-free;
- CI or a documented manual verification script exercises Spark Connect for each supported PySpark line;
- public docs explain the supported batch surface and the remaining exclusions.

This is a stricter bar than "the generated code looks similar." Spark Connect becomes supported only when a user can
run the same completed batch transform online and generated against Connect and receive clear diagnostics before they
hit unsupported classic behavior.

## Design Consequences

The PySpark target layer owns all Connect-specific decisions. Generic IR should not grow Spark Connect branches. The
generated code should stay readable and should not fork into separate ordinary and Connect modules unless a public API
or import genuinely differs. Capability checks decide whether a Structure operation is legal for the configured variant;
renderers and online runners consume those decisions rather than rediscovering them.

V3 streaming orchestration can later add Connect-specific streaming evidence, but Sprint 09 must not bundle that work
into the batch support claim.
