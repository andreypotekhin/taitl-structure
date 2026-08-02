# V9 API Catalog Design Gates

## Purpose

This design covers APICatalog rows that still need a real contract before implementation. It includes the streaming
design gates and the non-streaming rows currently marked as planned, deferred, or mixed in the public catalog.

The goal is to remove vague backlog language from the catalog. Every open row should say whether it is implemented,
unsupported, streaming-ineligible, caller-owned-guided, or design-gated with a design/specification link.

The remaining Variant completion sequence is maintained in the [V9 Variant ExecPlan](../planning/P07302602.V9-variant-type-and-helpers.plan.md).

## Non-Streaming Gate Families

## Priority

Resolve the non-streaming rows in this order:

1. Sampling, because it is a small relation operation with clear batch-only semantics and high practical value.
2. Aggregate aliases, because the likely result is an explicit not-applicable closure rather than implementation.
3. Nearest as-of joins, because forward/backward as-of joins already exist and the missing design is mostly tie and
   tolerance policy.
4. Missing-column set composition, because it extends existing exact-schema set operations but needs careful schema
   fill rules.
5. Join reordering, because it can affect semantics and must be opt-in, conservative, and explainable.
6. Variant/geospatial type models, because they are larger public type-system decisions.
7. XML helpers, because the user explicitly deprioritized them.

### XML Helpers

XML helpers are deliberately deprioritized. They still need a schema-carrying contract like JSON/CSV parsing, and
Structure should not expose untyped XML strings as a fallback. A future API must define whether XML is a public scalar
conversion, a schema-to-struct parser, or a source format. The parser form must carry an output `Schema`, normalized
options, nullability behavior, malformed-record policy, and PySpark profile support.

The v9 catalog follow-up should keep XML as low-priority `design-gated` documentation unless another active feature
requires it.

### Variant And Geospatial Helpers

Variant and geospatial support are type-model work. Variant field support needs schema rendering, materialization,
nullability, and target capability rules; more advanced serialization, equality, and construction helpers need a later
typed contract. Geospatial support needs geometry/geography types,
coordinate reference policy, function namespace, dependency policy, and cross-target capability checks. These should
not be added as raw SQL wrappers.

Variant field support is Spark-native enough for the bundled PySpark plugin. The admitted v9 slice is a public
`variant(nullable=True)` schema field, rendered and materialized as `VariantType`, guarded by a resolved PySpark 4
profile. The admitted helpers are strict/safe JSON parsing, strict/safe literal-path extraction with an explicit
Structure target type, Array/Map/Struct conversion (with String map keys), JSON-null testing, schema inspection,
merged-schema aggregation, and Spark 4.2 Variant validity checks. Literal Variant values, equality, mutation helpers
that require later Spark profiles, and row-expansion helpers remain outside this slice.

The child execution plan resolves those remaining gaps in this order: freeze the exact Spark profile matrix; add
validated JSON literals and typed equality; add the mutation family on the later Spark profiles that provide it; model
`variant_explode` and `variant_explode_outer` as table-valued row expansion rather than scalar wrappers; then close
dynamic paths, implicit result types, ordering, and provider-specific Python Variant objects as explicit exclusions.
Each step requires generated/online parity, capability tests, and ledger/documentation updates.

Geospatial is different: Spark has no single universally available geospatial API across supported runtimes. Apache
Sedona 1.9.0 is the selected future optional provider because it is open-source and supports the project's PySpark 3.5
and 4.0 targets. V9 admits a Sedona-specific optional geometry slice; the bundled PySpark plugin remains free of every
Sedona dependency and public geospatial export. Structure must not ship raw `ST_*` wrappers in the core PySpark surface.

The admitted slice is planar `GEOMETRY`, represented by `geometry(srid=..., nullable=True)`. `srid` is a required
positive literal on the Structure type; geometry values with different declared SRIDs are incompatible. It admits
WKT construction and serialization plus compiler-visible `intersects`, `contains`, and `within` predicates. The
optional plugin performs its availability check before materialization, rendering, or execution. `GEOGRAPHY`, unknown
or dynamic CRS values, CRS transformation, distance/area/length measurement, spatial joins and indexes, geometry
collections, non-WKT formats, and raw `ST_*` functions remain outside v9.

Selecting Sedona does not add a bundled dependency or import. The optional plugin's Compose integration images must
pin `apache-sedona==1.9.0` and the Spark/Scala-specific Sedona shaded JAR and GeoTools wrapper. The 3.5 and 4.0 lanes
must use their documented Java requirements rather than inherit the current shared image by assumption.

### Geo Provider Selection

The PySpark plugin owns the generic Geometry surface and a public `GeoProvider` extension contract. A provider owns
geometry schema materialization, lowering, generated imports, and runtime availability checks; Structure owns the
typed Geometry/SRID contract. No provider is selected by default and the plugin must not import or require Sedona.

Generated PySpark lowers through the common Spark SQL `GEOMETRY`/`ST_*` contract, never a provider-specific import or
name. The absence of a provider is valid for Spark-free compilation and code generation. Schema materialization and
online execution validate that the active Spark runtime supplies the common contract, reporting it rather than a
provider name. The Compose integration fixture uses Sedona as one conforming implementation, not a special production
path.

The admitted v9 DSL is deliberately small: `geometry`, WKT construction/serialization, and `intersects`, `contains`,
and `within`. All share Structure's declared SRID type rule. Measurements, transformations, spatial joins, indexes,
GEOGRAPHY, and raw `ST_*` calls remain outside the contract.

### Join Reordering

Join reordering may improve performance, but it can change row multiplication, nullability, hint placement, joined-scope
availability, and traceability if applied casually. The v9 outcome is design-gated with no public `join_order(...)`
helper. The current join IR records joins in source order, and later join predicates may legally read scopes created by
earlier joins. Reordering before Structure has dependency-safe predicate analysis would change user-visible semantics.

The only acceptable future design is opt-in and explainable. A future API must restrict the first slice to inner joins
or to joins with proven cardinality constraints, preserve source-order diagnostics, and show the chosen order in explain
output.

Structure should not introduce a cost model in the first slice. The first useful design is rule-based and conservative:
only reorder a contiguous chain of inner equality joins with no intervening hooks, assertions, dedupe, set operations,
or projections that read right-side fields. If a relation has a join hint, the hint must follow that relation after
reordering or the compiler must reject the reordering.

### Nearest As-Of Joins

Forward as-of joins are already implemented. Nearest as-of joins need a tie policy because two candidate rows can be
equally close. The design must define direction, tolerance, exact-match behavior, null time behavior, deterministic
tie-breaking, and generated PySpark lowering.

Nearest as-of should remain a selected-row join, not a general non-equi join. Generated PySpark should make candidate
distance and selection rank visible. The API should reject ties by default unless the caller chooses a deterministic
tie direction.

### Aggregate Aliases

Structure output schemas already own field names. Raw PySpark aggregate aliases would create a second naming system.
The design should either reject aggregate aliases as unsupported because schema constructors are the public alias
mechanism, or admit a narrow helper that maps directly to schema field aliases without exposing `GroupedData.agg`
dictionaries.

The preferred outcome is closure as unsupported or not applicable. Structure already names aggregate outputs by
assigning aggregate expressions to fields in the output schema constructor, and Spark column aliases are already
handled by schema field `alias=...`. A second aggregate aliasing mechanism would create ambiguity in generated code,
traceability, and diagnostics.

### Sampling

Sampling is physical-plan-sensitive. A public `sample(...)` helper needs replacement policy, fraction validation, seed
semantics, determinism expectations, streaming eligibility, and generated-code review shape. The default should be
batch-only and seed-required for repeatable tests.

Sampling should be a relation operation, not an expression. It changes row cardinality but not schema. It should be
batch-only for streaming compatibility because sampling an unbounded stream is operational policy and Spark's
micro-batch behavior is not a stable transformation contract.

### Missing-Column Set Composition

Exact-schema set operations are implemented. Missing-column union requires schema evolution rules: how missing fields
are filled, whether nullable-only missing fields are allowed, how nested structs behave, and how aliases are preserved.
This is a design gate, not a raw `allowMissingColumns=True` passthrough.

The output schema owns the result. Missing columns may be filled only when the output schema can explain the value:
nullable fields may receive typed nulls, and non-nullable fields need explicit defaults. Nested struct filling must
construct complete structs rather than relying on Spark's implicit recursive behavior.

## Streaming Gate Families

Streaming design gates are specified in [V9StreamingDesignGates.md](V9StreamingDesignGates.md). They cover chained
event-time windows, chained stateful operators, selected-row helpers, analytic windows, side-effect APIs, and
arbitrary state APIs.

## Catalog Status Rule

APICatalog should not leave an open row as generic `planned` or `deferred`. Use:

- `implemented` or `supported` when Structure owns the public contract;
- `unsupported` when the row is intentionally outside Structure;
- `design-gated` when this document or a linked specification defines the admission contract;
- `streaming-ineligible` for streaming shapes that require batch materialization.

Mixed rows are acceptable only when the table entry names the supported and gated portions precisely.
