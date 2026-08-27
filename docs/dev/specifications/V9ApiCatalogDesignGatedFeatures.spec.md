# V9 API Catalog Design-Gated Features

## Purpose

This specification makes APICatalog open rows implementation-ready. It includes both PySpark Structured Streaming
design gates and non-streaming planned/deferred rows such as `sample`, aggregate aliases, variant and geospatial
functions, join reordering, nearest as-of joins, and missing-column set composition. XML is recorded as deprioritized
design-gated work and is not part of the active v9 implementation path unless another approved slice needs it.

The companion gate registers are [API Catalog Gates](../gated/ApiCatalog.gates.md) and
[Streaming Gates](../gated/Streaming.gates.md).

The executable Variant completion sequence is [P07302602.V9-variant-type-and-helpers.plan.md](../planning/P07302602.V9-variant-type-and-helpers.plan.md).

## XML Helpers

XML helpers are low priority for this v9 follow-up.

Candidate public helpers:

- `from_xml(value, schema=SomeSchema, options=None)`
- `to_xml(value, options=None)`

Future acceptance requires a public XML capability check for every supported PySpark profile. `from_xml` must return a typed
`Struct[SomeSchema]`. Malformed XML behavior must be explicit: null result, diagnostic, or caller-selected permissive
mode. XML source reading remains caller-owned storage, not a transform helper.

## Variant And Geospatial Helpers

Variant helper support requires a public `variant(...)` field type before helper APIs. Geospatial support requires a public
geometry type and a documented coordinate reference policy; the v9 geometry type and policy are now specified below.

V9 outcome:

- Variant field support is implemented for the bundled PySpark plugin.
- Apache Sedona 1.9.0 is the selected optional integration provider. V9 admits a narrow provider-neutral typed
  `GEOMETRY` slice; the bundled PySpark plugin remains free of geospatial dependencies and provider-specific exports.
- `variant(nullable=True)` declares an opaque `VariantType`, renders as `T.VariantType()`, and materializes to
  PySpark's `VariantType` only for a transform whose resolved profile is a supported PySpark 4 profile. The resolved
  plugin profile may come from the global `[tool.structure.plugin.pyspark]` configuration or a compile/session
  override.
- `parse_json(...)`, `try_parse_json(...)`, `variant_get(...)`, `try_variant_get(...)`, `to_variant_object(...)`, and
  `is_variant_null(...)` are public only for supported PySpark 4 profiles. `schema_of_variant(...)` and
  `schema_of_variant_agg(...)` inspect Variant shape as SQL-format strings. `is_valid_variant(...)` requires the
  `>=4.2,<4.3` profile. Extraction paths are literal non-empty strings starting with `$`; extraction requires an
  explicit Structure `as_type`. `to_variant_object(...)` accepts Array, Map, and Struct values, and every Map key type
  must be String.
- Variant mutation helpers are recorded as a future-profile design gate:
  `variant_array_append(...)`, `try_variant_array_append(...)`, `variant_insert(...)`, `try_variant_insert(...)`,
  `variant_set(...)`, `try_variant_set(...)`, and `variant_delete(...)` are not admitted until a released PySpark 4.3+
  profile is added to Structure's compatibility matrix. The eventual Structure surface should accept literal
  `$`-prefixed paths even though Spark also supports runtime path columns.
- Geometry fields and helpers are exported through the provider-neutral PySpark DSL; the bundled plugin exports no
  provider-specific type, import, or implementation.

Remaining current-scope Variant work is PySpark 4.2 evidence closure for the admitted literal, equality, and
row-expansion contracts. Mutation helpers remain explicitly design-gated until their Spark profiles are released;
generated PySpark remains target-gated because Variant availability differs by Spark profile.

V9 resolves these remaining gates through the linked execution plan. Literal construction uses validated JSON text and
public `parse_json` lowering; equality uses the existing typed equality operators with Variant compatibility; mutation
helpers are admitted only on their owning Spark profiles; and row expansion is admitted only through a typed
table-valued/generator operation. Dynamic paths, implicit extraction result types, ordering, and private Python Variant
objects remain explicit exclusions unless a later design changes this contract.

### Provider-neutral Geometry Slice

The v9 optional-provider slice is Apache Sedona 1.9.0 `GEOMETRY`; `GEOGRAPHY` is not admitted. It belongs to a
Sedona-specific optional plugin, never the bundled PySpark plugin.

Public candidate API:

- `geometry(srid=..., nullable=True)` declares a planar geometry field. `srid` is a required positive integer literal.
- `geometry_from_wkt(value, srid=...)` returns `Geometry[srid]` from a String value.
- `geometry_as_wkt(value)` returns a String value.
- `intersects(left, right)`, `contains(left, right)`, and `within(left, right)` return nullable Boolean values.

These are provider-neutral PySpark DSL exports. They lower only through the common Spark SQL `ST_*` contract and must
not expose a provider import, provider configuration, or raw SQL escape hatch to transform authors.

Coordinate-reference rules:

- the declared SRID is part of a geometry type; Geometry values with unequal SRIDs cannot be combined by the admitted
  predicates;
- construction requires a literal SRID and records it in the symbolic type; rendering uses the common Spark SQL
  `ST_GeomFromWKT` contract and leaves provider-specific representation to the active runtime;
- WKT serialization preserves geometry text only. It does not promise to encode, infer, validate, transform, or
  compare coordinate reference systems outside the declared literal SRID;
- null geometries propagate null through construction, serialization, and predicates according to the active
  provider's common Spark SQL semantics.

Capability and target rules:

- the PySpark plugin owns a provider-neutral `GeoProvider` extension contract: geometry schema materialization,
  operation lowering, generated imports, and runtime availability validation are provider responsibilities;
- generated PySpark uses the common Spark SQL `GEOMETRY`/`ST_*` contract and must not contain a provider name or import;
- no provider is selected or configured by an end user. Geometry transforms compile normally and validate the active
  Spark runtime only when schemas are materialized or a transform executes. The diagnostic names the missing common
  Geometry SQL contract, not a particular provider;
- the bundled PySpark plugin must not import or require Sedona. Compose integration uses a Sedona adapter pinned to
  `apache-sedona==1.9.0`, the matching Spark/Scala-specific Sedona shaded JAR, and the GeoTools wrapper. PySpark 3.5
  and 4.0 lanes must honor Sedona's documented Java requirements.

Out of scope:

- `GEOGRAPHY`, unknown or runtime-selected SRIDs, CRS transformation, distance/area/length measurement, spatial
  joins and indexes, geometry collections, non-WKT serialization, raster support, and all raw `ST_*` wrappers.

Tests for the first implementation must cover symbolic field and SRID compatibility, unavailable-provider diagnostics,
provider-neutral generated calls, and live Sedona Compose parity for the supported 3.5 and 4.0 lanes.

## Join Reordering

V9 outcome:

- no public `join_order(...)` helper is exported;
- logical join reordering remains design-gated;
- Spark's own physical optimizer may still reorder internally, but Structure source and traceability order remain
  source-authored order.

Future candidate API:

- `join_order("source")`, the current default;
- `join_order("optimizer")`, an opt-in future mode;
- optional per-join hints that already exist remain lower-level directives, not cost-based reordering.

The first admitted reordering slice may reorder only inner joins whose predicates are pure equality predicates and
whose involved relations have no source-order-dependent hooks, validations, previous cardinality assertions, or
predicates that read a previously joined right-side scope. Explain output must show the original order and chosen order.
Any uncertainty falls back to source order.

Source placement:

- `join_order(...)` would be a statement in a step method before the affected join chain.
- `join_order("source")` is the default and produces no operation.
- `join_order("optimizer")` would record a relation-planning directive in IR.

Accepted first slice:

- contiguous chain of `inner_join(...)` or `rowset_join(..., how="inner")`;
- equality-only predicates joined by `&`;
- no `@raw` hook, `exactly_one(...)`, `require_unique(...)`, dedupe, set operation, ordering, limit, offset, or
  projection between the reordered joins;
- no outer, right, full, cross, semi, anti, temporal, as-of, or streaming joins;
- no relation read after its original join position that would change visible scope.
- compiler dataflow must prove each join predicate reads only the base left scope and that join's right scope before the
  join can move.

Diagnostics:

- if a future `join_order("optimizer")` helper is added, unsupported reordering uses `REL-E0707` or a new
  relation-planning diagnostic if `REL-E0707` is already assigned;
- the message must name the first operation that prevents reordering and say to keep `join_order("source")`.

Tests:

- no-Spark symbolic tests prove the directive is captured;
- rendering tests prove generated join order changes only for accepted chains;
- traceability tests prove original order and selected order are both visible;
- negative tests cover hooks, outer joins, assertions, hints that cannot follow a relation, and streaming joins.

## Nearest As-Of Joins

Implemented in v9 for the existing public API:

- `as_of_one(on=..., left_time=..., right_time=..., direction="nearest", tolerance=None, ties="error")`

Accepted rules:

- `ties` is `error`; directional tie preferences remain design-gated;
- null `left_time` or `right_time` values never match;
- generated PySpark must make the distance calculation and tie-breaker visible;
- streaming support is not included in the first slice.

Generated recipe:

- join candidate rows using the existing equality predicate plus a tolerance predicate when present;
- compute absolute distance between `left_time` and `right_time`;
- compute the minimum distance and count candidates at that minimum;
- fail when more than one best-distance candidate exists;
- use `row_number()` over the left-row identity, ordered by distance and right time;
- keep rank `1`;
- drop helper distance, minimum-distance, tie-count, rank, and row-identity columns.

Rejected rules:

- `ties` values other than `error`;
- streaming input in the first slice.

Tests cover symbolic capture, generated distance calculation, generated tie failure, generated deterministic ordering,
and traceability. Live Spark evidence remains a later acceptance item before claiming target-profile-specific timestamp
interval behavior.

## Aggregate Aliases

Aggregate aliases are closed as unsupported/not-applicable. Structure schema constructors already name aggregate
outputs:

```text
return ProductSummary(order_count=count())
```

Closure decision:

- raw aggregate aliases are unsupported/not-applicable in the public catalog;
- schema constructor assignment is the only aggregate output name mechanism;
- Spark physical column names are handled through output schema field aliases;
- guard tests verify that no public aggregate helper accepts `alias=`.

If future user evidence demands alias syntax, the only acceptable form is output-schema-local and must lower to the
same generated code as assigning to the field directly.

## Sampling

Implemented in v9 as a relation-level batch operation.

Candidate API:

- `sample(fraction, with_replacement=False, seed=None, reproducible=True)`

Accepted rules:

- `fraction` is a literal float in `[0, 1]` without replacement and non-negative with replacement;
- `seed` is required unless the caller explicitly opts into non-repeatable sampling;
- the operation is batch-only in streaming compatibility;
- generated PySpark renders `df.sample(withReplacement=..., fraction=..., seed=...)`;
- explain output states that row count and deterministic ordering are not guaranteed.

Source placement:

- `sample(...)` is a statement operation in a step method.
- It applies to the active relation.
- It may appear before or after joins, filters, and projections, but not inside an aggregate metric or expression.

Diagnostics:

- non-literal `fraction` is rejected;
- invalid fraction ranges are rejected;
- missing seed is rejected unless the spelling is `sample(..., reproducible=False)`;
- streaming compatibility classifies sampling as `batch_only` with a fix pointing to caller-owned streaming sampling or
  batch materialization.

Tests:

- symbolic operation ordering;
- generated `DataFrame.sample(...)` rendering;
- online/generated parity with a fixed seed;
- invalid fraction and missing seed diagnostics;
- streaming rejection through `STREAM-E0801`.

## Missing-Column Set Composition

Implemented first slice:

- `union_by_name(relation, allow_missing_columns=True)`

Accepted rules:

- only nullable top-level missing fields are filled with null;
- non-null missing fields require the future defaults design and are rejected;
- nested missing structs require a future exact nested schema fill rule;
- aliases must resolve to physical Spark column names consistently;
- streaming missing-column support is rejected until live restart evidence exists.

Candidate spelling:

- keep existing `union_by_name(relation)` exact-schema behavior unchanged;
- `union_by_name(relation, allow_missing_columns=True)` records a batch relation operation that renders Spark
  `unionByName(..., allowMissingColumns=True)`;
- `defaults=...` is reserved and rejected until explicit fill expressions are designed.

Fill rules:

- if a field exists on both sides, its type and nullability must be compatible with the output schema;
- if a nullable top-level field is missing on one side, Spark fills it with null;
- if a non-nullable field is missing, reject the transform;
- nested struct defaults must construct the complete nested schema in a future design;
- arrays and maps cannot be partially filled in this slice;
- physical aliases come from the output schema, not from the side where a field happened to exist.

Tests:

- nullable top-level fill;
- non-null missing field rejection;
- explicit default for non-null field;
- nested struct fill rejection until complete struct default exists;
- generated source uses `unionByName(..., allowMissingColumns=True)` for the admitted nullable top-level fill;
- streaming reports `STREAM-E0801` for `allow_missing_columns=True`;
- defaults remain design-gated.

The bullets above record the V9 first-slice boundary. The current V10 batch contract supersedes the defaults and nested
struct restrictions: typed `defaults={"field.path": literal}` values now support missing non-nullable fields and nested
struct fields, including alias-preserving rendering. Array/map element evolution and streaming missing-column union
remain gated. See the V10 continuation below.

## Streaming Design Gates

Streaming-specific contracts live in [V9StreamingDesignGatedFeatures.spec.md](V9StreamingDesignGatedFeatures.spec.md). This
specification includes them by reference so the v9 APICatalog follow-up plan can resolve all open catalog rows from one
place.

## Acceptance

The v9 API catalog design-gate plan is accepted when:

- every APICatalog row formerly marked `planned` or `deferred` is either implemented, unsupported, design-gated with a
  design/spec link, or split into precise supported and design-gated rows; XML may remain low-priority design-gated;
- tests guard the catalog against generic `planned` and `deferred` statuses;
- every implemented row has source syntax, IR/lowering, diagnostics, docs, and tests;
- every rejected row has a public rationale and a diagnostic or catalog reference;
- `make build` passes.

## V10 Continuation

V10 carries forward only the actionable core API slices from `docs/dev/deferred/ApiCatalog.deferred.md`: provider-neutral Geometry,
sampling refinements, and typed missing-column schema evolution. The grouped V10 API plan also records explicit
dispositions for XML, unreleased Variant mutation profiles, and opt-in join reordering. Application-specific future
documents are not part of the V10 implementation boundary.

The V10 contract requires canonical field paths for `union_by_name(..., defaults=...)`, typed literals, nullable or
explicitly defaulted nested structs, preserved aliases, and rejection of implicit array/map element evolution. Streaming
schema evolution is not promoted until the exact behavior has PySpark 3.5/4.0 evidence.
