# Sprint 09: Advanced Analytics, Spark Connect, Spark Streaming, Optimization, and Explain

## Sprint Goal

Build directly on Sprint 08 analytical operations by designing and implementing the advanced aggregation/window/HOF
surface left out of the first slice, promoting Spark Connect from experimental parity to supported batch status for
completed v1/v2 compiler-visible features, proving the first caller-owned Spark Structured Streaming slice, admitting
the PySpark join forms left out of the first analytical slice, adding explicit optimization directives, and making
explain output rich enough to review analytical pipelines.

## Product Outcome

Developers can write broader compiler-visible analytical transforms, run completed batch transforms online and
generated against Spark Connect, pass caller-owned streaming DataFrames through the first supported streaming-compatible
transform slice, express right/full/cross and broad predicate joins without hooks, request important Spark
physical-plan hints explicitly, and inspect analytical dataflow without reading generated code line by line.

## Scope

### In Scope

- Spark Connect support promotion for completed batch features.
- Live online and generated parity checks against a Spark Connect session.
- Generated-source scans that reject classic-only internals.
- Spark Connect setup diagnostics through CI or a documented manual verification script.
- Hook and StructureTools Spark Connect compatibility boundaries.
- Spark Structured Streaming first-slice support for caller-owned streaming DataFrames.
- Online and generated streaming runtime evidence for row-local projection, row-local filtering, schema-only
  validation, and stream-static left/inner lookup joins.
- Streaming compatibility diagnostics, source scans, explain output, and public references for supported and deferred
  streaming features.
- Full aggregation/window/HOF design and implementation beyond the Sprint 08 first slice.
- Advanced grouping through rollup, cube, grouping sets, and subtotal metadata.
- Additional aggregate metrics, filtered metrics, and post-aggregate `having(...)`.
- Reusable window specs, explicit row/range frames, distribution/value windows, and window aggregate helpers.
- Additional symbolic array and map higher-order helpers.
- Full PySpark rowset join support: `join_rowset(...)` for right, full, cross, non-equi, and disjunctive joins.
- Cache and persistence directives at subtransform boundaries.
- Repartition and coalesce directives.
- Checkpoint hints where the configured backend supports them.
- Join strategy directives for broadcast, shuffle hash, sort merge, and lookup projection where supported.
- Rich `structure explain` mode for field-level lineage.

### Out of Scope

- Automatic cost-based optimization.
- Automatic join reordering.
- Storage write orchestration.
- Spark Connect streaming orchestration.
- Spark Connect storage write orchestration.
- Certifying arbitrary hook body internals as Connect-compatible.
- Streaming source and sink generation.
- Stream-stream joins and streaming support for right, full, cross, non-equi, or disjunctive rowset joins.
- Lateral joins and table-valued-function joins.
- Generated documentation artifacts.
- Public pytest helper package.
- Production incremental compile and cache diagnostics.
- Streaming aggregation/window orchestration, watermarks, output modes, and state policy.

## Relevant Specification Items

- As a developer, I can add caching and persistence hints at step boundaries.
- As a developer, I can define advanced grouping patterns so that rollups, cubes, grouping sets, and multi-level
  summaries are supported when practical.
- As a developer, I can calculate Boolean, statistical, approximate, and collection aggregate metrics.
- As a developer, I can filter individual aggregate metrics and aggregate output rows.
- As a developer, I can reuse named window specifications with explicit row and range frames.
- As a developer, I can define distribution, value, and aggregate window expressions.
- As a developer, I can use additional symbolic array and map higher-order helpers.
- As a developer, I can add repartition and coalesce hints.
- As a developer, I can add checkpoint hints where supported.
- As a developer, I can specify join strategies and hints.
- As a developer, I can express right, full, and cross rowset joins without hiding them in hooks.
- As a developer, I can express non-equi and disjunctive join predicates when all expression nodes are compileable.
- As a developer, I can generate richer static dataflow explain output.
- As a developer, I can explain generated-code sections.
- As a developer, I can run supported completed batch transforms online with Spark Connect.
- As a developer, I can run supported generated completed batch transforms with Spark Connect.
- As a developer, I receive diagnostics before Spark Connect runs classic-only internals.
- As a maintainer, I can verify Spark Connect support through CI or a documented manual script.
- As a developer, I can pass a caller-owned streaming DataFrame through compatible online and generated transforms.
- As a developer, I can see clear streaming diagnostics when a transform needs state, lifecycle ownership, or a
  stream-stream join outside the first slice.

## Engineering Tasks

1. Add Spark Connect design, specification, public reference, and execution plan for full batch support.
2. Add live Spark Connect online parity checks for completed compiler-visible batch features.
3. Add live Spark Connect generated-code parity checks for completed compiler-visible batch features.
4. Add generated-source and runtime guardrails that reject SparkContext, RDD, JVM/Py4J, `_jdf`, and private classic
   PySpark fields for `target_variant = "spark-connect"`.
5. Add Spark Connect setup diagnostics through CI or a documented manual verification script.
6. Document and enforce hook and StructureTools compatibility boundaries for Spark Connect.
7. Add Spark streaming first-slice design, specification, public reference, deferred-feature reference, and execution
   plan.
8. Add online and generated Spark streaming parity evidence for caller-owned streaming DataFrames and static lookup
   inputs.
9. Add streaming generated-source scans, compatibility diagnostics, and explain output for supported and deferred
   operation families.
10. Implement the advanced analytical operation plan
   [P07052601.Advanced-analytical-operations.plan.md](../../planning/P07052601.Advanced-analytical-operations.plan.md).
11. Implement advanced grouping, aggregate metrics, filtered metrics, and `having(...)`.
12. Implement reusable window specs, explicit frames, broad window expressions, and backend diagnostics.
13. Implement additional symbolic array and map higher-order helpers.
14. Implement `join_rowset(...)` source capture, joined right relation scopes, IR, backend capabilities, diagnostics, and public
   docs.
15. Render right, full, cross, non-equi, and disjunctive joins in online and generated PySpark through shared recipes.
16. Add parity, capability, streaming-classification, and explain tests for full PySpark join support.
17. Implement optimization directive source capture and IR.
18. Add backend capability checks and diagnostics for each directive.
19. Render directives in online and generated PySpark through shared recipes.
20. Add tests proving directives do not change row or schema semantics.
21. Add rich explain output for field-level lineage through projections, filters, rowset joins, aggregations,
   selected-row helpers, exact/subset dedupe, higher-order expressions, hooks, and optimization boundaries.
22. Add generated-code section labels where they help explain reports point to emitted code.

## Acceptance Criteria

- Advanced aggregation, window, and HOF helpers compile through IR and shared PySpark recipes without hidden UDFs.
- Unsupported advanced analytical helpers fail with backend capability diagnostics before runtime.
- Full PySpark rowset joins are visible in source, IR, generated code, traceability, explain output, and parity tests.
- Cross joins require explicit Cartesian acknowledgement.
- Right and full joins enforce nullable-side output construction rules.
- Optimization directives are visible in source, IR, generated code, traceability, and explain output.
- Unsupported directives fail with backend capability diagnostics before runtime.
- Rich explain output can follow field lineage through projections, filters, joins, aggregations, windows, hooks, and
  optimization boundaries.
- Spark Connect is supported for completed batch features without changing public DSL or generated class APIs.
- Online and generated Spark Connect parity runs against a real Spark Connect session or an explicitly documented
  manual verification script blocks release until it passes.
- Classic-only internals fail through backend capability diagnostics before Spark Connect execution or generation.
- Public and development docs link to the Spark Connect support boundary.
- Spark streaming first-slice support returns streaming DataFrame plans from online and generated transforms when the
  caller supplies one streaming current input and static lookup side inputs.
- Streaming-compatible generated transform bodies contain no `readStream`, `writeStream`, query lifecycle calls, Spark
  actions, RDD conversion, Pandas conversion, or UDF fallback.
- Public and development docs link to the Spark streaming first-slice boundary and deferred streaming feature boundary.

## Outstanding Challenges

- Spark Connect support must stay batch-scoped. Streaming orchestration and storage writes are separate roadmap work.
- Spark streaming support in Sprint 09 must stay caller-owned. Generated sources, generated sinks, query lifecycle,
  watermarks, output modes, and state policy remain deferred.
- Explain output must expose lineage through analytical operations without becoming noisy by default.
- Optimization directives are user intent and backend requests, not physical-plan guarantees.
- Diagnostics and explain links should point end users to public `docs/reference` pages while keeping implementation
  details in `docs/dev/specifications`.
- Streaming compatibility remains conservative for selected-row windows, analytical windows, aggregation, and dedupe
  until watermark, state, and output-mode contracts are designed.
- Advanced HOF helpers must stay symbolic even when user callback syntax looks like ordinary Python.
- Transform composition and generated documentation/tooling remain adjacent v2 work; Sprint 09 must preserve the
  metadata Sprint 10 needs rather than hiding it inside target-specific renderers.

## Progress

- [x] (2026-07-05) Added experimental Spark Connect parity for completed compiler-visible batch features with
  variant-specific backend capability checks, classic-only internal exclusions, identical generated-code shape checks,
  traceability-shape checks, and public compatibility docs.
- [x] (2026-07-05) Added design, implementation specification, public reference, and ExecPlan for full PySpark join
  support scheduled into Sprint 09.
- [x] (2026-07-05) Added the advanced analytical operations design/spec/reference package and execution plan for the
  aggregation/window/HOF features left out of the Sprint 08 first slice.
- [x] (2026-07-05) Added design, specification, public reference, and execution plan for Spark Connect batch support
  promotion.
- [x] (2026-07-05) Added design, specification, public reference, deferred-feature reference, and execution plan for
  first-slice Spark streaming support scheduled into Sprint 09.
- [ ] Add live Spark Connect online/generated runtime evidence.
- [ ] Add Spark Connect CI or manual verification script.
- [ ] Add hook and StructureTools Spark Connect boundary diagnostics.
- [ ] Implement Spark streaming first-slice support from
  [P07052604.Spark-streaming-first-slice.plan.md](../../planning/P07052604.Spark-streaming-first-slice.plan.md).
- [ ] Add live Spark streaming online/generated runtime evidence or a release-blocking manual verification script.
- [ ] Implement advanced aggregation/window/HOF support from
  [P07052601.Advanced-analytical-operations.plan.md](../../planning/P07052601.Advanced-analytical-operations.plan.md).
- [ ] Implement full PySpark rowset join support.
- [ ] Implement explicit optimization directives.
- [ ] Implement rich explain output for completed batch operations.

## Explain Performance Metric

Track explain rendering time for analytical fixture transforms.

Targets:

- A 10-transform analytical fixture explain report renders in under 2 seconds excluding interpreter startup.
- Explain output remains compact by default and expands only when requested.

## Risks

- Optimization directives can imply guarantees Spark does not make.
- Full/right joins weaken the current-row assumption; diagnostics must force explicit projection where no left-row base
  is guaranteed.
- Cross joins can explode row counts; the DSL must require explicit acknowledgement.
- Rich explain output can become noisy if compact summaries are not the default.
- Spark Connect support can become misleading if runtime evidence is shape-only instead of live Connect execution.

## Notes

Keep optimization directives honest: they are explicit user intent and backend requests, not promises that Spark will
always choose a particular physical plan. Keep Spark Connect support scoped: this sprint supports completed batch
features, not streaming orchestration, storage writes, or arbitrary hook internals. Keep Spark streaming support
caller-owned: Structure returns compatible DataFrame plans but does not own streaming jobs.
