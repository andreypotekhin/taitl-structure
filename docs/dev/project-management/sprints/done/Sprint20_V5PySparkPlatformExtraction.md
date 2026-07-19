# Sprint 20: V5 PySpark Plugin Extraction

## Sprint Goal

Move PySpark target ownership behind the public Platform API while preserving the complete released PySpark contract.

## Product Outcome

PySpark users retain online and generated behavior, but Core no longer imports PySpark plans, runners, renderers,
capability rules, or PySpark platform DSL directly.

## Scope

### In Scope

- `structure.platform.pyspark` platform DSL and field/type definitions.
- PySpark `PlatformAPI` façade with schema, compiler, capability, execution, and generation service facets.
- Generic `StructureSession(runtime=..., context=...)` execution.
- Callback-backed CLI check, compile, explain, schema tooling, traceability, and generated-file workflows.
- PySpark classic and Connect regression, parity, and Spark-free compilation evidence.

### Out of Scope

- Root-export removal before replacement imports and fixtures are ready.
- New PySpark transformation families unrelated to extraction.
- External plugin conformance documentation.

## ExecPlan

`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`

## Acceptance Criteria

- PySpark uses the same public Platform API exposed to external distributions.
- Core artifact and runtime modules contain no concrete PySpark plan or runtime types.
- Existing supported PySpark semantics and generated output remain equivalent.
- Compiler-only commands remain free of PySpark, Java, and Spark startup.
- `make build` and supported PySpark integration lanes pass.

## Progress

- [x] (2026-07-18) Started after Sprint 19 foundation acceptance passed.
- [x] (2026-07-18) Moved the PySpark implementation package from Core to `structure.platform.pyspark` and updated
  repository imports without changing released behavior.
- [x] (2026-07-18) Moved PySpark online and generated execution runners, schema materialization, capability rules,
  and Spark Connect runtime diagnostics behind the PySpark platform package; Core retains only generic artifact,
  schema, and runtime boundaries.
- [x] (2026-07-18) Routed the legacy compiled-artifact builder through the selected platform's compiler façade;
  Core now receives opaque payloads plus its own transform-plan and schema boundaries.
- [x] (2026-07-18) Normalized `structure.core.platforms` into the standard app layout: `api`, `commands`, `logic`,
  and `model`, with a `Platform` endpoint for registry creation.
- [x] (2026-07-18) Routed configured project rendering through the selected platform's generation facet; Core groups
  source units and retains file ownership while PySpark renders its generated source.
- [x] (2026-07-18) Routed Platform registry and command creation through the `Platform` API façade; Core no longer
  imports or instantiates platform commands and logic directly.
- [x] (2026-07-18) Removed the final PySpark-named artifact accessor from the generic configured-project renderer;
  it now passes only opaque platform payloads to the selected generation facet.
- [x] (2026-07-18) Routed `Transform.generate()` rendering through the selected platform generation facet while
  retaining the existing PySpark disk-storage adapter as a compatibility boundary.
- [x] (2026-07-18) Renamed the Core platform-orchestration app to `structure.core.platforms` to distinguish it
  from the platform distribution namespace, `structure.platform`.
- [x] (2026-07-18) Aligned `structure.core.platforms` package exports, API consumption, and README with surrounding
  Core apps: only `api` exposes the app façade; commands and logic remain internal.
- [x] (2026-07-18) Routed `StructureSession` execution through the selected platform executor using a context-rich
  `ExecutionRequest`; Core no longer imports PySpark online or generated runners.
- [x] (2026-07-18) Routed Core capability resolution through the selected platform capability façade; PySpark owns
  concrete profile and variant rules.
- [x] (2026-07-18) Moved generated-file comparison, writing, and result models to Core; the PySpark file API retains
  compatibility aliases while Core CLI owns filesystem lifecycle.
- [x] (2026-07-18) Moved the PySpark recipe dataflow-read mapper out of Core traceability and into the PySpark
  platform logic package.
- [x] (2026-07-18) Moved the PySpark recipe streaming-compatibility classifier into the PySpark platform package;
  the existing Core compiler endpoint remains a thin façade.
- [x] (2026-07-18) Moved the PySpark recipe traceability builder into the PySpark platform package while preserving
  the existing Core traceability façade and generic result models.
- [x] (2026-07-18) Moved the PySpark StructType-to-Structure-source mapper into the PySpark platform package;
  Core retains generic schema-tool request validation and source rendering.
- [x] (2026-07-18) Routed schema inspection and native schema-source mapping through the selected platform schema
  facet; Core schema tooling no longer imports PySpark implementation modules.
- [x] (2026-07-18) Routed CLI explain rendering through the selected platform explain facet; PySpark owns recipe
  rendering while Core retains the CLI façade and platform selection.
- [x] (2026-07-18) Added lazy bundled-PySpark discovery for source checkouts without installed entry-point metadata,
  preserving metadata-first target selection for packaged installations and Docker integration.
- [x] (2026-07-18) Verified `make build`: 1,104 passed, 22 skipped; rigidity suite: 26 passed, 6 skipped.
- [x] (2026-07-18) Verified live integration: PySpark 3.5 (19 passed, 3 skipped), PySpark 4.0 (22 passed), Spark
  Connect 3.5 (17 passed, 5 skipped), and Spark Connect 4.0 (20 passed, 2 skipped).

## Completion

Sprint 20 is complete. Platform API v1 remains the supported API range. PySpark 3.5.0 and 4.0.0, including their
supported Spark Connect lanes, provide the recorded live evidence. Sprint 22 retains removal of compatibility exports
and backend-specific Core façade names.

Follow-up hardening removed the remaining direct `structure.platform.pyspark` imports from Core. Compiler analysis,
runtime schema materialization, legacy runtime execution adapters, and generated-file storage now dispatch through
generic Core boundaries and Platform API facets.

The PySpark plug-in entry point now delegates to `api`, where each Platform API service has its own named
source unit. Selected-platform values use the readable `platform.api` form, and runtime version consumers share the
single build-defined package version.
