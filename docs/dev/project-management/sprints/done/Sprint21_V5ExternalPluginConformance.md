# Sprint 21: V5 External Plugin Conformance

## Sprint Goal

Prove that an independently packaged plugin can use only the public Plugin API and publish the contract external
plugin authors need.

## Product Outcome

An external vendor can build, package, discover, configure, test, and diagnose a Structure plugin without importing
Core implementation modules.

## Scope

### In Scope

- Public plugin author guide, API reference, compatibility policy, and reusable conformance kit.
- A separately built internal `iterable` wheel registered through real package entry points.
- Finite iterable projection, inner/left joins, grouped sum/count, re-iterable results, and `collect()`.
- Opaque-plan serialization service facets and Core-owned envelope round trips.
- Installed-plugin eligibility, distribution disabling, duplicate-plugin-name diagnostics, and vendor-owned plugin
  DSL imports.
- Default-denied class-injection and private engine-manifest compatibility evidence, without publicizing the private
  engine extension.

### Out of Scope

- Public end-user documentation or production support for the iterable plugin.
- Infinite streaming, generation service facets, or broad analytical coverage for iterable data.
- Automatic compatibility between PySpark and iterable transform source.

## ExecPlan

`docs/dev/planning/done/P07162601.V5-plugin-architecture.plan.md`

## Acceptance Criteria

- Tests build and install the fixture wheel in isolation and discover it through distribution metadata.
- The fixture imports only public Core and Plugin API packages.
- API negotiation, execution, serialization, disablement, and conflict behavior pass through real entry points.
- The fixture proves that injection is blocked without the global opt-in and that a rejected private engine manifest
  fails its selected target rather than falling back.
- The conformance kit produces actionable failures for incomplete or inconsistent plugins.
- `make build` passes.

## Progress

- [x] (2026-07-23) Started after Sprint 20 closes.
- [x] (2026-07-23) Added the public `PluginConformance` helper and vendor author guide. The helper centralizes
  descriptor identity, symmetric API negotiation, and required-facet validation for both Core and external packages;
  it now rejects a missing authoring facet before a workflow starts.
- [x] (2026-07-23) Added an independently built `structure-iterable-example` wheel. The required specification test
  builds it into a temporary wheelhouse, installs it only into a temporary site directory, discovers its real entry
  point, verifies that its source imports no `structure.core` package, executes a one-shot iterable with repeatable
  collection, and round-trips its opaque JSON payload.
- [x] (2026-07-23) Added finite iterable execution semantics: deterministic projection, inner and left equi-joins,
  and insertion-ordered grouped `sum` and `count`. The installed-wheel test covers all operations alongside
  one-shot-input materialization and repeatable results.
- [x] (2026-07-23) Restructured the fixture as a starter plugin mirroring the bundled plugin's focused application
  layout. Its vendor-owned DSL plans now lower authored `@transform(target="iterable")` classes through the
  public compiler facet; the isolated-wheel test proves projection, join, and aggregation compilation and execution.
- [x] (2026-07-23) Routed `StructureSession.run(...)` to a transform-selected external plugin when its declared
  target differs from the session's default target. The session passes finite runtime values to the plugin, wraps a
  single raw target result in Core's `TransformResult`, and leaves existing PySpark session and pipeline behavior
  unchanged. The iterable example is now named `structure-iterable-example` and imports as `structure_iterable`.
- [x] (2026-07-23) Added `ProjectIterableScores` to the school example. After installing the external example plugin,
  it runs beside the school package's PySpark transforms through `StructureSession(runtime=...)`; the isolated-wheel
  test proves the mixed-project use without permitting cross-plugin composition.
- [x] (2026-07-23) Replaced the Iterable plugin's Fibonacci special case with a small declarative `recurrence` plan.
  School's `Fibonacci` class owns its state transition as a normal plan attribute, proving that additional finite
  sequences need no plugin change and that only Structure's `@transform` decorates a transform class.
- [x] (2026-07-23) Made Iterable inputs explicit transform constructor arguments. The school examples now declare
  `Student` and `SequenceRow` input models, bind lists or generators through `students=` and `rows=`, and preserve
  those names through generic plugin execution.
- [x] (2026-07-23) Simplified recurrence authoring: its default compiler lowering infers the sole declared input and
  the sole output field newly introduced by the output schema. School Fibonacci therefore needs neither `input="rows"`
  nor `value="fibonacci"`; either remains available only for ambiguous transforms.
- [x] (2026-07-23) Allowed recurrence `next=` to be a declaration-time state lambda, so school Fibonacci names its
  prior values directly while still lowering to the same opaque state-expression payload.
- [x] (2026-07-23) Extended the isolated installed-wheel test with disabled-distribution normalization, a second
  installed metadata record advertising the same target name, default-denied injection, and an incompatible private
  engine manifest. The selected wheel's descriptor supplies the identity used in each private-engine diagnostic.
