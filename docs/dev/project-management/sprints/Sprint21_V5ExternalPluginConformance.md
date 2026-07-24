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

`docs/dev/planning/P07162601.V5-plugin-architecture.plan.md`

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
- [x] (2026-07-23) Added an independently built `structure-iterable-fixture` wheel. The required specification test
  builds it into a temporary wheelhouse, installs it only into a temporary site directory, discovers its real entry
  point, verifies that its source imports no `structure.core` package, executes a one-shot iterable with repeatable
  collection, and round-trips its opaque JSON payload.
- [ ] Implement iterable target DSL semantics for projection, joins, and grouped aggregation.
