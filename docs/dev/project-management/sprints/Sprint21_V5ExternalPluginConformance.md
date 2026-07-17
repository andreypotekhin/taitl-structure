# Sprint 21: V5 External Plugin Conformance

## Sprint Goal

Prove that an independently packaged platform can use only the public Platform API and publish the contract external
plugin authors need.

## Product Outcome

An external vendor can build, package, discover, configure, test, and diagnose a Structure platform without importing
Core implementation modules.

## Scope

### In Scope

- Public platform author guide, API reference, compatibility policy, and reusable conformance kit.
- A separately built internal `iterable` wheel registered through real package entry points.
- Finite iterable projection, inner/left joins, grouped sum/count, re-iterable results, and `collect()`.
- Opaque-plan serialization service facets and Core-owned envelope round trips.
- Installed-plugin eligibility, distribution disabling, duplicate-platform-name diagnostics, and vendor-owned platform
  DSL imports.
- Private engine-manifest compatibility evidence, without publicizing the private engine extension.

### Out of Scope

- Public end-user documentation or production support for the iterable platform.
- Infinite streaming, generation service facets, or broad analytical coverage for iterable data.
- Automatic compatibility between PySpark and iterable transform source.

## ExecPlan

`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`

## Acceptance Criteria

- Tests build and install the fixture wheel in isolation and discover it through distribution metadata.
- The fixture imports only public Core and Platform API packages.
- API negotiation, execution, serialization, disablement, and conflict behavior pass through real entry points.
- The fixture proves that a rejected private engine manifest fails its selected target rather than falling back.
- The conformance kit produces actionable failures for incomplete or inconsistent plugins.
- `make build` passes.

## Progress

- [ ] Start after Sprint 20 closes.
