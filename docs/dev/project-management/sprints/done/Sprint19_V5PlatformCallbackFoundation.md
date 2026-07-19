# Sprint 19: V5 Platform API Foundation

## Sprint Goal

Establish the public Platform API contract and Core infrastructure needed to select, negotiate, and invoke an installed
platform without moving PySpark behavior yet.

## Product Outcome

Structure can discover installed platform metadata, resolve one target for each transform, negotiate the highest
mutually supported Platform API version, and assemble a Core-owned artifact through injected test Platform API objects.

## Scope

### In Scope

- One platform plugin and a versioned `PlatformAPI` façade with focused service facets.
- Metadata-only package entry-point discovery and distribution disabling.
- Duplicate platform-name, incomplete-plugin, load-failure, and incompatible-version diagnostics.
- Versioned platform configuration, target resolution for configuration, programmatic overrides, `@transform`,
  projects, and pipelines.
- Core artifact envelopes, plugin identity, negotiated version, cache-key changes, and opaque payloads.
- Core schema, compiler, capability, execution, generation, and serialization service-facet boundaries.
- Default-denied class injection, global `plugin_options = "allow_injection"` opt-in, private engine-manifest
  validation, engine-suite revisioning, and output-boundary checks.

### Out of Scope

- Moving production PySpark behavior behind the Platform API.
- Removing existing root exports.
- Public support for a second runtime platform.

## ExecPlan

`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`

## Acceptance Criteria

- Discovery does not import plugin modules until a target or capability query selects them.
- Both API downgrade directions and non-overlapping ranges are covered by tests.
- Platform configuration has deterministic table-merge, distribution-disablement, and target-resolution behavior.
- A transform resolves exactly one target and a cross-target pipeline fails before Platform API service facets run.
- Core workflows run against a fake v1 platform façade without knowing the opaque payload type.
- A replacement engine is never resolved or constructed by default. The global opt-in enables it only for its selected
  target; incompatible engine manifests fail activation without stock-engine fallback.
- `make build` passes.

## Progress

- [x] (2026-07-17) Renamed the implementation package from `structure.app` to `structure.core` across source,
  tests, package metadata, and public API snapshots.
- [x] (2026-07-17) Split each v1 Platform API class into its own source unit while retaining the package re-exports.
- [x] (2026-07-17) Hardened metadata-only platform discovery with normalized distribution disablement, identity checks,
  and actionable load and negotiation diagnostics.
- [x] (2026-07-17) Added immutable platform configuration merging and target resolution for decorators, overrides,
  defaults, and cross-target pipelines.
- [x] (2026-07-18) Completed the fake-platform Core workflows for compile, execution, generation, serialization, and
  default-denied engine replacement; added symmetric API-downgrade evidence.
