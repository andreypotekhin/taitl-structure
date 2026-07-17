# Sprint 19: V5 Platform Callback Foundation

## Sprint Goal

Establish the public callback contract and Core infrastructure needed to select, negotiate, and invoke an installed
platform without moving PySpark behavior yet.

## Product Outcome

Structure can discover installed platform metadata, resolve one target for each transform, negotiate the highest
mutually supported Platform API version, and assemble a Core-owned artifact through injected test callbacks.

## Scope

### In Scope

- One platform bootstrap provider and a versioned `PlatformAPI1` façade with focused service facets.
- Metadata-only package entry-point discovery and distribution disabling.
- Duplicate short-id, incomplete-provider, load-failure, and incompatible-version diagnostics.
- Target resolution for configuration, programmatic overrides, `@transform`, projects, and pipelines.
- Core artifact envelopes, provider identity, negotiated version, cache-key changes, and opaque payloads.
- Core schema, compiler, capability, execution, generation, and serialization service-facet boundaries.
- Private target-local engine-replacement manifest validation, engine-suite revisioning, and output-boundary checks.
- Optional synchronous `PlatformAPI1.send(message)` convention and diagnostics.

### Out of Scope

- Moving production PySpark behavior behind callbacks.
- Removing existing root exports.
- Public support for a second runtime platform.

## ExecPlan

`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`

## Acceptance Criteria

- Discovery does not import provider modules until a target or capability query selects them.
- Both API downgrade directions and non-overlapping ranges are covered by tests.
- A transform resolves exactly one target and a cross-target pipeline fails before callbacks run.
- Core workflows run against a fake v1 platform façade without knowing the opaque payload type.
- A compatible replacement engine is constructed only for its selected target; incompatible engine manifests fail
  activation without stock-engine fallback.
- Fake façades and engines prove synchronous `send(message)` request/reply and actionable failure diagnostics.
- `make build` passes.

## Progress

- [ ] Implement after the final v4 hardening sprint.
