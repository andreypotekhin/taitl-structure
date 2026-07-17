# Platform API

## Purpose

The Platform API lets Structure Core invoke a selected platform plugin without importing its platform DSL, runtime, or
lowered plan types. It supports bundled PySpark and independently packaged external platforms while preserving Core's
ownership of target resolution, workflow lifecycle, artifacts, diagnostic rendering, storage, and CLI behavior.

A platform plugin owns target semantics: its schema extensions, platform DSL, applicability checks, lowering, target
diagnostics,
and opaque runtime values. Structure makes no portability promise between platform-specific transforms.

## Scope

This specification defines the public v1 API definitions, one-plugin discovery model, negotiated `PlatformAPI` façade,
its focused service facets, compiler request/result boundary, and capability reporting. It also defines the public
limits that keep this API small.

Private Core engine replacement is not part of this specification. Its compatibility gate and manifest are intentionally
private and are described only in `docs/dev/design/PlatformCallbackArchitecture.md`.

## Discovery and Negotiation

A plugin distribution exposes exactly one plugin through one unversioned Python entry-point group named
`structure.platform`. The entry-point name is the short user-facing platform name, such as `pyspark` or `iterable`.
Discovery reads package metadata before importing the plugin implementation.

The plugin exposes a `PlatformDescriptor` with platform name, display name, distribution identity, plugin version, and
inclusive minimum/maximum Platform API versions. It then supplies one `PlatformAPI` façade for a requested version.

Core selects the highest version in the overlap of its range and the selected plugin's range. No overlap, a façade
for an unadvertised version, or a façade missing a required facet fails target activation before compilation or
execution.
An artifact records that negotiated version and loading it requests the recorded version rather than silently upgrading.

Conceptually, the unversioned API definitions contain:

    class PlatformPlugin(Protocol):
        @property
        def descriptor(self) -> PlatformDescriptor: ...

        def api(self, version: int) -> PlatformAPI: ...

## PlatformAPI

`PlatformAPI` is the only versioned public façade returned by a plugin. It is immutable or session-scoped and has no
global activation behavior. It exposes three required service facets and three optional lifecycle facets:

    class PlatformAPI(Protocol):
        schema: SchemaAPI
        compiler: CompilerAPI
        capabilities: CapabilitiesAPI
        executor: ExecutionAPI | None
        generator: GenerationAPI | None
        serializer: SerializationAPI | None

The version belongs to the import package, not the class names. For example, v1 code imports
`structure.platform.api.v1.PlatformAPI` and `CompilerAPI`; it never imports `PlatformAPI1` or `CompilerAPI1`.
Core activates exactly one negotiated versioned API package for a selected plugin and session. It rejects any attempt to
mix service facets from different versioned API packages in that façade.

`schema`, `compiler`, and `capabilities` must be present. `executor`, `generator`, and `serializer` are `None` only
when the capability facet reports that corresponding lifecycle service as unavailable. Core must report a mismatch
between claimed capability and missing/present optional facet as a platform compatibility error.

Core routes a workflow to the appropriate service facet. It never registers, discovers, version-negotiates, or
configures a
facet independently. A facet may delegate internally to any number of platform-owned classes; that is not a Platform
API extension point.

## Compiler Facet

The compiler facet has one public workflow-shaped entry point:

    class CompilerAPI(Protocol):
        def compile(self, request: CompileRequest) -> PlatformCompilation: ...

`CompileRequest` is immutable and contains only facts Core already owns: the discovered transform declaration, resolved
Core schema structure, selected target and target constraints, opaque platform configuration, and source locations for
diagnostics. Core does not add a method per expression, join, aggregation, validation rule, or compilation phase.

`PlatformCompilation` contains an opaque lowered payload, optional opaque analysis payload, deterministic fingerprint
material, and Core diagnostic records. The plugin decides platform DSL semantics, symbolic interpretation, operation
applicability, semantic validation, and lowering. For example, PySpark decides whether `having()` is valid after its
own grouping operation; the iterable plugin decides the limits of its smaller platform DSL. Core stores and routes
opaque
payloads but does not inspect them to infer target semantics.

The schema facet validates platform field ownership and materializes target schema representations. The capabilities
facet returns lifecycle capabilities plus target-defined inspection records. Execution validates a duck-typed runtime
and evaluates an opaque payload. Generation returns content only; Core owns file writes. Serialization encodes and
decodes only opaque payloads; Core owns the outer artifact envelope and persistence.

## API Containment Rules

- One entry point discovers one plugin; one plugin creates one negotiated façade.
- Core calls a small number of workflow-shaped facet methods, not target-operation methods.
- Platform plans, expressions, target schemas, analysis, and runtime objects remain opaque to Core.
- Optional lifecycle facets are absent when unsupported; there are no empty plugin registrations.
- Public v1 contracts are additive only. A new stable need creates a later version rather than mutating v1.
- Platform DSL APIs are imported from platform packages, never added to the `structure` root.

## Acceptance Evidence

Tests must prove metadata-only discovery, one plugin per platform name, symmetric version downgrade, and rejection of a
missing required facet or lifecycle capability/facet mismatch. Fake PySpark-like and iterable-like façades must prove
that Core invokes `compiler.compile(request)` without inspecting their opaque plans.

Tests must prove that no service facet is separately registered or independently negotiated.
