# Platform API

## Purpose

The Platform API lets Structure Core invoke a selected target platform without importing its authoring API, runtime, or
lowered plan types. It supports bundled PySpark and independently packaged external platforms while preserving Core's
ownership of target resolution, workflow lifecycle, artifacts, diagnostic rendering, storage, and CLI behavior.

A platform owns target semantics: its schema extensions, grammar, applicability checks, lowering, target diagnostics,
and opaque runtime values. Structure makes no portability promise between platform-specific transforms.

## Scope

This specification defines the public v1 bootstrap, one-provider discovery model, negotiated `PlatformAPI1` façade,
its focused service facets, compiler request/result boundary, capability reporting, and façade-level synchronous
messaging. It also defines the public limits that keep this API small.

Private Core engine replacement is not part of this specification. Its compatibility gate and manifest are intentionally
private and are described only in `docs/dev/design/PlatformCallbackArchitecture.md`.

## Discovery and Negotiation

A plugin distribution exposes exactly one provider through one unversioned Python entry-point group named
`structure.platform`. The entry-point name is the short user-facing platform id, such as `pyspark` or `iterable`.
Discovery reads package metadata before importing the provider implementation.

The provider exposes a `PlatformDescriptor` with platform id, display name, distribution identity, plugin version, and
inclusive minimum/maximum Platform API versions. It then supplies one `PlatformAPI1` façade for a requested version.

Core selects the highest version in the overlap of its range and the selected provider's range. No overlap, a façade
for an unadvertised version, or a façade missing a required facet fails target activation before compilation or
execution.
An artifact records that negotiated version and loading it requests the recorded version rather than silently upgrading.

Conceptually, the public bootstrap is:

    class PlatformProvider(Protocol):
        @property
        def descriptor(self) -> PlatformDescriptor: ...

        def api(self, version: int) -> PlatformAPI1: ...

## PlatformAPI1

`PlatformAPI1` is the only versioned public façade returned by a provider. It is immutable or session-scoped and has no
global activation behavior. It exposes three required service facets and three optional lifecycle facets:

    class PlatformAPI1(Protocol):
        schema: SchemaAPI1
        compiler: CompilerAPI1
        capabilities: CapabilitiesAPI1
        executor: ExecutionAPI1 | None
        generator: GenerationAPI1 | None
        serializer: SerializationAPI1 | None

        def send(self, message: object) -> object: ...  # optional, duck typed

`schema`, `compiler`, and `capabilities` must be present. `executor`, `generator`, and `serializer` are `None` only
when the capability facet reports that corresponding lifecycle service as unavailable. Core must report a mismatch
between claimed capability and missing/present optional facet as a platform compatibility error.

Core routes a workflow to the appropriate facet. It never registers, discovers, version-negotiates, or configures a
facet independently. A facet may delegate internally to any number of platform-owned classes; that is not a Platform
API extension point.

## Compiler Facet

The compiler facet has one public workflow-shaped entry point:

    class CompilerAPI1(Protocol):
        def compile(self, request: CompileRequest) -> PlatformCompilation: ...

`CompileRequest` is immutable and contains only facts Core already owns: the discovered transform declaration, resolved
Core schema structure, selected target and target constraints, opaque platform configuration, and source locations for
diagnostics. Core does not add a method per expression, join, aggregation, validation rule, or compilation phase.

`PlatformCompilation` contains an opaque lowered payload, optional opaque analysis payload, deterministic fingerprint
material, and Core diagnostic records. The platform decides target grammar, symbolic interpretation, operation
applicability, semantic validation, and lowering. For example, PySpark decides whether `having()` is valid after its
own grouping operation; the iterable platform decides the limits of its smaller grammar. Core stores and routes opaque
payloads but does not inspect them to infer target semantics.

The schema facet validates platform field ownership and materializes target schema representations. The capabilities
facet returns lifecycle capabilities plus target-defined inspection records. Execution validates a duck-typed runtime
and evaluates an opaque payload. Generation returns content only; Core owns file writes. Serialization encodes and
decodes only opaque payloads; Core owns the outer artifact envelope and persistence.

## Synchronous Messaging

`send(message)` belongs to `PlatformAPI1`, not to a service facet or Core engine. A stock or privately replaced Core
engine may call `platform.send(message)` and receive its direct return value. It is optional: absence of a callable
method is an unsupported target-local extension condition.

The message and reply may be arbitrary in-process Python objects. The platform vendor owns their types, mutation,
thread safety, recursion, and compatibility. Core does not serialize, cache, persist, inspect, transport, broadcast, or
asynchronously deliver them. A send failure is wrapped at the engine boundary with platform id, distribution, engine,
message type, and nesting-depth context.

`send` is an escape hatch, not a generic dispatcher. It must not carry normal compiler, schema, execution, generation,
or serialization requests. A recurring cross-platform need becomes a typed method in a later `PlatformAPI<N>` version.

## API Containment Rules

- One entry point discovers one provider; one provider creates one negotiated façade.
- Core calls a small number of workflow-shaped facet methods, not target-operation methods.
- Platform plans, expressions, target schemas, analysis, and runtime objects remain opaque to Core.
- Optional lifecycle facets are absent when unsupported; there are no empty provider registrations.
- Public v1 contracts are additive only. A new stable need creates a later version rather than mutating v1.
- Target authoring APIs are imported from platform packages, never added to the `structure` root.

## Acceptance Evidence

Tests must prove metadata-only discovery, one provider per platform id, symmetric version downgrade, and rejection of a
missing required facet or lifecycle capability/facet mismatch. Fake PySpark-like and iterable-like façades must prove
that Core invokes `compiler.compile(request)` without inspecting their opaque plans.

Tests must prove `PlatformAPI1.send(message)` returns synchronously, missing `send` reports unsupported use, nested
sends are allowed, and a thrown exception produces the standard engine-boundary diagnostic. They must also prove that
no service facet is separately registered or independently negotiated.
