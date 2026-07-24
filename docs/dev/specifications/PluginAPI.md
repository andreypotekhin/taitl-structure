# Plugin API

## Purpose

The Plugin API lets Structure Core invoke a selected plugin without importing its plugin DSL, runtime, or
lowered plan types. It supports bundled PySpark and independently packaged external plugins while preserving Core's
ownership of target resolution, workflow lifecycle, artifacts, diagnostic rendering, storage, and CLI behavior.

A plugin owns target semantics: its schema extensions, plugin DSL, applicability checks, lowering, target
diagnostics,
and opaque runtime values. Structure makes no portability promise between target-specific transforms.

## Scope

This specification defines the public v1 API definitions, one-plugin discovery model, negotiated `PluginAPI` façade,
its focused service facets, workflow request/result boundaries, and capability reporting. It also defines the public
limits that keep this API small. Plugin configuration and target-resolution precedence are specified separately in
[PluginConfiguration.md](PluginConfiguration.md).

Private Core engine replacement is not part of this specification. Its compatibility gate and manifest are intentionally
private and are described only in `docs/dev/design/PluginArchitecture.md`.

## Discovery and Negotiation

A plugin distribution exposes exactly one plugin through one unversioned Python entry-point group named
`structure.plugin`. The entry-point name is the short user-facing plugin name, such as `pyspark` or `iterable`.
Discovery reads package metadata before importing the plugin implementation.

A plugin name is an ASCII lowercase identifier matching `[a-z][a-z0-9-]*`. It is case-sensitive and must be used
unchanged in the entry point, descriptor, `@transform(target=...)`, and configuration. This deliberately keeps a
plugin table addressable as `plugin.<name>` in TOML and prevents aliases from obscuring duplicate detection.

The plugin exposes a `PluginDescriptor` with plugin name, display name, distribution identity, plugin version, and
inclusive minimum/maximum Plugin API versions. It then supplies one `PluginAPI` façade for a requested version.

Core validates that the descriptor's plugin name equals the entry-point name and that its normalized distribution
identity equals the distribution that supplied the entry point. A mismatch is a plugin compatibility error; Core must
not make a plugin reachable through an alias. The descriptor must be readable without global target activation.

Core selects the highest version in the overlap of its range and the selected plugin's range. No overlap, a façade
for an unadvertised version, or a façade missing a required facet fails target activation before compilation or
execution.
An artifact records that negotiated version and loading it requests the recorded version rather than silently upgrading.

Conceptually, the unversioned API definitions contain:

    class Plugin(Protocol):
        @property
        def descriptor(self) -> PluginDescriptor: ...

        def api(self, version: int) -> PluginAPI: ...

## PluginAPI

`PluginAPI` is the only versioned public façade returned by a plugin. It is immutable or session-scoped and has no
global activation behavior. It exposes four required service facets and three optional lifecycle facets:

    class PluginAPI(Protocol):
        schema: SchemaAPI
        authoring: AuthoringAPI
        compiler: CompilerAPI
        capabilities: CapabilitiesAPI
        executor: ExecutionAPI | None
        generator: GenerationAPI | None
        serializer: SerializationAPI | None

The version belongs to the import package, and the façade class follows the conventional name order. For example, v1
code imports `structure.plugin.api.v1.PluginAPI` and `CompilerAPI`; it never imports `V1PluginAPI` or
`CompilerAPI1`.
Core activates exactly one negotiated versioned API package for a selected plugin and session. It rejects any attempt to
mix service facets from different versioned API packages in that façade.

`schema`, `authoring`, `compiler`, and `capabilities` must be present. `executor`, `generator`, and `serializer` are `None` only
when the capability facet reports that corresponding lifecycle service as unavailable. Core must report a mismatch
between claimed capability and missing/present optional facet as a plugin compatibility error.

Core routes a workflow to the appropriate service facet. It never registers, discovers, version-negotiates, or
configures a
facet independently. A facet may delegate internally to any number of plugin-owned classes; that is not a Plugin
API extension point.

## Compiler Facet

The compiler facet has one public workflow-shaped entry point:

    class CompilerAPI(Protocol):
        def compile(self, request: CompileRequest) -> PluginCompilation: ...

`CompileRequest` is immutable and contains only facts Core already owns: the discovered transform declaration, its
backend-neutral `TransformPlan` analysis, selected target and target constraints, Core transport configuration, the
selected plugin's opaque `plugin_options`, and source locations for diagnostics. Core does not add a method per
expression, join, aggregation, validation rule, or compilation phase. Core does not validate vendor option names or
values, and never exposes one plugin's option table to another plugin.

`PluginCompilation` contains an opaque lowered payload, optional Core analysis payload, deterministic fingerprint
material, and Core diagnostic records. Core owns structural inspection and invokes step methods in declaration order.
The authoring facet supplies symbolic arguments and captures the returned plugin body. The plugin decides target
applicability, target-specific semantic validation, and lowering. Core stores and routes opaque payloads but does not
inspect them to infer target semantics.

The schema facet validates plugin field ownership and materializes target schema representations. The capabilities
facet resolves the target-defined capability model for an explicit profile and variant. Execution validates a duck-typed
runtime and evaluates an opaque payload. Generation returns content only; Core owns file writes. Serialization encodes
and decodes only opaque payloads; Core owns the outer artifact envelope and persistence.

## Facet Boundaries

Every public v1 request and result model is immutable. Requests contain Core-owned facts plus opaque plugin values
previously returned by the same negotiated façade. Results may contain opaque values, but every diagnostic and
capability record uses the standard Core model. A plugin must not require callers to import its implementation package
to construct a request.

The required authoring facet owns symbolic authoring without owning Transform lifecycle:

    class AuthoringAPI(Protocol):
        def open_step(self, request: StepAuthoringRequest) -> StepAuthoringSession: ...

Core resolves inheritance, bindings, source order, decorators, and hooks, then enters one returned session for the
step. The session returns one symbolic argument per input binding and captures the returned DSL value as an opaque
plugin body. It must not call Core compiler, frontend, capability, traceability, schema, or registry facades.

The required schema facet accepts a schema declaration and selected-plugin context, validates field ownership, and
returns a normalized Core schema plus an opaque materialization payload. It must reject a foreign plugin field before
the compiler facet starts.

The required compiler facet accepts `CompileRequest` and returns `PluginCompilation`. `CompileRequest.analysis` is
the backend-neutral `TransformPlan` produced by Core before the call. The plugin returns its opaque lowered payload;
Core attaches the plan to the compilation for its artifact workflow. Fingerprint material
must be deterministic for equal semantic source and configuration and must not contain runtime object identities.

The required capabilities facet resolves a semantic capability model from a target profile and variant:

    class CapabilitiesAPI(Protocol):
        def resolve(self, *, profile: str, variant: str) -> BackendCapabilities: ...

The returned model answers typed `CapabilityRequirement` queries. It must not inspect a live runtime or compile a
transform merely to answer a capability query.

When present, the execution facet accepts an opaque compiled payload, normalized Core inputs, a caller-owned opaque
runtime, optional caller context, and execution options. It validates runtime suitability and returns the target output
inside the Core result boundary. It never starts a global session or owns external resource lifecycle.

When present, the generation facet accepts an opaque compiled payload and Core-owned generation metadata, then returns
an immutable collection of relative paths and file contents. Paths must be relative, normalized, unique, and remain
inside the Core-selected generated root. Core preflights every result and performs all writes atomically only after all
selected facets succeed.

When present, the serialization facet encodes and decodes only opaque plugin payloads. It receives the recorded
Plugin API version and a plugin-private payload version, and returns deterministic bytes or immutable data plus that
payload version. Core owns checksums, envelope framing, storage paths, plugin identity checks, and recovery diagnostics.
The facet must reject a payload version it cannot decode rather than attempting an implicit upgrade.

An artifact envelope records the plugin name, normalized plugin distribution identity, plugin version, negotiated API
version, selected plugin configuration, payload version, Core schema/input/output metadata, diagnostics,
traceability, and semantic/cache fingerprints. Execution, generation, and decoding must reject an envelope whose
plugin, distribution, plugin version, API version, or effective engine identity does not match the selected target;
the safe remedy is to install the recorded plugin version or rebuild the artifact.

## API Containment Rules

- One entry point discovers one plugin; one plugin creates one negotiated façade.
- Core calls a small number of workflow-shaped facet methods, not target-operation methods.
- Plugin plans, expressions, target schemas, analysis, and runtime objects remain opaque to Core.
- Optional lifecycle facets are absent when unsupported; there are no empty plugin registrations.
- v1 includes the authoring boundary required for plugin-owned DSL semantics.
- Plugin DSL APIs are imported from plugin packages, never added to the `structure` root.

## Acceptance Evidence

Tests must prove metadata-only discovery, one plugin per plugin name, symmetric version downgrade, and rejection of a
missing required facet or lifecycle capability/facet mismatch. Fake PySpark-like and iterable-like façades must prove
that Core invokes `compiler.compile(request)` without inspecting their opaque plans.

Tests must prove that no service facet is separately registered or independently negotiated.

Plugin authors can use the public `PluginConformance.negotiate(...)` helper described in
[PluginAuthoring.md](../PluginAuthoring.md) to check descriptor identity, API-version negotiation, and required
facets without importing a Core implementation module.
