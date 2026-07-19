# Design: Platform Callback Architecture

## Purpose

v5 makes a Structure target platform a plugin rather than a collection of PySpark assumptions inside Core. A plugin
owns the platform DSL its users import, its schema conventions, lowering, semantic rules, and target diagnostics. Core
owns application workflows: discovery, target resolution, lifecycle, artifact envelopes, storage, final diagnostics,
and CLI presentation.

The design deliberately does not promise that transform code for one platform is portable to another. A PySpark
transform imports PySpark-oriented APIs; a Polars transform imports Polars-oriented APIs. Each transform compiles for
exactly one platform. Different transforms in one project may use different platforms, but a composed pipeline may not
cross that boundary.

This document defines the architecture to be implemented by v5. The executable sequence is
`docs/dev/planning/P07162601.V5-platform-callback-architecture.plan.md`.
The public API details are specified in `docs/dev/specifications/PlatformAPI.md`.

## Terms

**Platform name** is the user-facing short selection name, for example `pyspark` or `iterable`. It is the value used by
configuration and `@transform(target=...)`.

**Distribution** is the Python packaging identity of an installed wheel or project. It is used internally to discover a
plugin, disable a plugin, and explain conflicts. It is not combined with the platform name for normal selection.

**Platform plugin** is one distribution that exposes exactly one platform name. The bundled PySpark plugin lives at
`structure.platform.pyspark`; an external plugin owns its own Python package. A plugin has its own public platform DSL,
rather than contributing target operations to the `structure` package root.

**Callback object** is a target-supplied object invoked by a Core workflow. A Callback object owns target semantics but
not the workflow lifecycle. A function passed to another function remains a callback, not a Callback object.

**Engine** is the private Core implementation of one workflow, such as compilation, execution, generation, schema, or
serialization. Core supplies stock engines. A selected plugin can optionally replace one with a compatible private
class, as described below.

## User-Facing Shape

Structure retains its target-independent declarations:

    from structure import Schema, Transform, transform
    from structure.platform.pyspark import field
    from structure.platform.pyspark import col, join, sum

The second import is the target-owned platform DSL. It may be syntactically close to the native target and may differ
substantially from another platform's DSL. An external wheel is responsible for documenting its own import path.

`Schema` and the basic field-declaration contract remain in Core. A platform may extend the contract with custom field
definitions, or require that all usable field definitions come from that platform. Core asks the selected schema
Schema Callback object to validate platform ownership before a schema is materialized or lowered.

`@transform` accepts zero or one platform name. An explicit compile target must match the decorator target. Omitting the
decorator lets the explicit target, then the configured default, select the platform. A target constraint on `@raw`
narrows the selected platform, for example to a version or profile; it never changes the platform name. Declaring more
than one platform on a transform is an error.

## Discovery and Selection

Core enumerates one `structure.platform` Python package entry-point group before importing a plugin implementation.
Its entry-point name is the platform name, and metadata identifies the distribution that supplied it. One discovered
plugin returns the complete negotiated platform façade; Core does not discover separately registered service objects.

Installed plugins are eligible by default. Configuration can disable one or more distributions. When two eligible
distributions claim the same platform name, selection fails with a diagnostic naming both distributions and showing how
to disable one. Core never uses installation order to choose one.

After Core resolves a platform name, it loads the single selected plugin. The plugin exposes the plugin descriptor,
supported Platform API range, and optional private engine-replacement metadata, then returns the complete negotiated
platform façade. Core keeps discovered plugins and negotiated façades immutable or scoped to the current session;
importing a plugin must not create global active-target state.

## Public Platform API

Unversioned Platform API definitions are under `structure.platform.api`. Versioned API definitions are under
`structure.platform.api.v1`. The unversioned definitions describe plugin identity and supported API ranges, then obtain
one `PlatformAPI` façade for the selected version.

`PlatformAPI` exposes small schema, authoring, compiler, and capability service facets, plus optional execution, generation, and
serialization facets. Schema, authoring, compiler, and capability are required. The other facets are absent only when capabilities
report the related lifecycle service as unavailable. Requests and results use public immutable models. Target plans,
runtime values, and target-specific analysis may
be opaque to Core: Core routes and fingerprints them through the appropriate service facet but does not interpret them.

Core owns the workflow around each service facet. For example, the compile engine discovers a transform, establishes
source and diagnostic context, builds the neutral plan, invokes its methods in Core-controlled order through the
selected platform authoring facet, invokes `platform.compiler.compile(request)` with the completed plan, produces the
standard artifact envelope, caches it, and renders failures. The compiler facet decides whether a target operation is legal, such as whether
`having()` follows `group_by()`, performs target lowering, and supplies target diagnostic text through Core diagnostic
records.

Capabilities are lifecycle and inspection information at the Core boundary: online execution, generation, streaming,
schema materialization, and serialization. A plugin's semantic operation support remains target-owned. A capability CLI
can ask the plugin for those records; Core does not centralize a generic `join.left_join` compatibility taxonomy.

## Callback Version Negotiation

The plugin and Core declare a minimum and maximum supported Platform API version. Core chooses the highest shared
version, then asks the plugin for that version's `PlatformAPI` façade. This works in both directions: a newer
Core can use an older plugin at that plugin's maximum, and a newer plugin can use an older Core at the Core maximum. No
overlap is a target-activation error.

The negotiated version is recorded in every artifact. Loading an artifact requests its recorded version rather than
silently upgrading it. A plugin that claims support for a version but cannot return its complete façade is rejected
before the workflow begins.

## Default Engines and Private Replacements

The normal architecture is:

    Core workflow -> current stock engine -> negotiated PlatformAPI service facet

Core's router selects the platform, negotiates the public API, instantiates the relevant engine, and validates public
outputs. It cannot be replaced. That preserves consistent target resolution, artifacts, diagnostic presentation, and
the public `TransformResult` and schema/serialization contracts.

An advanced platform can make a selected workflow use a different engine class:

    Core workflow -> selected private replacement engine -> negotiated PlatformAPI service facet

The plugin exposes an optional, automatically discovered private manifest. Conceptually, it contains a mapping such
as `{CompileEngine: AcmeCompileEngine}`, a compatible `requires_structure` range, and an exact
`core_engine_revision`. Core constructs the replacement class itself with the current private `EngineContext`; the
plugin does not supply an instance or a public factory. The replacement may subclass the current private base engine or
the stock concrete engine, but it must return the same public artifacts, results, reports, schemas, and serialized
envelopes as the stock engine.

The replacement applies only after its platform is selected, its compatibility is verified, and Core finds the global
`platform.plugin_options = "allow_injection"` opt-in. It applies only to that workflow. Thus a project compiling a
PySpark transform and an iterable transform can use different engine classes without global mutable state. The
distribution name is relevant to discovery and duplicate-id diagnostics only; the replacement is selected by the
resolved platform name and its unique selected plugin.

## Engine Compatibility Gate

Private engine replacement is intentionally stricter than public Platform API compatibility. It does not participate in
Platform API downgrade negotiation. A plugin with a replacement manifest declares both a normal package dependency on
Structure and the manifest's `requires_structure` range plus `core_engine_revision` token.

Class injection is disabled by default. The global Structure configuration setting
`platform.plugin_options = "allow_injection"` is the sole v5 opt-in and applies to any selected plugin; it is not a
platform-specific setting. Before resolving a replacement class, Core requires that value and otherwise fails the
requested transform with a diagnostic naming the plugin and explaining that class injection is disabled by default.
The diagnostic shows the exact setting and advises users to enable it only for trusted plugins.

`core_engine_revision` represents the complete current suite of private engine contracts, including the current private
base classes and `EngineContext`. Structure changes it only when those contracts change. This catches the unsafe hybrid
case in which a plugin compiled for an older compiler engine is placed beside newer stock schema, execution, or
serialization engines. A Structure release that leaves private engine contracts unchanged need not force a plugin
rebuild.

Core validates both requirements before resolving or constructing replacement classes. A mismatch makes the selected
platform unavailable. Core must not ignore the manifest and silently use stock engines, because that would hide an
intended platform behavior change. The diagnostic identifies platform name, distribution, current and declared Structure
requirements, current and declared engine revisions, and whether the remedy is to install a compatible plugin, disable
the distribution, or select another target.

Engine bases, context, manifest shape, and engine-class mappings are private and unstable. They are deliberately not
part of public external-plugin documentation. Core still fails early and validates the standard public boundary after a
replacement runs, so a broken vendor class produces an actionable diagnostic rather than corrupting artifacts.

When engine behavior affects an artifact, Core adds the effective engine-suite revision and replacement class identity
to its cache and replay identity. This prevents a serialized or cached artifact from being reused under a materially
different engine implementation.

## Iterable Conformance Fixture

The internal `iterable` platform proves the public boundary from a separately packaged fixture wheel. It accepts finite
iterables of row mappings, including lists, tuples, and one-shot generators. It provides its own minimal platform DSL,
supports projection, inner and left equi-joins, grouped sum/count, and `collect()` returning deterministic row
mappings. It may materialize data for joins and aggregation, returns a re-iterable result, and implements deterministic
opaque-plan serialization.

It is not a public production target, does not claim streaming or generation, and must not appear in end-user feature
documentation. Its tests demonstrate real metadata discovery, API negotiation, disablement, conflict diagnostics,
isolation from `structure.core`, and the private engine-compatibility gate where useful.

## Migration Boundary

v5 removes target-owned platform DSL names and field constructors from `structure` immediately. PySpark users import
from
`structure.platform.pyspark`. The Core `StructureSession` becomes
`StructureSession(runtime=..., context=..., config=...)` and passes a duck-typed runtime to the selected platform;
PySpark accepts a `SparkSession` and iterable accepts an iterable. Core never imports a target runtime in compiler-only
workflows.

The older same-source multi-backend direction in `AlternativeBackends.md` is superseded for v5. A future portability
layer would require a separate proposal and cannot be inferred from the platform plugin architecture.
