# Plugin Authoring

An external Structure plugin is a Python distribution that implements a compile target.
Structure discovers entry points through plugin metadata. The plugin provides target-specific DSL from its own package, it must never import `structure.core`.

## Minimal distribution

For minimal distribution declare a single entry point with the exact plugin name. The example below exposes the `example` compile target
from a vendor package named `acme_structure_example`.

    [project.entry-points."structure.plugin"]
    example = "acme_structure_example:ExamplePlugin"

The plugin object provides a `PluginDescriptor` and supplies Plugin API implementatin. Descriptor
name and distribution must match the entry-point name and distribution name after normalizing hyphens,
underscores, and periods. Structure selects the highest version of Plugin API supported by both sides if plugin supplies several API versions.

    from structure.plugin.api import PluginDescriptor
    from structure.plugin.api.v1 import PluginAPI
    
    class ExamplePlugin:
        descriptor = PluginDescriptor(
            name="example",
            display_name="Acme Example",
            distribution="acme-structure-example",
            plugin_version="1.0.0",
            minimum_api_version=1,
            maximum_api_version=1,
        )
    
        def api(self, version: int) -> PluginAPI:
            if version != 1:
                raise ValueError(f"Unsupported Plugin API version: {version}")
            return PluginAPI(
                schema=ExampleSchema(),
                authoring=ExampleAuthoring(),
                compiler=ExampleCompiler(),
                capabilities=ExampleCapabilities(),
                executor=ExampleExecutor(),
                serializer=ExampleSerializer(),
            )

The `schema`, `authoring`, `compiler`, and `capabilities` facets are required. `executor`, `generator`, and
`serializer` are optional; omit a facet by passing `None` when the plugin does not support that workflow.
Structure sends requests, and plugin returns responses as defined in `structure.plugin.api.v1.model`.
Structure owns transform discovery, target selection, artifacts, file writes, diagnostics rendering, CLI integration, and
runtime lifecycle. Plugin owns schema definitions, authoring, compiling, capability reporting, diagnostics, with optionally executor and serializer.

## Target DSL and execution

Plugin owns target platform DSL and schema field declarations. End-user imports Structure declarations from `structure`
and the target vocabulary from the plugin package. For example:

    from structure import Schema, Transform, input, output, transform
    from acme_structure_example import text
    
    class Source(Schema):
        value = text()
    
    @transform(target="example")
    class Copy(Transform):
        source = input(Source)
        result = output(Source)

The authoring facet supplies symbolic values when Structure invokes the transform method. The compiler receives the opaque
captured body. Do not add an operation-specific Structure callback, write files from a generation facet, or start a global
runtime session.

## Conformance check

Use the public conformance helper in the plugin's own tests. It performs descriptor identity checks, selects the
highest mutually supported API version, requests that façade, and verifies all required facets. Its diagnostics pinpoint
the missing contract pieces.

    from structure.plugin.api.conformance import PluginConformance
    
    result = PluginConformance.negotiate(
        ExamplePlugin(),
        entry_name="example",
        distribution="acme-structure-example",
    )
    assert result.api_version == 1

To integrate, install the wheel in a clean test environment and discover it through `structure.plugin` entry-point metadata.
This proves that no source checkout import or hidden Structure dependency masks a packaging error. Sprint 21's finite
iterable fixture supplies this end-to-end evidence, including repeatable result collection and opaque-payload serialization.

The repository's [finite Iterable plugin example](../../examples/plugins/iterable/Readme.md)
shows the recommended focused package layout: entry point, API façade, target DSL, symbolic step authoring, compiler,
execution, schema, generation, capability, and serialization applications. It is a finite in-memory teaching target,
not a supported production backend.

## Unused-step optimization

The optional `PluginAPI.optimizer` facet enables Core-owned whole-step pruning. Existing plugins need not implement
it. Import `OptimizationAPI`, `OptimizationRequest`, `OptimizationGraph`, `OptimizationStep`, and `OptimizationReport`
from `structure.plugin.api.v1`; never import Core's optimizer implementation.

Return `PluginCompilation.executable_steps` as the complete ordered inventory of stable executable step identities.
`describe(request)` returns one `OptimizationStep` per inventory entry in the same order. Dependencies identify
earlier producers, not mutable frame names. Resolve hook replacements and reused frame names before describing the
graph. Final outputs populate `output_roots`; visible stage outputs populate `stage_roots`. Input frames need no node,
but their required binding and validation contracts must survive optimization.

Set `removable=True` only with established removal-safety facts. The default false retains unknown behavior. Supply
a plain-language reason for retained checks, hooks, nondeterminism, lifecycle operations, and other observable work.
Core retains output roots, visible stage roots, unsafe steps, and their transitive dependencies in original order.
Missing nodes, duplicate identities, cycles, and dangling dependencies fail with `PLUGIN-E2708` rather than silently
producing a partial program. See the actionable diagnostic and verify the plugin's ordered producer mapping.

`apply(request, selection)` filters whole steps using `selection.retained_steps`, returning a new `PluginCompilation`
with a matching inventory. Core revalidates the resulting graph. Preserve validations already assigned during
lowering, original analysis, diagnostics, public schemas, and resource ownership; do not force new runtime actions.
Attach the selection to the executable payload when needed for explain/serialization. Core also exposes it as
`PluginCompilation.optimization`, `CompiledTransform.optimization`, and `PluginArtifact.optimization`. The report includes decisions, retained and
removed identities, and plain-language explanations. Documentation-only compilation does not call this facet.

Pruning runs after plugin lowering and pre-optimization streaming diagnosis, before rendering or execution. Plugins
must preserve source/safety errors and base subsequent executable-growth information on retained work. Configuration
and transform options use `prune_unused_steps`; false leaves the compiled payload unchanged by this pass.

## Compatibility policy

Plugin API versions are inclusive ranges. A newer Structure and an older plugin may use the plugin's highest supported
version when their ranges overlap; an older Structure behaves the same way with a newer plugin. A serialized artifact keeps
its negotiated version and must be rebuilt if that version is unavailable. A plugin must reject a requested version it
does not advertise.
