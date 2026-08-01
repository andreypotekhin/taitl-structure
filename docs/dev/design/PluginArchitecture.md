# Design: Plugin Architecture

## Purpose

Target ownership lives outside Core. Core retains the application workflow—configuration, discovery, structural
analysis, target resolution, artifacts, storage, CLI presentation, and final diagnostics. A selected plugin owns its
target DSL, schema extensions, target validation, lowering, runtime execution, generation, and target diagnostics.

The design does not promise that source written for one plugin is portable to another. A transform resolves exactly one
target; different transforms in a project may select different installed plugins, but a composed pipeline cannot cross
that boundary.

## Public Shape

The `structure` package exports target-neutral declarations such as `Schema`, `Transform`, `input`, `output`,
`@transform`, `StructureConfig`, and `StructureSession`. Target DSL names are imported from their plugin packages:

    from structure import Schema, Transform, input, output
    from structure.plugin.pyspark import string, where

`@transform(target=...)` may name a target. Otherwise, resolution uses an explicit workflow `target=`, then
`plugin.default`. An explicit target and decorator target must agree. Target-specific configuration belongs in the
opaque `[plugin.<name>]` table; the PySpark plugin owns `plugin.pyspark.profile` and `plugin.pyspark.variant`.

## Discovery and Negotiation

An installed distribution exposes one entry point in the `structure.plugin` group. Core reads entry-point metadata
before importing the implementation. It rejects duplicate eligible plugin names rather than depending on installation
order; normalized distribution names can be disabled in configuration.

After target resolution, Core loads the one selected plugin, validates its descriptor identity, and negotiates the
highest Plugin API version in the Core/plugin overlap. No overlap, a descriptor mismatch, or an incomplete facade fails
activation before compilation or execution. Artifacts record the plugin name, distribution identity, plugin version,
negotiated API version, plugin options, payload version, and Core structural metadata. Loading requests the recorded
version rather than silently upgrading a payload.

## Plugin API Boundary

`PluginAPI` is the sole versioned public facade. Schema, authoring, compiler, and capabilities facets are required.
Execution, generation, and serialization facets are optional only when the capability report says that lifecycle is
unavailable.

Core owns transform lifecycle semantics: declaration inheritance, source order, bindings, lanes, outputs, hook
placement, artifact envelopes, cache identity, generated-file preflight/writes, and diagnostic rendering. For each
step, Core opens the selected plugin's authoring session, receives its symbolic arguments, and captures one opaque
plugin body. The selected plugin validates and lowers target semantics through its compiler facet. Core transports but
does not inspect target expressions, plans, schemas, runtime values, or payloads.

The resulting architecture is:

```text
Core workflow -> negotiated PluginAPI service facet -> opaque target result -> Core public boundary
```

This keeps Core able to orchestrate schema, compilation, execution, generation, serialization, capabilities, and CLI
workflows without importing PySpark implementation modules or any external plugin package.

## PySpark Plugin

The bundled `pyspark` plugin implements the supported PySpark 3.5.x and 4.0.x behavior. It owns fields, expressions,
joins, aggregations, schema materialization, capability rules, lowering, online execution, generated rendering, and
Spark Connect diagnostics. Its `ordinary` and `spark-connect` variants are PySpark plugin options; Core handles them
as opaque values.

Compiler-only workflows remain free of PySpark, Java, SparkSession, and cluster startup. At runtime,
`StructureSession` passes a caller-owned Spark session to the selected plugin executor. Online and generated PySpark
execution consume the same lowered semantic contract so validation, projection, hooks, aliases, and expressions retain
parity. Structure never owns Spark session lifecycle, reads, writes, streaming query lifecycle, triggers,
checkpoints, or sinks.

## External Plugin Evidence

The finite Iterable example is built and installed as a separate wheel to prove the public boundary. It imports only
the public API and demonstrates real entry-point discovery, version negotiation, disabled-distribution and duplicate
diagnostics, target isolation, opaque serialization, and default-denied private-engine injection. Its step-oriented DSL
demonstrates finite projection, joins, grouping, recurrence, repeatable collection, and target-owned Python generation.
It is a teaching and conformance fixture, not a supported production target.

## Private Engine Replacement

Private Core-engine replacement is not a Plugin API extension point. It is disabled by default, applies only after its
plugin has been selected, and is gated by an exact private engine-suite revision plus a Structure compatibility range.
The sole opt-in is `plugin.plugin_options = "allow_injection"`; users should enable it only for trusted plugins. A
failed gate makes the selected target unavailable rather than silently falling back to a stock engine. The manifest,
engine classes, and context are private and intentionally absent from public plugin-authoring guidance.

## Transformation API Coverage

The PySpark plugin maintains a versioned transformation coverage catalog for the PySpark 3.5.x/4.0.x intersection.
The catalog is not a mechanical wrapper list: an operation is admitted only when its source syntax is typed, symbolic
execution determines type/nullability/cardinality, the IR is backend-neutral, shared recipes drive online and generated
execution, capability checks fail early, diagnostics name a remedy, and the appropriate parity and target evidence
exists. Loading, storage, catalog management, writes, actions, query lifecycle, raw SQL, arbitrary callbacks, and
dynamic schemas remain outside the transformation catalog.

The catalog records API family, Structure spelling, status (`supported`, `scheduled`, `deferred`, `unsupported`, or
`design-gated`), target profiles, input/output contracts, semantic differences, exclusion reasons, and evidence. A
public helper cannot be documented as supported without capability, rendering, and execution/generated-code parity
evidence. The catalog and its machine-readable inventory are maintained locally; the build does not download Spark
documentation.

## PySpark API Closure

The PySpark plugin owns target meaning end to end: symbolic capture, validation, recipes, capabilities, online
evaluation, generated source, traceability, explain, and diagnostics. Core owns declarations, method order, lanes,
outputs, lifecycle, and routing to exactly one selected plugin. Every API follows this path:

```text
authoring helper -> symbolic body -> immutable target record -> recipe -> capability/diagnostic ->
online evaluator and generated renderer -> explain/traceability
```

The v6 closure work formalized implicit global aggregation, explicit scalar UDFs, typed relation operations, and the
bounded ordered `scan(...)` recurrence while retiring example hooks only when output-equivalence evidence proved the
migration. Relation operations include typed generators, exact-schema set composition, named self aliases, typed order
and literal bounds, plan-visible assertions, bounded hierarchy closure/fallbacks, and declared-key first-qualified
selection. Sampling, physical-plan directives, dynamic schemas, source/sink ownership, arbitrary UDTFs/Pandas/RDD
operations, and unbounded recurrence remain explicit boundaries.

Plugin decomposition extracts focused delegates for operation construction, expression/type construction, symbolic result
construction, evaluation, step execution, rendering, module generation, and traceability. Public imports and generated
behavior remain stable; extraction is not a module-count goal. No component may claim support by calling raw DataFrame
code, Spark actions, or hidden Python fallbacks on only one execution path.
