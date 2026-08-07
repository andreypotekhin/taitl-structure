# Project Background

Structure began as a way to express schema-driven PySpark transformations in ordinary Python while preserving the
DataFrame operations Spark can optimize. It provides two equivalent delivery paths: direct execution against a
caller-owned runtime and optional generated source that projects the same checked transformation behavior.

The project deliberately does not own data loading, storage writes, orchestration, Spark session lifecycle, or
streaming query lifecycle. It concentrates on typed schemas, transform structure, compiler-visible operations,
diagnostics, and generated-code reviewability.

## Target Architecture

Structure makes the target boundary explicit. Core is no longer a PySpark implementation layer: it owns source
discovery, target selection, structural analysis, artifact envelopes, generated-file lifecycle, and diagnostics. A
selected plugin owns the target DSL and semantics. The bundled PySpark plugin remains the supported production target,
while the public, versioned Plugin API permits an independent distribution to provide another target.

This separation is intentional rather than a portability claim. Target-specific source imports target-specific DSL
names—for PySpark, `structure.plugin.pyspark`. A transform resolves exactly one target from its decorator, an explicit
`target=`, or `plugin.default`; composed pipelines may not cross targets. Different independent transforms in one
project may select different installed plugins.

Core discovers plugin metadata through Python package entry points before importing an implementation, rejects
duplicate eligible names, negotiates the highest mutually supported Plugin API version, and stores opaque plugin
payloads in versioned Core artifact envelopes. The plugin authoring, schema, compiler, capability, execution,
generation, and serialization facets keep target data out of Core while letting Core retain a single coherent workflow
and diagnostic surface.

PySpark compilation remains Spark-free: ordinary `check`, `compile`, inspection, and generation workflows require no
PySpark import, Java process, Spark session, or cluster. At execution time, the caller supplies Spark; the PySpark
plugin performs target validation and execution. Online and generated PySpark paths use one lowered semantic contract
to keep their observable behavior aligned.

The repository's finite Iterable plugin is conformance evidence for external packaging, discovery, negotiation,
execution, serialization, and generation. It is deliberately a teaching fixture rather than a second supported runtime.

See [Plugin Architecture](PluginArchitecture.design.md), [Architecture](../Architecture.md), and
[Plugin API](../specifications/PluginAPI.spec.md) for the durable design and public contract.
