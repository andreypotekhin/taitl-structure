# Architecture

The detailed parity and boundary register is [Parity.md](Parity.md). Current API and streaming gates are maintained in
[docs/dev/gated](gated/), with deferred direction in [docs/dev/deferred](deferred/).

Structure is a schema-driven compiler and runtime toolkit. Core owns the public workflow: it discovers source,
resolves configuration and a plugin target, analyzes transform structure, manages artifacts, and presents diagnostics.
A selected plugin owns the target language and runtime semantics. The bundled PySpark plugin is the supported target;
the public Plugin API also permits independently packaged plugins.

## Goals

- Schema-first, IDE-friendly transform authoring.
- Optimizer-visible target execution and optional generated code.
- One explicit target per transform and composed pipeline.
- Plugin-owned target DSLs without target imports in Core workflows.
- Spark-free compiler-only commands for the bundled PySpark plugin.
- Caller-owned Spark, streaming, input/output, and orchestration lifecycle.

## High-Level Data Flow

```text
source schemas and transforms
        |
        v
Core: configuration, discovery, structural analysis, target resolution
        |
        v
selected Plugin API: schema + authoring + compiler + capabilities
        |
        v
Core artifact envelope (opaque plugin payload)
        |                         |
        v                         v
Plugin executor              Plugin generator
        |                         |
        v                         v
target runtime               Core-owned generated-file write
```

Core creates no globally active target. It discovers package entry-point metadata before importing an implementation,
selects one plugin by name, negotiates the highest mutually supported Plugin API version, and invokes the resulting
facade. An artifact records its plugin identity, negotiated API version, selected plugin options, and opaque payload
version so it cannot silently run under a different plugin contract.

## Ownership Boundary

`structure` exports target-neutral declarations such as `Schema`, `Transform`, `input`, `output`, and
`StructureSession`. Target DSL names belong to a plugin package. A PySpark project therefore imports its field
definitions, expressions, joins, and hooks from `structure.plugin.pyspark`.

Core owns structural transform semantics: inheritance, input and lane bindings, source order, output routing, artifact
envelopes, cache identity, file lifecycle, and diagnostic presentation. The plugin authoring facet provides symbolic
arguments and captures a step body. The plugin compiler validates and lowers that opaque body. Core transports opaque
plugin values but does not inspect them to infer target semantics.

The required Plugin API facets are schema, authoring, compiler, and capabilities. Execution, generation, and
serialization are optional only when the plugin capability report declares the corresponding lifecycle unavailable.
Core owns the workflow around every facet: for example, it owns generated-file validation and atomic writes while a
plugin generator supplies relative paths and content.

## Selection and Configuration

Each transform resolves exactly one target in this order: `@transform(target=...)`, an explicit caller `target=`, then
`plugin.default`. A composed pipeline must resolve to one identical target before any target service runs. A project may
contain independent transforms for different installed plugins, but Structure does not translate or compose data across
plugin boundaries.

Plugin selection is configured under `[tool.structure.plugin]`; target-specific options live in
`[tool.structure.plugin.<name>]`. The PySpark plugin owns its `profile` and `variant` options. CLI commands use
`--target`; Python sessions use `StructureSession(target=...)`; capability and schema-tool workflows accept the same
generic target name. See [PluginConfiguration.md](specifications/PluginConfiguration.spec.md).

## PySpark Plugin

The bundled `pyspark` plugin supplies the supported PySpark 3.5.x and 4.0.x target behavior, including schema
materialization, DSL authoring, capability rules, lowering, execution, rendering, and target diagnostics. Its
`ordinary` and `spark-connect` variants are plugin options, not Core concepts. Compiler-only workflows remain free of
PySpark, Java, SparkSession, and cluster startup; runtime execution receives a caller-owned Spark session.

Online and generated execution consume one PySpark-owned lowered semantic contract, preserving parity in expression
lowering, validation placement, hooks, aliases, and projection shape. Structure never takes ownership of Spark session
creation or termination, reads, writes, streaming queries, checkpoints, triggers, or orchestration.

## Extension Boundary

External plugins are normal Python distributions with a `structure.plugin` entry point and a vendor-owned import
package. They implement the public Plugin API only and can verify their descriptor, negotiation, and required facets
with `PluginConformance`. The repository's finite Iterable example is conformance evidence and a teaching example, not
a supported production target.

Private Core engine replacement is deliberately outside the public Plugin API and disabled by default. It is
target-local, exact-revision-gated, and documented only in [PluginArchitecture.md](design/PluginArchitecture.design.md).
