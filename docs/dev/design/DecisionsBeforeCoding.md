# Decisions Before Coding

## Purpose

This specification records the release-shaping decisions that must be settled before broad implementation begins.
It is a guardrail for the first coding passes: contributors should not rediscover these choices while implementing
schemas, discovery, validation, joins, hooks, configuration, generation, and runtime execution.

This document does not replace narrower specifications. It points to the owning documents and states which decisions
are mandatory for v1 implementation.

## Decision Set

The v1 implementation must follow these decisions:

- ordinary Python source roots are the input roots;
- generated code lives below a distinct generated namespace;
- schema declarations use explicit immutable type objects;
- compiler commands do not import PySpark or require Spark, Java, a SparkSession, or a cluster;
- online execution is the default runtime mode;
- generated PySpark remains an optional, committed, reviewable artifact;
- online and generated execution consume the same checked semantic contract;
- default validation is schema-only, with value-level data-quality constraints requiring explicit opt-in;
- hooks are explicit runtime escape hatches and are opaque to compile-time expression analysis;
- `@expr_fn` is the public compiler-visible extension point for reusable expression logic;
- v1 lookup joins use `join_one(...)`; row-multiplying and existence joins are v2+ features;
- diagnostics are registry-backed, stable, structured, and linked to documentation;
- the first implementation checkpoint is first executable slice, a narrow executable vertical slice, before full v1 breadth.

## Owning Specifications

The implementation must treat these documents as the source of truth:

```text
docs/specifications/SourceModuleRules.md
docs/specifications/DSL.md
docs/specifications/SymbolicExecution.md
docs/specifications/SchemaDeclarationSyntax.md
docs/specifications/SchemaModel.md
docs/specifications/SchemaSemantics.md
docs/specifications/SchemaInheritance.md
docs/specifications/NullabilityAndTypeCoercion.md
docs/specifications/ValidationSemantics.md
docs/specifications/JoinSemantics.md
docs/specifications/HookSemantics.md
docs/specifications/ConfigSchema.md
docs/specifications/CLI.md
docs/specifications/CompatibilityPolicy.md
docs/specifications/CompilerPerformanceTargets.md
docs/specifications/Diagnostics.md
docs/specifications/IntermediateRepresentation.md
docs/specifications/ExecutionSemanticContract.md
docs/specifications/OnlineExecution.md
docs/specifications/PySparkCodeGeneration.md
docs/specifications/BackendCapabilities.md
docs/specifications/DataQualityConstraints.md
docs/specifications/StreamingCompatibility.md
docs/specifications/AnalyticalJoinCoverage.md
```

When these documents overlap, the narrower feature specification owns the detailed behavior. This document owns only
the pre-coding decision inventory.

## Challenge Resolution Index

The pre-coding documentation gaps from `docs/dev/design/Challenges.md` are resolved as follows:

| Challenge | Resolution |
| --- | --- |
| C1 | `SourceModuleRules.md`; `D06172601.Source-root-resolution.md` |
| C2 | `SchemaDeclarationSyntax.md`; `D06172602.Schema-declaration-syntax.md` |
| C3 | `NullabilityAndTypeCoercion.md`; `P06172601.Nullability-and-type-coercion-rules.plan.md` |
| C4 | `DSL.md`; `HookSemantics.md`; implementation proof remains a Sprint 0 spike |
| C5 | `DSL.md`; `SymbolicExecution.md`; implementation proof remains a Sprint 0 spike |
| C6 | `SourceModuleRules.md` |
| C7 | `D06182601.Generated-code-ownership.md` |
| C8 | `HookSemantics.md`; `D06182602.Hook-input-escape-hatch.md` |
| C9 | `JoinSemantics.md`; `D06172607.Join-semantics.md` |
| C10 | `ValidationSemantics.md`; `DataQualityConstraints.md`; `D06182603.Intermediate-validation-policy.md` |
| C11 | `StreamingCompatibility.md`; `D06182604.Streaming-compatibility-v1.md` |
| C12 | `IntermediateRepresentation.md`; `PySparkCodeGeneration.md`; `CompatibilityPolicy.md` |
| C13 | `CompilerPerformanceTargets.md` |
| C14 | `CompilerPerformanceTargets.md`; production incremental compile remains v2 implementation work |
| C15 | `D06182606.No-spark-compile-dependency.md` |
| C16 | `Readme.md` generated-code comparison |
| C17 | `docs/dev/Testing.md`; `docs/dev/Style.md`; feature-spec acceptance criteria |
| C18 | `ConfigSchema.md` |
| C19 | `docs/Compatibility.md`; `CompatibilityPolicy.md`; `D06182605.Versioning-and-compatibility-policy.md` |
| C22 | `P06202601.v1-first-executable-slice.plan.md`; first executable slice model fixture; Sprint 01 plan |
| C23 | `BackendCapabilities.md`; `BackendCapabilities` design; `D06202604.Backend-capability-interface.md` |
| C24 | `ExecutionSemanticContract.md`; `ExecutionSemanticContract` design; `D06202601` |
| C25 | `Readme.md`; `docs/Compatibility.md`; compileability checker design |
| C26 | `DataQualityConstraints.md`; `DataQualityConstraints` design; `D06202602` |
| C27 | `AnalyticalJoinCoverage.md`; `AnalyticalJoinCoverage` design; `D06212601` |
| C29 | `docs/Diagnostics.md`; `Diagnostics.md`; `DiagnosticsContract` design; `D06202603` |

C20 is superseded by C31. C21, C28, C30, and C31 remain real gaps, but they are not missing semantic design or
specification documents. They track executable package wiring, operational integration recipes, executable test
breadth, and licensing/governance signals.

## Release-Blocking Decisions

### Package and Source Layout

Structure ships as the importable package `structure`. User source code is discovered under configured source roots,
not under a Structure-specific project folder.

Default source-root resolution:

1. CLI flags.
2. Configuration from `[tool.structure]`.
3. Configuration from `structure.toml`.
4. `src` when `./src` exists and contains importable modules or packages.
5. Project root.

Generated code defaults to:

```text
generated/structure_generated/<source import path>/pyspark/...
```

The generated namespace must not shadow the shipped `structure` package.

### Schema Syntax

The canonical schema form is:

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
```

Lowercase sentinels such as `string` are not canonical. Annotation-only, dataclass, Pydantic, and Spark-string type
syntax are outside v1 unless a later compatibility layer is explicitly specified.

### Spark-Free Compiler

`structure check`, `structure compile`, and `structure compile --fail-on-diff` must operate without PySpark, Java,
SparkSession creation, Spark startup, or cluster access.

Runtime execution and runtime tests may import PySpark. Compiler tests must not require PySpark unless the test is
specifically exercising generated or online runtime behavior.

### Execution Modes

Online execution is the default:

```toml
[tool.structure]
execution_mode = "online"
```

Generated execution remains available:

```toml
[tool.structure]
execution_mode = "generated"
```

Both modes must lower from the same checked IR and target execution recipe. The online runner must not execute rendered
generated source text. The generator must not re-decide semantics while formatting source.

### Generated Code Ownership

Generated code is owned by Structure and may be committed, reviewed, diffed, and imported. Developers must not hand-edit
generated files. CI should use `structure compile --fail-on-diff` once generation exists.

### Validation Boundary

Default validation is schema-only. It validates DataFrame shape and must not scan rows.

Value-level constraints, uniqueness checks, referential checks, row counts, freshness checks, and other data-quality
work require explicit opt-in because they may trigger Spark actions or expensive plans.

### Hook Boundary

Hooks are intentional PySpark escape hatches. They are attached to a compiled subtransform with `@before(...)` or
`@after(...)`, run at runtime, and must return a DataFrame. The compiler records hook metadata and treats the hook body
as opaque.

### Join Boundary

v1 supports `join_one(...)` lookup joins with explicit `Join.LEFT` or `Join.INNER`. It must not silently deduplicate
right-side rows. If right-side uniqueness is not proven, Structure emits a warning by default.

### Extension Boundary

The supported public extension surface is:

- `@expr_fn` for compiler-visible reusable expression helpers;
- `@before(...)` and `@after(...)` for runtime DataFrame escape hatches.

Compiler registries, backend capability providers, validation policy plugins, schema type adapters, and diagnostic
renderers are internal or deferred until specified.

## Implementation Rules

Before implementing a feature, the contributor must identify its owning specification and add missing behavior there
before adding code.

Every supported operation must have:

- public DSL syntax or explicit runtime API;
- compiler metadata or IR shape;
- backend capability requirement when backend support matters;
- online and generated execution behavior when both modes support it;
- diagnostics for invalid source and unsupported targets;
- acceptance tests or planned tests in `tests/specs`.

## Diagnostics

Expected pre-coding decision violations include:

```text
DISC-E0201  invalid source root
DISC-E0202  unsafe source module import
CONF-E0101  unknown configuration key
SCHEMA-E0301 invalid schema declaration
DSL-E0401   unsupported symbolic expression
HOOK-E0701  invalid hook declaration
JOIN-W0601  join_one uniqueness is not proven
BACKEND-E2401 unsupported backend target
```

Exact code numbers are owned by `docs/specifications/Diagnostics.md` and the diagnostic registry. Feature specs may
use provisional examples until the registry exists, but implementation tests must assert registered codes.

## Implementation Checklist

1. Keep this decision inventory in sync with the specifications it references.
2. Add or update a narrower specification before implementing any public behavior not covered here.
3. Implement source-root discovery before broad transform discovery.
4. Implement import-safe schema and transform metadata before symbolic execution.
5. Implement schema model, nullability, validation, hooks, joins, and generation against the shared IR contracts.
6. Add diagnostics with links to the most specific public documentation.
7. Add spec tests before marking user stories complete in `docs/dev/Specification.md`.

## Acceptance Criteria

- Every item from `docs/dev/design/Challenges.md` under "Recommended Pre-Coding Docs to Add" has an owning
  implementation-ready specification.
- A contributor can identify whether a feature belongs in Sprint 01, v1, v2, v3, or v4 without reading design discussion
  transcripts.
- Source layout, schema syntax, validation, hooks, joins, configuration, compatibility, diagnostics, and compiler
  performance all have concrete acceptance criteria in specifications.
- No implementation work depends on importing PySpark during compiler commands.
- New feature work can cite a narrower spec rather than relying on this summary alone.
