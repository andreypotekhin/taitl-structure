# Capabilities

Capabilities tell whether a configured target can run or generate a checked Structure transform plan. Backend-specific
rules stay at the target boundary while discovery, symbolic execution, and generic IR remain backend-neutral.

This page covers the capability interface, compatibility policy, target variants, and alternative-backend extension
boundary. It explains the decision model, configuration, PySpark support, and requirements for a future backend. The
normative sources are the [Backend Capabilities
specification](../dev/specifications/BackendCapabilities.spec.md),
[Alternative Backends](../dev/specifications/AlternativeBackends.spec.md), [Compatibility
Policy](../dev/specifications/CompatibilityPolicy.spec.md),
and the [Backend Capabilities design](../dev/design/BackendCapabilities.design.md).

## Capability Boundary

Feature references define what a Structure operation means. Capabilities answer whether the selected target can lower
that meaning in the requested mode, profile, and variant. For example, join semantics define cardinality rules while a
capability profile decides whether the target can lower that join shape.

Capability checks must not be scattered through discovery, symbolic execution, generic IR construction, runtime
orchestration, or generated-code rendering. Those phases emit or consume capability requirements through one interface.

```text
Structure source
  -> backend-neutral TransformPlan
  -> capability requirements
  -> target capability decisions
  -> execution or generated output
```

Capability selection and checks are Spark-free. They do not import PySpark, start Java, create a `SparkSession`, connect
to a cluster, or inspect the installed runtime.


## Interface And Decisions

The internal capability object has a stable identity and a set of supported feature families:

```text
BackendCapabilities
  id
  target
  profile
  variant
  family
  requirements
  decisions
  imports
```

Requirements identify a feature without embedding backend implementation details:

```text
CapabilityRequirement
  group
  name
  mode
  source
  docs
```

Typical groups include `runtime`, `output`, `expression`, `join`, `type`, `validation`, `streaming`, and `hook`.
Examples are `runtime.online_execution`, `output.generated_python`, `expression.string_trim`,
`join.composite_equi_join`, `type.decimal_precision_38`, and `validation.strict_projection`.

A decision is one of:

```text
supported
unsupported
degraded
opaque
unknown
```

For the active target, required `unsupported` and `unknown` decisions are errors before execution or generation.
`degraded` is normally a warning, and `opaque` is a compatibility warning unless the target would invoke code without
a safe target declaration. Every decision should state the target, feature, source location when available, suggested
fix, and the relevant documentation link.

The structured decision also carries `backend`, `requirement`, `supported`, diagnostic `code`, `title`, `problem`,
`why`, `use`, `docs`, and `required_target`. Supported decisions may leave explanatory fields empty; unsupported or
unknown decisions must provide enough context to select a supported operation or target.

The concrete backend interface is intentionally small:

```text
BackendCapabilities
  id
  imports()
  supports(requirement)
  require(requirement)
```

`supports(...)` returns a `CapabilityDecision` without raising. `require(...)` returns a supported decision or enters
the
backend diagnostic path. `imports()` returns deterministic generated import metadata, including the PySpark aliases for
functions, types, DataFrame, SparkSession, Column, and generated runtime schema helpers.

The backend identity retains name, target range, variant, and semantic family. Ordinary PySpark uses
`ordinary_pyspark`; Spark Connect uses `spark_connect_dataframe`. A semantic family may evolve without renaming the
implementation family recorded by existing generated artifacts.


## Active Target Configuration

The default is:

```toml
[tool.structure]
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
```

Structure supports Python 3.11 and newer. The default PySpark profile targets PySpark 3.5.x and 4.0.x. The target
layer owns version-specific API decisions and should prefer the oldest compatible API when a range spans supported
PySpark lines.

The active target is distinct from compatibility reporting. Future configuration may ask for a report across targets:

```toml
compat_targets = ["pyspark", "polars", "duckdb"]
```

Compatibility targets do not change the active execution target. Non-PySpark targets may be recorded as reserved
metadata but do not claim execution support.

The target-family vocabulary is diagnostic metadata rather than an additional support claim. It includes
`pyspark_dataframe`, `spark_connect_dataframe`, `typed_python_dataframe`, `local_lazy_dataframe`,
`local_eager_dataframe`, `sql_relation`, `meta_relational_dsl`, and `distributed_python_dataframe`. A family does not
override an explicit capability decision.


## PySpark Profiles And Variants

`target_backend = "pyspark"` selects the bundled production target. The ordinary variant uses an in-process
`SparkSession`, `DataFrame`, and `Column` contract.

Spark Connect is a PySpark variant, not a separate backend id:

```toml
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

The variant uses the PySpark DataFrame and Column API over a remote session. It must reject classic-only assumptions
such as `SparkContext`, RDD access, direct JVM/Py4J access, `_jdf`, and private classic PySpark fields before execution
or generation is claimed compatible. Public DSL syntax and `StructureSession` semantics do not change.

Spark Connect support is limited to completed compiler-visible feature families with runtime evidence and explicit
documentation. Streaming lifecycle remains caller-owned.


## No-Spark and Private-API Boundary

Capability resolution and compiler checks must work without PySpark, Java, a Spark session, a cluster, or inspection of
the installed runtime. The ordinary PySpark profile may use DataFrame and Column APIs; Spark Connect rejects
`SparkContext`, RDD access, direct JVM/Py4J access, `_jdf`, and private classic PySpark fields. A capability profile
must
name those exclusions instead of discovering them by importing or executing the runtime during compilation.

## Target Adapter

An alternative target supplies an adapter for the modes it honestly supports:

```text
TargetAdapter
  capabilities
  type_mapper
  expression_lowerer
  relation_lowerer
  validation_lowerer
  hook_abi
  runtime_support
  generator
  online_runner
```

An adapter may provide generated SQL before direct execution, or a compatibility report before a full lowering path.
It must not rewrite user Structure source, silently fall back to UDFs, collect data locally, or claim unsupported IR is
portable. Output must be deterministic for identical source, configuration, target profile, and Structure version.


## Hooks And Portability

Compiler-visible Structure source is the portable portion of a transform. Hook bodies are opaque runtime source and may
use target-specific APIs. They must be scoped to the targets that can invoke them:

```python
@raw(inout=lane(orders) | lane(orders), target_backend="pyspark")
def clean_with_pyspark(self, *, orders, spark, ctx):
    return orders
```

`target_backend` may be one backend, a list, `"configured"`, or `"all"`. An omitted value resolves from
`hook_target_default`. A claim of `"all"` expresses an ABI promise but does not make the body inspectable; compatibility
checks may still report it as opaque.

The active compilation includes hooks whose effective target set contains the active target. Runtime execution must
refuse a hook whose target set excludes the active target. Compatibility reports should state whether target scope was
explicit or inherited.


## Alternative Backend Contract

Future backend support is Python-hosted and shares the same compiler-visible source contract. Initial backend families
may include PySpark-family targets, typed Python DataFrames, local lazy/eager DataFrames, SQL relations, and
meta-relational DSLs. The roadmap prioritizes PySpark-family variants first, then Polars LazyFrame and DuckDB, with Ibis
as a later meta-backend candidate. Dask DataFrame and Ray Dataset remain deferred until the relational core is stable.

An alternative backend is admitted only when it provides:

- a capability profile and target identity;
- type mapping for every claimed schema type;
- expression, filter, projection, join, and validation lowering for every claimed feature;
- a direct or generated execution mode reference;
- hook ABI rules or an explicit no-hooks limitation;
- unsupported-capability diagnostics with suggested fixes;
- deterministic output and no-runtime-import compiler tests;
- parity tests against PySpark or documented semantic-difference tests where parity is impossible.

Experimental profiles must be clearly labeled in diagnostics and documentation. Unsupported active-target requirements
fail before execution or generation; non-active compatibility checks may classify them as unsupported, degraded, opaque,
or unknown.


## Compatibility Reports and StructureTools

Compatibility checks resolve the target registry, validate generic IR, convert operations, expressions, validation
points, output modes, runtime modes, and hooks into requirements, then render a deterministic report containing target,
family, mode, supported/unsupported/degraded features, opaque boundaries, warnings, fixes, and documentation links.
For the active target, unsupported and unknown required capabilities are errors. Non-active targets may be reported as
unsupported, degraded, opaque, or unknown.

Future programmatic compatibility APIs should use the same engine and diagnostics as the CLI:

```python
StructureTools.compatibility.check(source_roots=["src"], targets=["pyspark", "polars"])
StructureTools.compatibility.explain(transform=EnrichOrders)
StructureTools.compatibility.targets()
```

Results are structured values before rendering, compiler checks do not import backend runtimes, and callers may choose
fail-fast exceptions or report-returning behavior. The CLI reports non-PySpark targets as pending rather than
claiming that an unimplemented backend was checked.


## Compatibility Policy

Structure follows semantic versioning after 1.0. Major releases may change DSL behavior, runtime APIs, config keys,
generated helper contracts, compatibility rules, supported Python/PySpark lines, or traceability/config schemas. Minor
releases may add features, diagnostics, target support, or backward-compatible metadata. Patch releases fix bugs and
documentation without changing public behavior.

Execution compatibility means caller-owned DataFrames bind to declared inputs, `StructureSession` owns the runtime
context, and direct execution preserves the same supported semantics as generated PySpark. Generated files are optional
committed artifacts and should carry Structure version, backend, profile, variant, and useful source identity metadata.

Generated-runtime helper changes require a major release, a compatibility shim, or a regeneration path that fails with a
clear upgrade diagnostic. Projects that commit generated output can verify it with:

```bash
structure compile --fail-on-diff
```

Compiler traceability uses a `major.minor` schema version. Breaking changes require a major bump; additive fields
require a minor bump, and consumers should ignore unknown additive fields. Config values and unknown keys remain strict;
New optional keys may appear in minor releases, while changing or removing a documented key requires a major release
after
1.0.


## Diagnostics

`BACKEND-E2401` means the configured target has no capability profile. `BACKEND-E2402` means the source or IR requires a
feature outside the selected profile. Future compatibility diagnostics may report degraded or unknown capabilities and
target-scoped hook violations.

A useful diagnostic identifies:

```text
Target: pyspark >=3.5,<4.1
Variant: ordinary
Transform: orders.transforms.order.EnrichOrders
Step: add_customer
Feature: join.null_safe_equality
Problem: the selected target profile cannot lower this requirement
Use: choose a supported operation or select a target profile that admits it
See: the linked feature or capability background
```


## Capability Catalog And Acceptance

Capability groups describe Structure semantics rather than mirroring every target-library function. Current groups are
`backend`, `expression`, `aggregate`, `compile`, `dedupe`, `docs`, `explain`, `higher_order`, `imports`, `join`,
`optimization`, `validation`, `streaming`, and `window`; future profiles may add `runtime`, `output`, `hook`, and
`type`.

The ordinary PySpark profile covers fields, literals, projections, filters, boolean and equality expressions, null-safe
equality, casts, standard helpers, lookup/analytical/rowset joins, aggregates, windows, higher-order arrays and maps,
schema-only validation, strict projection, streaming row-local work, stream-static left and inner joins, and generated
imports. Deferred requirements remain explicit unsupported decisions, including join strategies, repartition, richer
field-lineage explain, incremental compilation, stream-stream joins, and streaming orchestration. Generated
documentation is a capability-gated but implemented opt-in artifact family.

An alternative profile is complete only when it supplies type mapping, expression/filter/projection/join/validation
lowering, runtime or generated mode behavior, hook ABI rules or an explicit no-hooks limitation, deterministic output,
unsupported-capability diagnostics, no-runtime-import compiler tests, and parity or documented semantic-difference
tests.
The active target fails on required unsupported or unknown capabilities; compatibility reports may classify features as
degraded, opaque, or unknown.


## Appendix: Deliberate Boundaries

Capabilities do not define the meaning of joins, schemas, streaming operations, or validation. Those feature contracts
remain in their focused references. Capabilities only decide whether a selected target can support those meanings in the
requested mode. Capabilities also do not make Structure an orchestrator: data loading, storage writes, Spark lifecycle,
streaming query lifecycle, and deployment remain caller-owned.
