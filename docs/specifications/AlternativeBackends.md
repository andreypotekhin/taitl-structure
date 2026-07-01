# Alternative Backends

## Purpose

This specification defines the future backend-extension contract for Structure. It allows the same compiler-visible
Structure source code to be checked against, and eventually emitted for, multiple execution backends.

PySpark remains the only v1 supported runtime target. This document specifies the architecture that future work must
follow before adding Python-hosted Spark SQL, type-safe PySpark DataFrame patterns, Pandas, Polars, DuckDB, Spark
Connect, Ibis, or other targets through Ibis.

## Scope

This specification owns:

- backend-neutral source compatibility;
- backend target identity and families;
- active-target and multi-target compatibility checks;
- target adapter responsibilities;
- fail-fast behavior for unsupported IR;
- warnings for opaque or potentially non-portable source;
- hook target scoping;
- StructureTools compatibility APIs;
- acceptance criteria for future backend additions.

Backend-specific specifications still own concrete lowering. For example, `PySparkCodeGeneration.md` owns PySpark
source rendering. A future `PolarsCodeGeneration.md` or `DuckDBCodeGeneration.md` would own those targets.

## Same-Source Contract

Structure should distinguish two source categories:

```text
compiler-visible source
opaque runtime source
```

Compiler-visible source includes:

- schema declarations;
- compiled subtransform methods;
- expression helper functions that symbolic execution can inspect;
- Structure expression, filter, join, validation, and projection DSL;
- transform, input, output, validation, and streaming metadata.

Opaque runtime source includes:

- hook bodies;
- caller-owned session objects;
- caller-owned backend objects;
- arbitrary imports or side effects inside code the compiler does not inspect.

The same-source portability promise applies only to compiler-visible source. Hooks may still live in the same class, but
they must be target-scoped and excluded from claims that the compiled Structure source is backend-neutral.

## Target Identity

Resolved configuration must select a target:

```text
BackendId
  name
  target
  family
```

Examples:

```text
pyspark >=3.5,<4.1 pyspark_dataframe
spark_sql >=3.5,<4.1 sql_relation
polars >=1.0 local_lazy_dataframe
duckdb >=1.0 sql_relation
pandas >=2.0 local_eager_dataframe
ibis >=9.0 meta_relational_dsl
```

`name` is the configured backend id. `target` is a version range or capability profile. `family` captures semantic
shape.

## Target Families

Allowed initial families:

```text
pyspark_dataframe
spark_connect_dataframe
typed_python_dataframe
local_lazy_dataframe
local_eager_dataframe
sql_relation
meta_relational_dsl
distributed_python_dataframe
```

Family is diagnostic metadata. Support is still decided by explicit capability requirements.

## Configuration

Future configuration keys:

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
compat_targets = ["pyspark", "polars", "duckdb"]
hook_target_default = ["pyspark"]
```

Compatibility notes:

- `target_pyspark` remains supported for the PySpark target.
- `target_profile` is the generic successor for non-PySpark targets.
- `compat_targets` asks for a portability report and does not change the active target.
- `hook_target_default` supplies the effective target set for unmarked hooks.

`hook_target_default` values:

```text
["pyspark"]       default before alternative backends are stable
["configured"]   the active target only
["all"]          author promises backend-portable hook ABI
"explicit"       every hook must declare target_backend
```

`"all"` should warn for opaque hooks during compatibility checks because the compiler cannot prove the hook body is
portable.

## Target Adapter

Every backend must provide a target adapter:

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

Only supported modes need implementation. For example, DuckDB may provide a SQL generator before it provides online
execution. Spark SQL may provide SQL rendering while still using PySpark-family runtime support.

Adapter rules:

- no target runtime imports during compiler commands;
- no silent UDF, row-wise, collect, or local-materialization fallback;
- deterministic output for identical source, config, target profile, and Structure version;
- diagnostics with target, feature, source location when available, suggested fix, and docs link;
- capability tests for every supported and unsupported feature family.

## Capability Requirements

Alternative backend support extends the existing requirement model:

```text
CapabilityRequirement
  group
  name
  mode
  source
  docs
```

Additional groups:

```text
runtime
output
hook
type
```

Examples:

```text
runtime.online_execution
output.generated_python
output.generated_sql
hook.target_scoped_hook
type.decimal_precision_38
expression.string_trim
join.composite_equi_join
validation.strict_projection
```

Capability decisions may return:

```text
supported
unsupported
degraded
opaque
unknown
```

For the active target, `unsupported` and `unknown` are errors when the feature is required to run. `degraded` is a
warning unless the project config chooses fail-on-degradation. `opaque` is a warning for compatibility reports and an
error only when a target would invoke opaque code without a safe target declaration.

## Compatibility Checks

`structure check` must validate the active target:

```bash
structure check --target-backend pyspark
structure check --target-backend polars --target-profile ">=1.0"
```

Future multi-target checks:

```bash
structure check --compat-targets pyspark,polars,duckdb
structure explain orders.transforms.order.EnrichOrders --compat-targets pyspark,polars
```

Required active-target checks:

1. Resolve target capabilities from static metadata.
2. Validate generic IR.
3. Convert every IR operation, expression, validation point, output mode, runtime mode, and hook declaration into
   capability requirements.
4. Fail on unsupported or unknown required capabilities.
5. Warn on degraded, opaque, or portability-risk capabilities.
6. Render a deterministic report.

Required compatibility report fields:

```text
target
family
mode
supported features
unsupported features
degraded features
opaque boundaries
warnings
suggested fixes
docs
```

## StructureTools API

StructureTools should expose the same compatibility engine without requiring shell commands:

```python
from structure import StructureTools

report = StructureTools.compatibility.check(
    source_roots=["src"],
    targets=["pyspark", "polars", "duckdb"],
)
```

Recommended API shape:

```text
StructureTools.compatibility.check(...)
StructureTools.compatibility.explain(transform=...)
StructureTools.compatibility.targets()
```

Rules:

- the API must use the same target registry and diagnostics as the CLI;
- results must be structured values before any text rendering;
- the API must not import backend runtimes during compiler checks;
- callers can choose fail-fast exceptions or report-returning behavior.

## Hook Target Scoping

Hook decorators gain optional target metadata:

```python
@after(normalize, lane=orders, target_backend="pyspark")
def clean_with_pyspark(self, *, orders, spark, ctx):
    ...
```

Rules:

- `target_backend` is a backend id, a list of backend ids, `"configured"`, or `"all"`.
- Use `target_backend="pyspark"` for a single backend.
- Use `target_backend=["pyspark", "polars"]` only when one hook intentionally supports multiple Python-hosted backends.
- Missing `target_backend` resolves from `hook_target_default`.
- Active compilation includes only hooks whose effective target set includes the active target.
- A hook with `"all"` claims the hook ABI is portable, but its body remains opaque.
- Compatibility checks warn for unmarked hooks when checking any target outside the default.
- Compatibility checks warn when a hook body appears to import or reference a backend outside its declared target set.
- Runtime execution must refuse to invoke a hook when the active backend is not in the hook's effective target set.

Hook metadata recorded in IR extends `HookDef`:

```text
HookDef
  name
  target_step
  timing
  source_order
  pass_inputs
  schema_mode
  project_output
  streaming_safe
  target_backend
  target_defaulted
  source_path
  source_line
```

Target-specific hooks are opaque target operations. Traceability must show the boundary, effective target set, and
whether the target set was explicit or inherited from configuration.

## Diagnostics

Future diagnostics should extend the backend and hook code ranges. Proposed codes:

```text
BACKEND-E2403  unsupported execution mode for backend
BACKEND-W2401  degraded backend capability
BACKEND-W2402  backend compatibility is unknown
HOOK-E0702     hook is not enabled for target backend
HOOK-W0702     hook backend portability is opaque
HOOK-W0703     hook target default was inherited
```

Unsupported active-target example:

```text
CompileError BACKEND-E2402: Unsupported backend capability

Target:
  polars >=1.0

Transform:
  orders.transforms.order.EnrichOrders

Subtransform:
  add_customer

Feature:
  join.null_safe_equality

Problem:
  The Polars target profile does not support this Structure null-safe join requirement yet.

Use:
  Rewrite the join to use supported equality semantics, or keep target_backend = "pyspark" for this transform.

See docs/specifications/AlternativeBackends.md
```

Opaque hook warning example:

```text
Warning HOOK-W0702: Hook backend portability is opaque

Hook:
  EnrichOrders.remove_negative_totals after normalize

Targets checked:
  pyspark, polars

Problem:
  The hook body is not compiler-visible, so Structure cannot prove it works for Polars.

Use:
  Add target_backend="pyspark", write a Polars-specific hook, or move the logic into compiler-visible Structure DSL.

See docs/specifications/AlternativeBackends.md
```

## Backend Admission Criteria

A new backend may be documented as supported only when it has:

- a capability profile;
- generic compatibility checks;
- type mapping for every supported schema type;
- expression, filter, projection, join, and validation lowering for every claimed feature;
- generated or online execution mode specification;
- hook ABI rules or an explicit no-hooks limitation;
- diagnostics for unsupported capabilities;
- deterministic output tests;
- no-runtime-import compiler tests;
- parity tests against PySpark or documented semantic difference tests where parity is impossible.

Experimental backend profiles may exist behind explicit config, but diagnostics and docs must label them experimental.

Backend roadmap priority:

- v2: PySpark-family targets first, including Spark SQL exploration and typed Python DataFrame/Dataset patterns.
- v3: Polars LazyFrame and DuckDB.
- v4: Ibis.
- Beyond v4: other targets only through Ibis when Ibis supports them.
- Deferred: Dask DataFrame and Ray Dataset until after the relational core is stable.

## Acceptance Criteria

Alternative backend infrastructure is ready when tests prove:

- target profiles resolve without importing backend runtimes;
- unknown backend ids fail before source execution reaches target lowering;
- compiler-visible IR requirements are checked for the active target;
- unsupported active-target features fail before online execution or generation;
- degraded features produce structured warnings;
- compatibility reports can compare at least two target profiles for one transform;
- hook target defaults are resolved deterministically;
- unmarked hooks warn during multi-target compatibility checks;
- hooks declared for another target are not invoked at runtime;
- StructureTools compatibility APIs return the same structured diagnostics as CLI checks;
- generated artifacts include the target backend and target profile in deterministic metadata;
- traceability marks target backend, target profile, and opaque hook target scope.

## Non-Goals

This specification does not require v1 to implement non-PySpark execution. It also does not require Structure to become
a wrapper around every backend's native API. Backend support is admitted only for Structure semantics that can be
represented in IR, checked, lowered, tested, and explained.
