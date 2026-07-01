# Design: Alternative Backends

## Purpose

Structure should eventually let the same compiler-visible Structure source lower to more than one execution backend.
PySpark remains the v1 product target, but the architecture should not force PySpark assumptions into DSL discovery,
symbolic execution, IR, diagnostics, or compatibility checks.

This design describes how Structure can grow from a PySpark runtime/compiler into a backend-neutral Structure compiler
with Python-hosted target adapters for PySpark, Spark SQL, Pandas, Polars, DuckDB, and other relational DataFrame DSLs.

## Same-Source Goal

The goal is:

```text
one Structure source module
  -> checked backend-neutral IR
  -> target-specific online execution or generated artifacts
```

The promise applies to compiler-visible Structure code: schemas, compiled subtransforms, expression helpers, filters,
joins, validation policy, traceability, and target capability checks.

Hooks are explicitly excluded from the same-source promise. They are target-specific escape hatches. Structure should
make hook target scope visible, configurable, and checkable so a PySpark hook is never accidentally called with a
Polars, Pandas, DuckDB, or Spark SQL object.

## Candidate Backends

Initial candidates:

- PySpark DataFrame: current v1 target, distributed, lazy, optimizer-visible, online and generated.
- Spark SQL: PySpark-family SQL/relation target through Python `SparkSession` APIs.
- Type-safe Python Dataset/DataFrame patterns: an investigation area for Python apps that want stronger static typing
  over PySpark DataFrames without leaving Python.
- Spark Connect: PySpark-family target with client/server constraints, deferred until the contract is tested.
- Pandas DataFrame: local eager target, valuable for small data, tests, examples, and developer tooling.
- Polars LazyFrame: local or distributed-adjacent lazy target, strong expression model, good fit for compiler-visible
  IR.
- DuckDB SQL or relation API: local analytical SQL target, useful for generated SQL and embedded analytics.
- Ibis: portable expression DSL that can target DuckDB, BigQuery, Snowflake, Polars, and others; attractive as a
  meta-backend, but it may hide target-specific differences behind another abstraction.
- Dask DataFrame and Ray Dataset: useful scale-out Python ecosystems, but their semantics and optimizer visibility
  leave them out of scope until after the relational core is stable.

Roadmap priority:

- v2: PySpark-family targets first, including Spark SQL exploration and typed Python DataFrame/Dataset patterns.
- v3: Polars LazyFrame and DuckDB as the first non-PySpark targets.
- v4: Ibis as a meta-backend.
- Beyond v4: other targets only through Ibis when Ibis supports them, unless a later design reopens direct support.
- Deferred: Dask DataFrame and Ray Dataset until after the relational core is stable.

## Target Families

Backends should be grouped by semantic family rather than by library name alone. This is future diagnostic vocabulary,
distinct from the current v1 implementation family `ordinary_pyspark`.

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

Family metadata lets diagnostics say more than "unsupported". For example, Pandas may support a projection but not a
streaming check; DuckDB may support a join but require generated SQL rather than online DataFrame execution.

## Architecture

The compiler remains IR-first:

```text
source
  -> discovery
  -> symbolic execution
  -> backend-neutral IR
  -> generic validation
  -> target compatibility checks
  -> target execution plan
  -> online runner or generated emitter
```

Target adapters own:

- capability profile;
- type mapping;
- expression lowering;
- relation operation lowering;
- validation implementation;
- hook ABI;
- generated layout;
- runtime support helpers;
- diagnostics for target-specific limitations.

Generic compiler phases must not import PySpark, Pandas, Polars, DuckDB, Ibis, or any other backend runtime while
running compiler commands. Target profiles are static metadata selected from configuration.

## Capability Boundary

The existing backend capability interface is the right boundary. It should grow from a PySpark version selector into a
target registry:

```text
BackendCapabilities
  id
  family
  modes
  imports()
  supports(requirement)
  require(requirement)
  explain(plan)
```

`supports(...)` remains non-throwing. `require(...)` emits structured diagnostics. `explain(...)` can power CLI and
StructureTools compatibility reports.

Capabilities describe Structure semantics, not backend APIs. Requirements should ask whether the target supports
`expression.trim`, `join.left_join`, `validation.strict_projection`, or `runtime.online_execution`, not whether a
specific method name exists in a library.

## Compatibility Workflow

Compatibility checks should run in three places:

- `structure check --target-backend TARGET` for the configured target;
- future `structure check --compat-targets pyspark,polars,duckdb` for a portability report;
- `StructureTools.compatibility.check(...)` for notebooks, tests, and build plugins that prefer a Python API.

Reports should classify every relevant feature as:

```text
supported
unsupported
degraded
opaque
unknown
```

`unsupported` fails the active target. `degraded` warns when semantics are preserved but performance, ordering,
nullability precision, type precision, or streaming behavior is weaker. `opaque` identifies hooks and other user-owned
runtime code the compiler cannot inspect.

## Fail-Fast Rules

For the active target, Structure should fail before online execution or generation when:

- no capability profile exists for `target_backend`;
- the IR contains an operation the profile does not support;
- the target supports the operation only through a prohibited fallback such as UDF, row-wise iteration, `collect`, or
  local materialization;
- the configured execution mode is unavailable for the target;
- a hook would be invoked against a backend outside the hook's effective target set;
- validation or schema projection cannot be expressed for the target.

Silent fallback is not allowed. A target adapter that cannot preserve Structure semantics must say so through a
diagnostic.

## Hooks

Hooks become target-scoped:

```python
@after(normalize, lane=orders, target_backend="pyspark")
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Unmarked hooks receive an effective target set from configuration. The compatibility-friendly default is:

```toml
[tool.structure]
hook_target_default = ["pyspark"]
```

That default preserves today's PySpark hook behavior while keeping future non-PySpark checks honest. Projects that
want stricter portability may set a future `hook_target_default = "explicit"` mode so every hook must declare
`target_backend`.

Hook compatibility diagnostics should be conservative:

- warn when an unmarked hook inherits a default while checking more than one target;
- warn when a hook imports or names a backend library outside its declared target set;
- fail when a hook would be active for a backend whose hook ABI cannot call it safely;
- show hook boundaries in explain and traceability as target-specific opaque operations.

## Backend Notes

### Spark SQL

Spark SQL should be explored with DuckDB because both raise SQL lowering questions: expression rendering, relation
binding, temp view naming, schema validation, and explain output. Spark SQL remains a PySpark-family target because
Structure would drive it through Python `SparkSession` APIs, not generated non-Python code.

Spark SQL may be useful when SQL text is easier to review than chained DataFrame calls or when teams already inspect
Spark SQL plans. It should still consume the same checked IR and the same PySpark-family capability profile.

### Type-Safe Python Dataset/DataFrame Patterns

Non-Python Spark Dataset support is out of scope. The useful question for Structure is whether Python applications can
get a Dataset-like authoring experience through stronger typing around PySpark DataFrames: generated protocols, typed
row proxies, schema-aware wrappers, or mypy/IDE-friendly helpers.

This target should be treated as a PySpark-family design exploration, not as a separate non-Python backend.

### Pandas

Pandas is useful for local tests, examples, and small jobs, but it is eager and not optimizer-backed. The adapter must
classify operations that require global materialization as degraded or unsupported. Pandas should not become a backdoor
for row-wise Python logic in compiled Structure code.

### Polars

Polars LazyFrame is a strong candidate because it is expression-oriented, lazy, and has clear projection/filter/join
semantics. It can validate whether the IR is genuinely backend-neutral without requiring a cluster.

### DuckDB

DuckDB is a strong candidate for Python-hosted SQL or relation API output. The adapter would need a SQL expression
renderer, type mapper, and explicit handling for table binding, relation names, and schema validation.

### Ibis

Ibis may be useful as a meta-backend, but it adds a second abstraction layer. Structure should first prove PySpark,
Polars, and DuckDB targets so the project understands its own backend contract before delegating portability to Ibis.
After Ibis support exists, other targets should normally come through Ibis rather than direct Structure adapters.

## Consequences

The project keeps PySpark excellent while preventing PySpark from hardening into the compiler core. The cost is
discipline: every new DSL feature needs generic IR, generic validation, capability requirements, target adapter tests,
and diagnostics before it is claimed portable.

This is the right pressure. Backend portability should be earned by explicit contracts, not by hoping similar DataFrame
APIs behave the same.
