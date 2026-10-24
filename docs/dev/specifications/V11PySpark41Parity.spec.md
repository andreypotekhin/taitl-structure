# V11 PySpark 4.1 Parity Specification

## Scope and status vocabulary

This specification covers PySpark 4.1 APIs that could affect Structure's typed DataFrame transformation contract. It does
not make a support claim by listing an upstream API. `supported` means the API has a complete Structure contract and
positive evidence. `planned` means implementation is intended but incomplete. `design-gated` means the design,
implementation, or required runtime evidence remains incomplete. `caller-owned-guided` means callers may use the
upstream API around a Structure transform.
`streaming-ineligible` means the operation may be valid for batch but cannot be used in Structure's streaming contract.
`unsupported` means Structure deliberately does not model it.

## Feature 1: 4.1 expressions and Column transformation

The [versioned inventory](../../../src/structure/plugin/pyspark/resources/pyspark-4.1-python-api-delta.json) compares
the pinned PySpark 4.0 and 4.1 Python indexes. It records 43 new function-index names and one new SQL-index symbol;
`Column.isin(DataFrame)` and complex-valued observations are separately recorded semantic extensions. Index addition
does not necessarily mean first implementation: the 4.1 pages for `random` and `try_to_date` report earlier versions.
String and temporal expressions need scalar type and nullability contracts. Geospatial functions and KLL/Theta sketches
have separate typed-value contracts. `Column.transform` has a typed
whole-expression implementation for PySpark 4.1: the callback receives the complete symbolic expression and returns
one symbolic expression. The result may have a different type and nullability from the input. Ordinary PySpark and
Spark Connect 4.1 live online/generated evidence passes, so this surface is `supported` for `>=4.1,<4.2` on both
variants. Streaming compatibility continues to be determined by the returned expression and its existing
streaming checks. This is distinct from array `functions.transform`, represented by Structure's `arr_transform(...)`,
whose callback receives an array element. Random functions such as `random`, `uniform`, `randstr`, and `uuid` require an
explicit seed policy; without one they are `design-gated` or `streaming-ineligible` rather than silently treated as
deterministic.

Acceptance requires schema/type inference, nullability tests, compiler capability diagnostics, online and generated
ordinary-PySpark parity, generated-source inspection, and Connect evidence for each row claimed in both variants.

The first scalar admission is `chr`, `quote`, `try_to_date`, `random`, and `uuid`, on exact `>=4.1,<4.2` ordinary and
Connect targets. The expression design's [scalar contract](../design/V11PySpark41ExpressionParity.design.md#admitted-scalar-contract)
defines operands, results, nullability, seed policy, generated spelling, and streaming classification. Each helper
requires its own `expression.<name>` capability. Parse-failure nullability must survive source non-null narrowing.
Random/UUID recipes remain nondeterministic even when seeded and report `STREAM-E0801` for streaming use anywhere
within the step. The approved TIME contract adds `time(precision=6)`, public `Time`/`TimeType` exports, and six typed
helpers: `current_time`, `make_time`, `to_time`, `try_to_time`, `time_diff`, and `time_trunc`. Precision is an integer
from 0 through 6 and participates in schema identity. Python `datetime.time` values follow Spark's clock-field
conversion, ignoring timezone metadata. Comparisons require matching TIME precision; ordering accepts any TIME
precision. Casts are restricted to String and TIME, and TIME arithmetic is excluded. Formats and units may be typed
String expressions. The Spark feature flag `spark.sql.timeType.enabled` remains caller-configured on the Spark
session (for example, through `SparkSession.builder.config(...)`); it is not a Structure transform option. Native
Spark failures surface when disabled.
Admission requires the exact `>=4.1,<4.2` ordinary and Connect variants, Spark-free gate/schema checks, generated
schema inspection, and online/generated runtime parity. No streaming claim is implied by batch evidence.

## Feature 2: relational query operations

The approved API and semantics are specified in
[V11PySpark41QueryOperations.design.md](../design/V11PySpark41QueryOperations.design.md). An additional
`InputScope` builds an immutable typed inner query with `.where()`/`.filter()`, `.project()`, `.order_by()`, and
ordered `.limit()` operations. The supported query consumers are batch-only and admitted independently for exact
ordinary and Connect `>=4.1,<4.2` profiles; both variants have positive native/online/generated parity evidence.

`.exists()` returns a native-shaped Boolean expression that may be projected, filtered, negated, or combined. Existing
`exists(..., on=...)` and `not_exists(..., on=...)` remain filtering semi/anti joins. `.isin(relation_or_query)` adds
uncorrelated native IN-subquery behavior while preserving existing literal/list `isin(...)`; scalar values compare to
one output column and struct values compare to projected columns in order. Spark three-valued null semantics are
preserved. Correlated inner queries and lateral joins mark visible outer fields with `.outer()`.

`lateral_join(...)` supports prepared and inferred typed queries, inner and left joins, a separate post-query `on=`
condition, and an explicit `allow_cartesian=True` requirement when no correlation or connecting condition exists.
Query operations are filtering, projection, ordering, and limits; query-local joins, aggregates/windows, nested
queries, correlated IN, and arbitrary DataFrame callbacks remain unsupported. A raw Python function returning a
DataFrame is caller-owned.

Acceptance requires online and generated ordinary/Connect parity for correlated and uncorrelated cases, empty and
duplicate right inputs, SQL null behavior, ordered limits with post-limit `on`, output schema/nullability,
alias/scope diagnostics, explain/traceability, and generated-source safety checks.

Evidence: both focused query integration cases pass on ordinary PySpark/Spark 4.1.0 and Spark Connect/PySpark/Spark
4.1.0, including empty IN-subquery behavior. Spark-free query contract and user-story tests pass. The default target
profile is `>=3.5,<4.2`; exact-target-only features remain capability-gated.

## Feature 3: observations and approximate sketches

Typed batch observations follow the contract in the
[observation design](../design/V11PySpark41ObservationsAndSketches.design.md#observation-boundary) and its
[completed implementation plan](../past/planning/P10182602.V11-batch-observations.plan.md). A transform declares a named metric
Schema with `observation(Schema)`, binds it once with `observe(Schema(...))` or
`observe(self.declaration, Schema(...))`, and exposes a typed handle beside its row outputs. `get()` adds no Spark
action; the caller first performs an action on a returned path that traverses the observation. Metric nullability
follows global aggregate semantics, including nullable `sum` on empty input. Observations preserve native ordinary and
Connect lifecycle behavior, are batch-only, and follow stage-output visibility. Native/online/generated parity passed
for values, schema, empty input, conditional counts, collection snapshots, and Connect's pre-action state on ordinary
and Connect PySpark/Spark 4.1.0. The exact `>=4.1,<4.2` variants are admitted independently; earlier and later profiles
remain gated. See the [observation ExecPlan](../past/planning/P10182602.V11-batch-observations.plan.md).

KLL and Theta sketches are implemented as opaque Spark Binary values with recursive logical metadata. Typed KLL
construction, query, merge, and rendering helpers and typed Theta construction, set operations, and estimates are
admitted for exact `>=4.1,<4.2` ordinary and Connect batch profiles. Online/generated/native parity, schema metadata,
and Parquet readback pass on both variants. Ordinary PySpark 4.1 additionally passes watermarked aggregate and
same-configuration checkpoint-restart parity. Connect streaming remains outside the backend contract. See the
[KLL/Theta implementation ExecPlan](../planning/P10182603.V11-kll-theta-sketches.plan.md) for detailed evidence and
native edge behavior.

## Feature 4: Arrow UDF/UDTF and state processors

The general [UDF specification](UDF.spec.md) admits typed scalar, scalar-iterator, grouped-aggregate,
and analytic-window callbacks on exact ordinary and Connect `>=4.1,<4.2` profiles after online/generated parity. Ordinary
scalar and iterator streaming pass processed-data and same-checkpoint restart parity; grouped/analytic streaming and
Connect streaming remain excluded. Arrow UDTF and general vectorized callbacks remain caller-owned boundaries. The explicit state
processor surfaces are separate families: row-based `transform_with_state(...)` targets ordinary PySpark 4.1;
`transform_with_state_in_pandas(...)` targets ordinary PySpark 4.0 and 4.1; and legacy
`apply_in_pandas_with_state(...)` targets ordinary PySpark 3.5, 4.0, and 4.1. Each has its own compiler path,
target-profile rows, live evidence, and checkpoint-restart gate. The row family is governed by the dedicated
[admission and typed-parity plan](../past/planning/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md),
which admits Append and Update output modes and rejects Complete during compilation. Spark Connect and
Dataset/Scala arbitrary-state APIs remain outside the row claim. The row API supports Append and Update and the None,
ProcessingTime, and EventTime modes in their valid combinations; Complete is rejected during compilation. Ordinary
PySpark 4.1 online/generated evidence covers composite keys, both timer clocks, and same-checkpoint restart. Pandas
`transform_with_state_in_pandas(...)` is supported on ordinary PySpark 4.0 and 4.1 after both complete lanes passed;
see its [completed ExecPlan](../past/planning/P10042604.V11-transform-with-state-in-pandas.plan.md). Legacy
`apply_in_pandas_with_state(...)` remains `design-gated` pending its own profile-specific callback and parity evidence.
The accepted typed row contract is recorded in `V11TransformWithStateTypedParity.design.md`.

In typed row mode, `on_rows(self, key, rows, timers)` is required, `on_timer(self, key, timer, timers)` is optional,
and `on_initial_state(self, key, initial, timers)` is optional when paired with an `initial_state=` relation.
The callback context exposes current processing time and current watermark in milliseconds along with timer management;
reading the watermark requires a watermarked input. Callback signatures are checked before constructing the Spark
operator. Converted input and state values expose the declared Structure fields, and yielded values must match the
declared output Schema, including non-null constraints. A callback may yield zero or many rows.

The typed row state surface uses named `ValueState`, `ListState`, and `MapState` attributes. It includes optional TTL
with ProcessingTime and typed initial-state callbacks paired with an initial relation. The opaque native path preserves
Spark processor methods and Python constructs outside the typed contract. Composite keys are supported by the row
operation. The Pandas and legacy families do not inherit the row family's status or evidence.
Typed `close` callbacks and schema evolution are deferred because cleanup is not guaranteed after worker failure and
checkpoint migration needs its own contract.

The ordinary `pyspark41` integration lane runs the runtime-version assertion and the V11 test directory only.
Its image uses Protobuf 6.33.0 for PySpark 4.1's generated state protocol; 3.5 and 4.0 retain Protobuf 5.29.3. The Spark
test session uses RocksDB because the default HDFS-backed state store rejects TransformWithState's multiple column
families. Both 4.1 state processor APIs also require pandas, PyArrow, and Protobuf on the driver and workers; the Pandas
API requires those packages on PySpark 4.0 as well.
The row API's 4.1 fixture and the Pandas API's profile-specific fixtures are the relevant positive evidence. The
ordinary lane does not select pre-V11 integration or concept tests; Connect 4.1 runs their full selection. Neither
selection establishes full upstream processor parity:
typed Structure callbacks cover the documented subset, while opaque native processors must be exercised for additional
native capabilities claimed by their profile.

## Feature 5: target and evidence matrix

The release matrix has six backends: `pyspark35`, `pyspark40`, `pyspark41`, `spark-connect35`,
`spark-connect40`, and `spark-connect41`. Connect 4.1 runs the full integration and live concept suites, with the
Iceberg and Delta providers kept in isolated passes. Those providers are admitted only for Connect 4.1 and excluded
from Connect 3.5/4.0 general selections. Reliable batch checkpointing is admitted separately on exact Connect 4.0 and
4.1; Connect 3.5 retains its checkpoint gate. Each configured backend reports the exact
PySpark and Spark version, target profile, target variant, image digest or pinned package version, and test selection.
The 4.1 profile is `>=4.1,<4.2`. The initial ordinary 4.1 selection is limited to
the backend version check and V11 integration tests. Ordinary 4.1 is release-blocking for every supported row after its
profile-specific evidence is complete; Connect 4.1 is release-blocking only for rows whose catalog entry claims Connect
support.

The staged matrix runs through `make integration`; run ordinary and Connect 4.1 through
`make integration BACKEND=pyspark41` and `make integration BACKEND=spark-connect41`. The Spark-free `make build` remains
mandatory and must not require Docker, Java, or an installed PySpark package.

## Cross-cutting requirements

Every supported row has a machine-readable inventory entry, a capability key, a stable diagnostic for unsupported
profiles or variants, a public API reference entry, a catalog entry, online/generated parity tests, and live evidence.
Every design gate names the missing contract, owner boundary, and supported caller remedy. Streaming classifications are
explicit. Generated code contains no lifecycle, arbitrary UDF/UDTF, or state-store ownership unless a later approved
specification changes that rule.
