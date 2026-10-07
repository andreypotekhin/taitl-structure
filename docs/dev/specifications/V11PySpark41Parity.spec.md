# V11 PySpark 4.1 Parity Specification

## Scope and status vocabulary

This specification covers PySpark 4.1 APIs that could affect Structure's typed DataFrame transformation contract. It does
not make a support claim by listing an upstream API. `supported` means the API has a complete Structure contract and
positive evidence. `planned` means implementation is intended but incomplete. `design-gated` means the contract is not
safe to implement yet. `caller-owned-guided` means callers may use the upstream API around a Structure transform.
`streaming-ineligible` means the operation may be valid for batch but cannot be used in Structure's streaming contract.
`unsupported` means Structure deliberately does not model it.

## Feature 1: 4.1 expressions and Column transformation

The implementation inventory compares the PySpark 4.0 and 4.1 Python references and records every newly added or
signature-changed row-preserving function. Deterministic numeric, string, binary, temporal, and collection functions
with explicit scalar input/output types are candidates for `supported`. `Column.transform` has a typed
whole-expression implementation for ordinary PySpark 4.1: the callback receives the complete symbolic expression and
returns one symbolic expression. The result may have a different type and nullability from the input. Its public status
remains `planned` until ordinary 4.1 live online/generated evidence passes; Spark Connect remains separately gated.
This is distinct from array
`functions.transform`, represented by Structure's `arr_transform(...)`, whose callback receives an array element.
Random functions such as `random`, `uniform`, `randstr`, and `uuid` require an explicit seed
policy; without one they are `design-gated` or `streaming-ineligible` rather than silently treated as deterministic.

Acceptance requires schema/type inference, nullability tests, compiler capability diagnostics, online and generated
ordinary-PySpark parity, generated-source inspection, and Connect evidence for each row claimed in both variants.

## Feature 2: relational query operations

`DataFrame.exists` and the IN-subquery addition are represented as boolean relation predicates with named correlation
scope. The compiler must reject accidental outer-column capture, ambiguous aliases, and unsupported multi-row scalar
assumptions. `lateralJoin` is admitted only with an explicit row-cardinality and output-schema contract. A raw Python
function returning a DataFrame is not compiler-visible and remains caller-owned unless a future typed relation-lambda
design is approved.

Acceptance requires positive correlated and uncorrelated cases, empty and duplicate right-side cases, null behavior,
alias collisions, explain/traceability output, ordinary and generated parity, and Connect-specific tests where the
upstream API supports Connect.

## Feature 3: observations and approximate sketches

Observation metrics are not silently added to a transform's row schema. The design must choose between a typed metric
channel, a caller-owned observation hook, or an explicit unsupported status. Complex metric values must define allowed
types, serialization, retrieval timing, and batch/streaming behavior. KLL and Theta sketch aggregates must define their
binary result type, merge semantics, optional dependency behavior, determinism, and whether users can consume the
result inside a Structure schema. Until those decisions are implemented and tested, the rows remain `design-gated`.

Acceptance is a documented positive or negative contract, with no misleading support claim and with runtime diagnostics
that point to the caller-owned alternative when applicable.

## Feature 4: Arrow UDF/UDTF and state processors

Arrow UDF and UDTF decorators and general vectorized callbacks remain caller-owned boundaries. The explicit state
processor surfaces are separate families: row-based `transform_with_state(...)` targets ordinary PySpark 4.1;
`transform_with_state_in_pandas(...)` targets ordinary PySpark 4.0 and 4.1; and legacy
`apply_in_pandas_with_state(...)` targets ordinary PySpark 3.5, 4.0, and 4.1. Each has its own compiler path,
target-profile rows, live evidence, and checkpoint-restart gate. The row family is governed by the dedicated
[admission and typed-parity plan](../planning/past/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md),
which admits Append and Update output modes and rejects Complete during compilation. Spark Connect and
Dataset/Scala arbitrary-state APIs remain outside the row claim. The row API supports Append and Update and the None,
ProcessingTime, and EventTime modes in their valid combinations; Complete is rejected during compilation. Ordinary
PySpark 4.1 online/generated evidence covers composite keys, both timer clocks, and same-checkpoint restart. The Pandas
and legacy Pandas rows remain `design-gated` until their own profiles pass their remaining live behavior, callback,
parity, and restart tests. The accepted typed row contract is recorded in `V11TransformWithStateTypedParity.design.md`.

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

The ordinary `pyspark41` integration lane initially runs the runtime-version assertion and the V11 test directory only.
Its image uses Protobuf 6.33.0 for PySpark 4.1's generated state protocol; 3.5 and 4.0 retain Protobuf 5.29.3. The Spark
test session uses RocksDB because the default HDFS-backed state store rejects TransformWithState's multiple column
families. Both 4.1 state processor APIs also require pandas, PyArrow, and Protobuf on the driver and workers; the Pandas
API requires those packages on PySpark 4.0 as well.
The row API's 4.1 fixture and the Pandas API's profile-specific fixtures are the relevant positive evidence; unrelated
pre-V11 integration and concept tests are not run on 4.1. The lane is not evidence of full upstream processor parity:
typed Structure callbacks cover the documented subset, while opaque native processors must be exercised for additional
native capabilities claimed by their profile.

## Feature 5: target and evidence matrix

The eventual release matrix has six backends: `pyspark35`, `pyspark40`, `pyspark41`, `spark-connect35`,
`spark-connect40`, and `spark-connect41`. General Connect 4.1 coverage remains deferred, while the separately
evidenced Delta Connect package is admitted only for its exact Delta rows. Each configured backend reports the exact
PySpark and Spark version, target profile, target variant, image digest or pinned package version, and test selection.
The 4.1 profile is `>=4.1,<4.2`. The initial ordinary 4.1 selection is limited to
the backend version check and V11 integration tests. Ordinary 4.1 is release-blocking for every supported row after its
profile-specific evidence is complete; Connect 4.1 is release-blocking only for rows whose catalog entry claims Connect
support.

The staged matrix runs through `make integration`; the configured 4.1 lane runs through
`make integration BACKEND=pyspark41`. Add general Connect 4.1 coverage only after a separate API and test selection is reviewed. The
Spark-free `make build` remains mandatory and must not require Docker, Java, or an installed PySpark package.

## Cross-cutting requirements

Every supported row has a machine-readable inventory entry, a capability key, a stable diagnostic for unsupported
profiles or variants, a public API reference entry, a catalog entry, online/generated parity tests, and live evidence.
Every design gate names the missing contract, owner boundary, and supported caller remedy. Streaming classifications are
explicit. Generated code contains no lifecycle, arbitrary UDF/UDTF, or state-store ownership unless a later approved
specification changes that rule.
