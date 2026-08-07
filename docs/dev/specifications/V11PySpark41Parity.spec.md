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
with explicit scalar input/output types are candidates for `supported`. `Column.transform` is a candidate for a typed
higher-order array transformation: the callback receives a symbolic element and must return one symbolic element with a
declared result type. Random functions such as `random`, `uniform`, `randstr`, and `uuid` require an explicit seed
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

## Feature 4: Arrow UDF/UDTF and row-based transformWithState

Arrow UDF and UDTF decorators, vectorized UDF execution, and row-based `transformWithState` are explicit boundaries.
The specification must document why arbitrary Python execution, user-owned state, checkpoint/retry behavior, and
streaming timeouts do not fit the current compiler contract. A future implementation may promote a narrow typed slice,
but only after defining serialization, worker failure, state schema, initialization, timeout, recovery, and Connect
behavior. V11's default acceptance is a stable design gate and actionable diagnostic, not a partial implementation.

## Feature 5: target and evidence matrix

The matrix has six backends: `pyspark35`, `pyspark40`, `pyspark41`, `spark-connect35`, `spark-connect40`, and
`spark-connect41`. Each backend reports the exact PySpark and Spark version, target profile, target variant, image digest
or pinned package version, and test selection. The 4.1 profile is `>=4.1,<4.2`. Ordinary 4.1 is release-blocking for
every supported row; Connect 4.1 is release-blocking only for rows whose catalog entry claims Connect support.

The complete matrix runs through `make integration`; a selected lane runs through
`make integration BACKEND=pyspark41` or `make integration BACKEND=spark-connect41`. The Spark-free `make build` remains
mandatory and must not require Docker, Java, or an installed PySpark package.

## Cross-cutting requirements

Every supported row has a machine-readable inventory entry, a capability key, a stable diagnostic for unsupported
profiles or variants, a public API reference entry, a catalog entry, online/generated parity tests, and live evidence.
Every design gate names the missing contract, owner boundary, and supported caller remedy. Streaming classifications are
explicit. Generated code contains no lifecycle, arbitrary UDF/UDTF, or state-store ownership unless a later approved
specification changes that rule.
