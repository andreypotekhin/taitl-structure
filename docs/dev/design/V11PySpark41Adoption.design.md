# V11 PySpark 4.1 Adoption Design

## Design intent

PySpark 4.1 is a versioned target profile, not a reason to expose an untyped wrapper around every new Spark API.
Structure remains an intermediate-representation-first library: source operations are captured symbolically, checked
against declared schemas and capabilities, and rendered into ordinary PySpark or Spark Connect code. The design keeps
the user-visible promise small enough to verify and keeps opaque execution at an explicit boundary.

The upstream 4.1 release adds a large SQL-function surface, a Column transformation API, IN-subquery and lateral-join
query APIs, richer observations, Python Arrow UDF/UDTF facilities, row-based streaming state, and several APIs outside
Structure's transformation boundary. The upstream release notes and 4.1 Python reference are the inventory inputs:
`https://spark.apache.org/releases/spark-release-4-1-0.html` and
`https://spark.apache.org/docs/4.1.0/api/python/reference/`.

## Target variants

The ordinary PySpark adapter is the primary implementation and evidence target because it exercises the complete
classic DataFrame runtime and is the least ambiguous path for new Python APIs. Spark Connect is a second variant, not an
implicit consequence of ordinary support. Each feature records whether its PySpark 4.1 documentation supports Connect,
whether generated code avoids classic-only objects, and whether a live Connect test passes.

The implementation uses the exact profile `>=4.1,<4.2` during adoption. Existing `>=3.5,<4.1` behavior remains intact
and is tested as a regression. A broad default profile is promoted only after the closeout gate.

## Feature admission rule

An API is admitted only if Structure can state its input types, output type, nullability, row cardinality, determinism,
streaming classification, target-variant support, generated spelling, online evaluator behavior, diagnostics, and
traceability. If any of those are unknown, the catalog records a design gate and names the caller-owned remedy.

## Work packages

The expression package handles row-preserving APIs. It gives each admitted function a typed expression node or a
reusable existing node, defines nullability and deterministic-seed rules, and rejects random or opaque calls when the
source does not provide an explicit policy.

The query package handles operations that change relation shape or introduce a nested query. `exists` is represented as
a boolean relation predicate with declared correlation scope. `lateralJoin` is admitted only if the relation and
cardinality contract can be expressed without a raw DataFrame callback; otherwise it remains a caller-owned hook.

The observations-and-sketches package separates row output from metrics and opaque binary sketches. Complex observation
values may be recorded as a runtime capability or remain outside the compiler-visible transform contract. KLL and Theta
sketches need explicit binary-type, mergeability, dependency, and deterministic-output contracts before support.

The Python-and-streaming package records Arrow UDF/UDTF and row-based `transformWithState` as design-gated. These APIs
execute user Python or own state and retries, so no generated Structure support is claimed merely because PySpark 4.1
exposes them. The package specifies the boundary, diagnostics, and a future promotion test.

The infrastructure package adds versioned Compose images, ordinary and Connect runners, backend metadata, capability
profiles, and evidence reports. It must permit one backend to be selected without starting unrelated Spark services.

## Rejected shortcuts

Do not map unknown 4.1 functions through a generic `call_function` escape hatch and call that parity. Do not infer
Connect support from ordinary Spark success. Do not make a Python callback symbolic by executing it during compilation.
Do not put streaming state or lifecycle ownership into generated modules. Do not drop 3.5 or 4.0 lanes when adding 4.1.
