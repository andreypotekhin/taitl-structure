# Backend Capability Interface

Structure's compiler is IR-first, but IR does not remove the need for target knowledge. A projection, join, validation
step, or streaming-compatible operation can be valid Structure IR while still being unsupported by a concrete backend
or PySpark target range.

This design resolves C23 by making backend capability checks a named boundary. The same boundary is also the admission
point for future alternative backends described in [AlternativeBackends.md](AlternativeBackends.md).

## Problem

Without one capability interface, target behavior drifts into many places:

- symbolic execution starts knowing PySpark details;
- compileability checks accumulate version-specific conditions;
- online execution and generated code make separate syntax choices;
- streaming compatibility repeats backend support rules;
- unsupported features produce inconsistent diagnostics.

That drift would make v2 analytical features, Spark Connect, and future alternate backends expensive. It would also
weaken the no-Spark compiler contract because code might be tempted to probe an installed PySpark runtime.

## Design

Backend support is represented by a small static object selected from resolved configuration:

```text
BackendCapabilities
  id
  imports()
  supports(requirement)
  require(requirement)
```

`supports(...)` answers a question without raising. `require(...)` uses the same decision and raises the standard
backend diagnostic when unsupported.

The input to both methods is a `CapabilityRequirement`:

```text
CapabilityRequirement
  group
  name
  source
  docs
```

Groups are intentionally broad:

- `backend`
- `expression`
- `join`
- `validation`
- `streaming`
- `imports`

Future alternative backends may add `runtime`, `output`, `hook`, and `type` groups when those requirements are needed
to distinguish online execution, generated output mode, target-scoped hook support, and backend type limits.

The current implementation has one concrete target profile: ordinary PySpark for `target_pyspark = ">=3.5,<4.1"`.
The profile is static source data. Selecting it must not import `pyspark`, start Java, create a Spark session, or touch
a Spark cluster. Future profiles must follow the same no-runtime-import rule for Spark SQL, typed PySpark DataFrame
patterns, Pandas, Polars, DuckDB, Spark Connect, Ibis, or other backend runtimes.

## Current Capability Shape

The default PySpark profile supports:

- field references, literals, projection, filtering, boolean operations, equality, null-safe equality, casts, and
  standard expression helper calls;
- `join_one`, left and inner lookup joins, composite equi-joins, broadcast hints, existence joins, `join_many(...)`, and
  deterministic lookup dedupe;
- schema-only validation, strict projection, and allow-extra projection;
- row-local projection and filtering for streaming-compatible transforms;
- stream-static left and inner lookup joins;
- deterministic generated PySpark import names.

Features outside that set are unsupported until their specifications promote them. Examples include temporal joins,
as-of joins, aggregations, broad window helpers, stream-stream joins, streaming orchestration, and Spark Connect.

## Diagnostics

Unsupported backend work uses the diagnostics component prefix reserved by the diagnostics contract:

```text
BACKEND-E2401  unsupported backend target
BACKEND-E2402  unsupported backend capability
```

Every backend diagnostic includes:

- target backend;
- target range;
- feature group;
- feature name;
- problem;
- why the capability is unavailable;
- suggested fix;
- documentation link.

This replaces older `STRUCT-E...` examples with the current registry-backed code format.

## Consequences

New DSL operations must declare backend capability behavior before they are considered supported. Compiler checks,
online lowering, generated code, streaming classification, and future explain output should all ask the same capability
object instead of owning their own backend support rules.

Spark Connect remains v4 work. It may arrive earlier only if it can be expressed as another backend capability profile
without changing public DSL syntax, generated class construction, `run(...)` signatures, or streaming orchestration
semantics.
