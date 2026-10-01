# PySpark SQL Boundary Contracts

## Purpose

This design records the decisions made while closing the PySpark `>=3.5,<4.1` SQL baseline gaps. It preserves useful
PySpark migration paths when Structure can state a typed, compiler-visible contract. It keeps the remaining behavior at
an explicit caller-owned boundary rather than admitting raw SQL, mutable runtime schemas, or untyped serialized state.

The owning execution sequence is
[P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md). The function-level status and migration
remedy remain in [PySpark SQL Baseline Gaps](../gaps/PySpark-SQL-baseline.gaps.md).

## Query-clock expressions

Structure will provide typed candidates for `current_date`, `curdate`, `current_timestamp`, `now`, `localtimestamp`,
and `current_timezone`. The first five are non-null query-clock expressions. Their IR records both
`nondeterministic=True` and `query_stable=True`: repeated calls in one evaluated query have the same value, but no
result is promised to repeat across query evaluations, retries, or streaming micro-batches. `current_timezone` instead
is a non-null read of session configuration.

These expressions preserve row cardinality and participate wherever the existing streaming classifier permits scalar
expressions. Tests must prove types, same-query equality, generated spelling, ordinary PySpark and Spark Connect
behavior, and the absence of a cross-query reproducibility claim.

## AES-GCM equivalent

Structure will provide `aes_encrypt`, `aes_decrypt`, and `try_aes_decrypt` as an intentionally narrower typed
equivalent to PySpark AES. It supports AES-GCM only. The public form accepts typed String or Binary payload, key, and
AAD expressions; ciphertext and plaintext results are Binary. A Python key literal is rejected, while nullable symbolic
keys preserve Spark's nullable data-flow behavior.

`aes_encrypt(..., key=..., aad=None, iv=None)` permits an optional Binary IV expression for deterministic protocols
and external interoperability. When omitted, Spark generates and prefixes the GCM nonce. When supplied, compilation
emits a stable crypto warning explaining that the caller owns nonce uniqueness for every key and should omit `iv` unless
an external protocol requires it. The compiler cannot prove runtime key or IV lengths.

The strict decrypt helper preserves Spark failure behavior. `try_aes_decrypt` returns nullable Binary when decryption or
authentication fails. CBC, ECB, padding selection, Python key literals, and arbitrary `ColumnOrName` coercions remain
native-PySpark remedies. Structure does not own secret resolution, storage, rotation, access policy, or audit logging.

## Runtime JSON schema inference

Structure does not infer a runtime JSON schema into a compiled row scope or output Schema. `from_json` and `json_tuple`
remain the typed alternatives: their result Schema and generated field names are declared before execution.
`schema_of_json` remains a literal inspection helper; its DDL String does not create a symbolic output Schema.

Callers that need data-driven inference, drift handling, or dynamic fields use native PySpark at an explicit hook or
integration boundary. They own the inferred schema, evolution policy, and return contract on the far side of that
boundary.

## Opaque sketches and bitmaps

Sketch state is a first-class Structure value, never ordinary `Binary`. A declared Schema field can carry an opaque HLL
or Bitmap value while Spark physically represents it as Binary. A raw Binary value needs an explicit import into the
appropriate opaque kind and is validated only by the target runtime when consumed. Sketch values cannot be cast to
ordinary Binary or another sketch type.

The default baseline exposes HLL construction, pairwise and aggregate union, and estimate, plus Bitmap
position/bucket helpers, construction, aggregate OR, and count. HLL union rejects mixed `lgConfigK` precision by
default. A literal opt-in permits Spark's mixed-precision union and emits a warning that precision may be reduced.
Sketch aggregates follow the existing grouped-aggregate streaming rules and state warnings, subject to dedicated live
evidence for each claimed profile.

Persisted opaque state is Spark/profile-specific rather than an interchange format. Documentation and diagnostics must
require users to pin the compatible Spark and provider profile. Count-Min remains caller-owned because its baseline SQL
surface produces bytes without a typed SQL consumer. KLL and Theta reuse this opaque-type model only on the PySpark 4.1
profile; they are not default-baseline support. Observation metrics are a separate future side-channel design, not
regular transform output fields.

## Evidence and diagnostics

No boundary decision alone promotes an API to implemented. Each candidate needs a capability key, typed symbolic
contract, generated rendering, online/generated parity, target-specific ordinary PySpark and Spark Connect evidence,
and a streaming classification. Diagnostics must state the unsupported form, the safe Structure alternative when one
exists, and the native-PySpark remedy otherwise. No diagnostic or generated source may expose key literals or other
secret material.
