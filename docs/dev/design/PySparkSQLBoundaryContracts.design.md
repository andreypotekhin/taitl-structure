# PySpark SQL Boundary Contracts

## Purpose

This design records the decisions made while closing the PySpark `>=3.5,<4.1` SQL baseline gaps. It preserves useful
PySpark migration paths when Structure can state a typed, compiler-visible contract. It keeps the remaining behavior at
an explicit caller-owned boundary rather than admitting raw scalar SQL, mutable runtime schemas, or untyped serialized
state. The separately designed [Typed SQL Execution](TypedSqlExecution.design.md) feature is a full-relation operation
with a declared schema; it does not change the exclusion of raw scalar SQL expressions.

The owning execution sequence is
[P09302601](../planning/past/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md). The function-level status and migration
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

## Timestamp-without-time-zone conversion

`timestamp()` and `timestamp_ntz()` are distinct schema types. The former represents Spark's instant-based
`TimestampType`; the latter represents wall-clock `TimestampNTZType`. They do not compare or assign across types, and
Python `datetime` annotations continue to mean `timestamp()` unless a schema explicitly declares NTZ.

`convert_timezone(source_tz, target_tz, source_ts)` mirrors PySpark: `source_ts` must be TimestampNTZ, and each zone
may be a typed String expression. `source_tz=None` preserves PySpark's session-time-zone default. The result is
TimestampNTZ and nullable when any input is nullable. The operation is a stateless row-local expression, including in
streaming transforms. It does not silently reinterpret instant-based Timestamp values; callers must choose an explicit
conversion boundary for LTZ/NTZ changes. Runtime profile evidence remains a release check.

## Calendar-date construction

`make_date(year, month, day)` accepts compiler-visible Integer or Long expressions and always returns a nullable Date.
It does not validate calendar ranges at authoring time: Spark's `spark.sql.ansi.enabled` setting controls whether an
invalid date produces NULL or fails at execution. The operation is stateless and row-local; Structure neither reads nor
changes the Spark SQL ANSI setting.

## NTZ parsing

`to_timestamp_ntz(value, format=None)` accepts typed String expressions for the input and optional datetime pattern.
It has a stable TimestampNTZ result independent of `spark.sql.timestampType`; invalid text follows Spark's ANSI
policy, while `try_to_timestamp` is the nullable-on-invalid parser. The optional pattern may vary per row, matching
PySpark's typed `ColumnOrName` parameter. This is a
stateless row-local expression. The generic `to_timestamp` and `try_to_timestamp` result type remains separate because
PySpark selects its timestamp family from session configuration.

## Temporal and interval baseline

`Temporal` and `Interval` are string-valued constants, not a closed input vocabulary. `Temporal.DAY_OF_YEAR` is
`"doy"`; `"dayofyear"` remains an accepted migration alias. `extract`, `date_part`, and `datepart` accept a
compiler-visible String literal or constant, never a row-dependent field name. They accept typed temporal and
interval sources, returning Integer parts except Decimal(8,6) seconds.

Generic `make_timestamp`, `to_timestamp`, and `try_to_timestamp` resolve `spark.sql.timestampType` before symbolic
authoring. The setting and `spark.sql.legacy.interval.enabled` enter the compiler fingerprint; a live session reads
them from Spark, and an explicit Structure override must match. Offline compilation defaults to LTZ and nonlegacy
intervals. Explicit LTZ/NTZ constructors and parsers ignore the generic timestamp setting. `to_timestamp` and
`unix_timestamp` accept typed String patterns; their no-input query-clock forms retain literal patterns. A pattern on
already typed Date/LTZ inputs warns, as does a generic NTZ constructor given a time zone.

`types.interval(...)` selects one exact qualifier via `type=` or `unit=`. `interval(...)` requires exactly the
corresponding components and casts Spark's `make_ym_interval`/`make_dt_interval` result to that qualifier. The public
Spark constructors remain available. YearMonth and DayTime qualifiers can appear in Schema. Mixed Calendar intervals
remain expression-only on 3.5; 4.0 may declare them, subject to PySpark's Python Row conversion behavior. Live checks
confirm interval arithmetic and date shifts across classic and Connect targets. Adding a Calendar interval to a Date
returns Date. DayTime interval values convert to Python `timedelta`; YearMonth conversion varies by runtime (it failed
in PySpark 4.0 classic and Spark Connect Arrow conversion), while Calendar values fail conversion in both tested 4.0
paths. Keep interval values inside SQL expressions and collect a non-interval result. Arithmetic preserves interval
families and types date/timestamp subtraction as DayTime or Calendar according to the legacy setting.

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

The complete default-baseline implementation and documentation contract is maintained in
[Sketches and Bitmaps design](SketchBitmap.design.md). It separates the supported HLL/Bitmap surface from
the V11 design-gated KLL/Theta work without treating Spark Binary state as portable interchange data.

## Typed generators and relation distribution

`stack(rows, *values, to=StackRow, scope=...)` is the one additional typed generator shape. `StackRow` is a declared
Schema, not a runtime alias list: it fixes each output field's name, type, and nullability before Spark runs. The
generator multiplies each input row by `rows`; values are arranged row-major and each output field uses the common
Structure type of values in that position. As in PySpark, an incomplete final row is padded with NULL; therefore every
Schema field that might receive padding must be nullable. `stack` is a fixed stateless generator, not batch-only, and
is classified streaming-compatible while ordinary and Spark Connect runtime evidence remains a release check.

Generic generators stay caller-owned because Structure cannot infer their output schema, aliases, or cardinality from a
function spelling. They belong at a raw PySpark boundary with the caller declaring the result relation on the far side.

Structure may own relation distribution without owning writer layout. Relation coalescing is spelled
`coalesce(partitions=count)` with a keyword-only positive integer count at the current unshaped relation boundary. It is
row-preserving, makes no output-order or stable-partition-identity promise, and on streaming relations emits a
suppressible throughput-tuning advisory. Scalar `coalesce(value, fallback, *values)` requires at least two values; a
single value should be used directly. Hash repartitioning is `repartition(count, *keys)` or `repartition(*keys)`: a
leading integer always means count, while a constant integer key must be an explicit literal expression. It preserves
rows and schema, promises neither order nor stable partition identity, and on streaming relations emits a suppressible
throughput advisory. `repartition_by_range` remains batch-only. Writer
partition transforms, including `years`, `months`, `days`, `hours`, and `bucket`, remain caller-owned output-layout
policy.

## Variant mutation

The existing typed literal-path Variant mutations remain visible but target-gated. They are released only after a
concrete profile proves capability, symbolic typing, generated and online parity, classic and Connect behavior, and the
applicable streaming classification. The default `>=3.5,<4.1` baseline makes no Variant mutation claim.

## URL, XML, runtime, and opaque Python

`url_encode` and strict `url_decode` are typed row-local String helpers; strict decode retains Spark's malformed-input
failure behavior. `try_url_decode` is implemented as a PySpark 4.0 target gate. URL parsing remains separate because its
result shape needs its own typed contract. Live ordinary/Connect parity evidence remains a release check.

XML remains gated until one design owns declared input/output schemas, parser and serializer options, malformed-input
behavior, nullability, and target evidence together. XML sources and writers remain caller-owned.

Source, session, catalog, partition, and engine metadata, along with arbitrary reflection, remain caller-owned runtime
reads. Query-clock expressions are the narrow exception because their symbolic type, query stability, and
nondeterminism are declared. Pandas UDFs, UDTFs, arbitrary Python callbacks, and custom runtime types likewise remain
at an explicit raw boundary; existing scalar UDFs and compiler-visible expression callbacks are distinct supported
forms.

## Geospatial provider boundary

The full geospatial contract is maintained in [Geospatial design](Geospatial.design.md) and its execution sequence in
[P10012602](../planning/past/P10012602.Geospatial-provider-boundaries.plan.md). The default baseline does not include native
Spark Geometry/Geography or external provider support.

Future native PySpark 4.1+ APIs use familiar no-prefix `st_*` names. External providers use their own namespaces, such
as `sedona.st_geomfromwkt`, and must match an application, transform, or step `geo_provider` selection. Geometry and
Geography values retain dialect, kind, and fixed or mixed SRID facts; they cannot cross provider dialects directly.
An ordinary declared `binary()` field is the deliberate handoff boundary. WKB, EWKB, and any other Binary codec remain
caller-owned interoperability decisions rather than portable Structure guarantees.

## Evidence and diagnostics

No boundary decision alone promotes an API to implemented. Each candidate needs a capability key, typed symbolic
contract, generated rendering, online/generated parity, target-specific ordinary PySpark and Spark Connect evidence,
and a streaming classification. Diagnostics must state the unsupported form, the safe Structure alternative when one
exists, and the native-PySpark remedy otherwise. No diagnostic or generated source may expose key literals or other
secret material.
