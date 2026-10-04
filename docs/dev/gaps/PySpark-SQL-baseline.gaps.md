# PySpark SQL Baseline Gaps

This is the function-level migration gap register for the ordinary PySpark `>=3.5,<4.1` baseline. The baseline is the
intersection of the public PySpark 3.5.x and 4.0.x APIs. It complements the family summary in
[APITracker](../../compatibility/APITracker.md) and the public compatibility summary in
[APICompatibility.md](../../compatibility/APICompatibility.md).

The register targets PySpark parity for migration: preserve the PySpark name and semantics where Structure can own a
typed contract; record an explicit Structure equivalent where the public spelling must differ; and give every remaining
function a precise gate or caller remedy. It is not a promise to expose arbitrary SQL strings or Python callbacks.

## Status Vocabulary

| Status | Meaning |
| --- | --- |
| `implemented` | Structure owns a typed equivalent with required evidence. |
| `candidate` | The baseline function fits the typed model and is ready for implementation after audit. |
| `design-gated` | A type, schema, cardinality, time, state, security, or runtime contract is missing. |
| `target-gated` | Support depends on a PySpark, provider, or runtime profile outside the baseline. |
| `caller-owned-guided` | Migration remains possible through native PySpark at an explicit boundary. |
| `unsupported` | The API conflicts with the compiler-visible Structure contract. |

## Conditional, Assertion, Predicate, and Null Functions

These common functions already have typed Structure equivalents. They are listed individually here so their target
presence, public spelling, and migration contract are visible per function rather than inherited from a family summary.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `when(condition, value)` | yes | yes | `implemented` | `when(condition, value)` builds a typed conditional expression; chain `.when(...)` and finish with `.otherwise(...)` as needed. | Use `when(...)` with compiler-visible conditions and values. |
| `coalesce(*cols)` | yes | yes | `implemented` | Scalar `coalesce(*values)` requires at least two expressions; relation partition coalescing uses keyword-only `coalesce(partitions=...)`. | Use scalar `coalesce(a, b, ...)` or relation `coalesce(partitions=n)` according to the input type. |
| `nullif(col1, col2)` | yes | yes | `implemented` | `nullif(left, right)` returns the first typed value unless equal, and is nullable because a match yields null. | Use `nullif(...)` directly. |
| `nvl(col1, col2)` | yes | yes | `implemented` | `nvl(value, fallback)` is the exact-name null fallback. | Use `nvl(...)` directly. |
| `nvl2(col1, col2, col3)` | yes | yes | `implemented` | `nvl2(value, if_not_null, if_null)` preserves the typed branch result contract. | Use `nvl2(...)` with typed branch expressions. |
| `ifnull(col1, col2)` | yes | yes | `implemented` | `ifnull(value, fallback)` is the exact-name null fallback. | Use `ifnull(...)` directly. |
| `zeroifnull(col)` | no | yes | `implemented` | `zeroifnull(value)` preserves the input numeric type and substitutes zero only for null; the 3.5 renderer/evaluator use typed `coalesce(value, 0)` because PySpark added the named wrapper in 4.0.0. | Use Structure's typed helper on either target; native PySpark 3.5 can use `coalesce(col, lit(0).cast(col_type))`. |
| `assert_true(condition, errMsg=None)` | yes | yes | `implemented` | `assert_true(condition, message=...)` is a typed Boolean guard; use `.isNull()` when composing PySpark's null-on-success contract as a predicate. | Use `assert_true(...).isNull()` in a filter or guard. |
| `raise_error(errMsg)` | yes | yes | `implemented` | `raise_error(message)` is a typed error expression with a compiler-visible message. | Use `raise_error(...)` directly. |
| `equal_null(col1, col2)` | yes | yes | `implemented` | `equal_null(left, right)` returns non-null Boolean and treats two nulls as equal. | Use `equal_null(...)` when null-safe equality is required. |
| `like(str, pattern)` | yes | yes | `implemented` | `like(value, pattern)` accepts typed String operands and returns nullable Boolean. | Use `like(...)` with a literal or String expression pattern. |
| `ilike(str, pattern)` | yes | yes | `implemented` | `ilike(value, pattern)` accepts typed String operands and preserves Spark's case-insensitive matching semantics. | Use `ilike(...)` with a literal or String expression pattern. |
| `regexp(str, regexp)` | yes | yes | `implemented` | `regexp(value, pattern)` is the exact-name regular-expression predicate. | Use `regexp(...)` with a compiler-visible pattern. |
| `regexp_like(str, regexp)` | yes | yes | `implemented` | `regexp_like(value, pattern)` preserves the PySpark spelling and accepts typed String operands. | Use `regexp_like(...)` directly. |
| `rlike(str, regexp)` | yes | yes | `implemented` | `rlike(value, pattern)` is the regular-expression predicate alias. | Use `rlike(...)` directly. |
| `isnull(col)` | yes | yes | `implemented` | `isnull(value)` returns a non-null Boolean null test. | Use `isnull(...)` directly. |
| `isnotnull(col)` | yes | yes | `implemented` | `isnotnull(value)` returns a non-null Boolean null test. | Use `isnotnull(...)` directly. |
| `isnan(col)` | yes | yes | `implemented` | `isnan(value)` distinguishes floating NaN from SQL null. | Use `isnan(...)` directly. |
| `nanvl(col1, col2)` | yes | yes | `implemented` | `nanvl(value, replacement)` substitutes only for NaN; SQL null remains distinct. | Use `nanvl(...)` directly. |

The signatures and common target presence are listed in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) function indexes.

## Common Function-Index Exclusions

These names are present in both pinned PySpark function indexes but remain outside Structure's selected typed surface.
They are listed individually so the index census has a per-name disposition and a concrete caller-owned remedy.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `call_function(funcName, *cols)` | yes | yes | `unsupported` | Dynamic SQL function lookup bypasses the typed function contract. | Use a typed Structure function, or keep `call_function(...)` inside a native PySpark boundary. |
| `expr(str)` | yes | yes | `unsupported` | Arbitrary SQL text is outside compiler-visible expression ownership. | Use typed Structure expressions, or isolate `expr(...)` in native PySpark. |
| `col(col)` | yes | yes | `caller-owned-guided` | Dynamic name resolution is not Structure's declared-field model. | Refer to declared fields through the Structure transform input; use native `col(...)` at an explicit boundary when dynamic lookup is required. |
| `column(col)` | yes | yes | `caller-owned-guided` | Alias of dynamic column-name resolution, not a declared-field reference. | Refer to declared fields through the Structure transform input; use native `column(...)` at an explicit boundary when dynamic lookup is required. |
| `struct(*cols)` | yes | yes | `caller-owned-guided` | PySpark infers a result schema from runtime columns; Structure requires a declared `Schema`. | Declare the output `Schema` and construct typed fields; retain native `struct(...)` where inference is intentional. |
| `call_udf(udfName, *cols)` | yes | yes | `caller-owned-guided` | Registered-function lookup depends on session state and an external result contract. | Register and invoke through native PySpark at a boundary with an explicitly declared output schema. |
| `pandas_udf(f=None, returnType=None, functionType=None)` | yes | yes | `caller-owned-guided` | Python callback execution, Arrow behavior, and result typing are runtime-owned. | Use native PySpark with an explicit return type and keep the callback at a declared boundary. |
| `udf(f=None, returnType=StringType(), *, useArrow=None)` | yes | yes | `caller-owned-guided` | Python callback execution and Arrow selection do not have a compiler-visible contract. | Use native PySpark with an explicit return type at a declared boundary. |
| `udtf(cls=None, *, returnType, useArrow=None)` | yes | yes | `caller-owned-guided` | Callback-defined row expansion and schema depend on runtime class behavior; the optional return-type signature varies by target. | Use native PySpark with an explicitly declared output schema and cardinality boundary. |
| `unwrap_udt(col)` | yes | yes | `caller-owned-guided` | Removing a user-defined type wrapper depends on opaque UDT metadata. | Keep UDT unwrapping in native PySpark and declare the resulting Structure schema at the boundary. |
| `current_catalog()` | yes | yes | `caller-owned-guided` | Session catalog identity is not a portable row-local value. | Read session metadata in native PySpark outside the typed row expression. |
| `current_database()` | yes | yes | `caller-owned-guided` | Session database identity is not a portable row-local value. | Read session metadata in native PySpark outside the typed row expression. |
| `current_schema()` | yes | yes | `caller-owned-guided` | Session schema identity is not a portable row-local value. | Read session metadata in native PySpark outside the typed row expression. |
| `current_user()` | yes | yes | `caller-owned-guided` | Runtime identity is environment-dependent. | Read runtime identity in native PySpark at the session boundary. |
| `user()` | yes | yes | `caller-owned-guided` | Runtime identity is environment-dependent. | Read runtime identity in native PySpark at the session boundary. |
| `version()` | yes | yes | `caller-owned-guided` | Runtime version is deployment metadata rather than a portable row-local contract. | Read runtime version in native PySpark outside the typed row expression. |
| `input_file_block_length()` | yes | yes | `caller-owned-guided` | File split metadata depends on the physical input plan. | Keep file metadata access in native PySpark at a physical-input boundary. |
| `input_file_block_start()` | yes | yes | `caller-owned-guided` | File split metadata depends on the physical input plan. | Keep file metadata access in native PySpark at a physical-input boundary. |
| `input_file_name()` | yes | yes | `caller-owned-guided` | Input path is physical-source metadata and is not portable across execution plans. | Keep input-path access in native PySpark at a physical-input boundary. |
| `monotonically_increasing_id()` | yes | yes | `caller-owned-guided` | Generated identifiers depend on physical partitioning and are not stable across plans. | Generate IDs in native PySpark only when physical-plan dependence is acceptable. |
| `spark_partition_id()` | yes | yes | `caller-owned-guided` | Partition identity exposes the physical execution plan. | Keep partition inspection in native PySpark at an explicit physical-plan boundary. |
| `java_method(*cols)` | yes | yes | `caller-owned-guided` | JVM reflection has no stable cross-runtime type or failure contract. | Use native PySpark reflection only within a runtime-specific boundary. |
| `reflect(*cols)` | yes | yes | `caller-owned-guided` | JVM reflection has no stable cross-runtime type or failure contract. | Use native PySpark reflection only within a runtime-specific boundary. |
| `typeof(col)` | yes | yes | `caller-owned-guided` | Runtime type inspection is not a stable substitute for declared Structure types. | Use declared Structure types; keep `typeof(...)` in native PySpark for diagnostics or runtime-specific logic. |

These exclusions correspond to the named categories in the machine-readable transformation inventory. Their exact
boundary reasons are maintained there; this table gives each common indexed symbol its signature, status, and
migration remedy.

## Contract Closure

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| Ordering descriptors | `implemented` | Harden validation across consumers. | Preserve direction and null placement. |
| `stack` | `implemented` | `stack(rows, *values, as_=Schema, scope=None)` fixes row multiplication, position-wise common types, and trailing-NULL padding before execution. | Preserve output aliases, types, nullability, and streaming row expansion. |
| Relation distribution | `implemented` | `coalesce(partitions=...)` and typed hash `repartition(count, *keys)` / `repartition(*keys)` preserve row/schema; range repartition remains batch-only. | Preserve the leading-integer count rule and avoid ordering or stable-partition promises. |
| Binary conversion | `implemented` | Typed `to_binary` and `try_to_binary` with literal formats. | Preserve format and failure behavior. |
| Unix-seconds formatting | `implemented` | Typed `from_unixtime` with numeric seconds and a literal format. | Preserve session-time-zone formatting. |
| Unix-seconds parsing | `implemented` | Typed `unix_timestamp` with String/Date/Timestamp inputs and the query-time default form. | Preserve the default format and query-time stability. |
| UTC conversion | `implemented` | Typed `to_utc_timestamp` and `from_utc_timestamp` with literal timezones. | Preserve timestamp nullability and timezone semantics. |
| NTZ timezone conversion | `implemented` | `convert_timezone(source_tz, target_tz, timestamp_ntz)` accepts typed String zone expressions and PySpark's `None` source-zone default. | Keep wall-clock NTZ values distinct from instant-based Timestamp values. |
| Date construction | `implemented` | `make_date(year, month, day)` accepts typed Integer/Long expressions and returns nullable Date. | Invalid-component behavior follows Spark ANSI configuration. |
| NTZ parsing | `implemented` | `to_timestamp_ntz` parses a String expression with an optional typed String format expression to nullable TimestampNTZ. | Keep result type independent of `spark.sql.timestampType`; malformed text follows Spark's ANSI setting. |

## Newly Reconciled Numeric Functions

The official PySpark 3.5.6 and 4.0.0 function indexes contain the following common numeric names missing from the
selected inventory. The five exact-name aliases now have typed helpers and preserve their SQL function names in
generated code. The `try_*` operations are inventoried but remain design-gated until Structure specifies their
nullable-on-overflow behavior, ANSI interaction, operand/result types, and (for `try_divide`) zero-divisor and
interval behavior.

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or missing contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `ceiling(col)` | yes | yes | `implemented` | Typed numeric alias of `ceil`, rendered as `F.ceiling(...)`. | Use `ceiling(...)` to preserve the PySpark spelling. |
| `negate(col)` | yes | yes | `implemented` | Typed numeric unary negation, rendered as `F.negate(...)`. | Use `negate(...)` directly. |
| `negative(col)` | yes | yes | `implemented` | Typed numeric unary negation, rendered as `F.negative(...)`. | Use `negative(...)` directly. |
| `positive(col)` | yes | yes | `implemented` | Typed numeric unary plus, rendered as `F.positive(...)`. | Use `positive(...)` directly. |
| `power(col1, col2)` | yes | yes | `implemented` | Typed Double result alias of `pow`, rendered as `F.power(...)`. | Use `power(...)` to preserve the PySpark spelling. |
| `try_add(left, right)` | yes | yes | `design-gated` | Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. | Use native PySpark until the safe-arithmetic contract is closed. |
| `try_divide(left, right)` | yes | yes | `design-gated` | Always floating-point division; zero divisor yields null; Spark also accepts interval operands. | Use native PySpark until numeric and interval overloads are specified. |
| `try_multiply(left, right)` | yes | yes | `design-gated` | Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. | Use native PySpark until the safe-arithmetic contract is closed. |
| `try_subtract(left, right)` | yes | yes | `design-gated` | Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. | Use native PySpark until the safe-arithmetic contract is closed. |
| `try_avg(col)` | yes | yes | `design-gated` | Aggregate returns null on overflow; aggregate widening, filtering, and streaming semantics need a Structure contract. | Use native PySpark until the safe-aggregate contract is closed. |
| `try_sum(col)` | yes | yes | `design-gated` | Aggregate returns null on overflow; aggregate widening, filtering, and streaming semantics need a Structure contract. | Use native PySpark until the safe-aggregate contract is closed. |

Sources: [PySpark 3.5.6 SQL functions](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[PySpark 4.0.0 SQL functions](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html), and
the PySpark API pages for [`try_divide`](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_divide.html)
and [`try_avg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_avg.html).

### Core aggregate functions

These core aggregates are documented Python functions in both pinned indexes. The same-named Structure aggregate
helpers are typed and share the grouped-aggregate implementation; the rows make the `pyspark.sql.functions`
spellings explicit in the source inventory and migration register.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `avg(col)` | yes | yes | `implemented` | `avg(value, *, where=None)` returns a typed nullable aggregate result. | Use `avg(...)` directly. |
| `count(col)` | yes | yes | `implemented` | `count(value=None, *, where=None)` counts non-null values, or rows when no value is supplied. | Use `count(...)`; `count()` corresponds to PySpark `count("*")`. |
| `collect_list(col)` | yes | yes | `implemented` | `collect_list(value, *, order_by=None, where=None, element_type=None)` returns a typed array; nulls are skipped and order is defined only when requested. | Use `collect_list(...)`; specify `order_by=` when deterministic order matters. |
| `collect_set(col)` | yes | yes | `implemented` | `collect_set(value, *, where=None, element_type=None)` returns a typed de-duplicated array; nulls are skipped and order is unspecified. | Use `collect_set(...)` when set-like aggregation is intended. |
| `max(col)` | yes | yes | `implemented` | `max(value, *, where=None)` returns the maximum orderable value. | Use `max(...)` directly. |
| `min(col)` | yes | yes | `implemented` | `min(value, *, where=None)` returns the minimum orderable value. | Use `min(...)` directly. |
| `sum(col)` | yes | yes | `implemented` | `sum(value, *, where=None)` returns a typed nullable aggregate result. | Use `sum(...)` directly. |

### Common numeric expressions

The names below are also present in the selected numeric intake and in both official function indexes. Their typed
helpers are already listed in the transformation inventory and coverage ledger; these rows make their exact migration
contracts visible. `log` and `width_bucket` have narrower Structure signatures than PySpark when their base or bucket
count is row-dependent, so those cases retain an explicit native-PySpark remedy.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `abs(col)` | yes | yes | `implemented` | `abs(value)` accepts typed numeric input, preserves its type, and propagates nullability. | Use `abs(...)` directly. |
| `acos(col)` | yes | yes | `implemented` | `acos(value)` accepts numeric input and returns nullable Double radians. | Use `acos(...)` directly. |
| `acosh(col)` | yes | yes | `implemented` | `acosh(value)` accepts numeric input and returns nullable Double. | Use `acosh(...)` directly. |
| `asin(col)` | yes | yes | `implemented` | `asin(value)` accepts numeric input and returns nullable Double radians. | Use `asin(...)` directly. |
| `asinh(col)` | yes | yes | `implemented` | `asinh(value)` accepts numeric input and returns nullable Double. | Use `asinh(...)` directly. |
| `atan(col)` | yes | yes | `implemented` | `atan(value)` accepts numeric input and returns nullable Double radians. | Use `atan(...)` directly. |
| `atan2(y, x)` | yes | yes | `implemented` | `atan2(y, x)` accepts numeric expressions and returns nullable Double radians. | Preserve PySpark's `y, x` argument order. |
| `atanh(col)` | yes | yes | `implemented` | `atanh(value)` accepts numeric input and returns nullable Double. | Use `atanh(...)` directly. |
| `bin(col)` | yes | yes | `implemented` | `bin(value)` accepts Integer/Long and returns nullable Binary-format String. | Use `bin(...)` directly. |
| `bround(col, scale=0)` | yes | yes | `implemented` | `bround(value, *, scale=0)` uses half-even rounding and preserves the typed numeric result. | Use a literal integer `scale`. |
| `cbrt(col)` | yes | yes | `implemented` | `cbrt(value)` accepts numeric input and returns nullable Double. | Use `cbrt(...)` directly. |
| `ceil(col)` | yes | yes | `implemented` | `ceil(value)` returns Spark's ceiling result type and preserves input nullability. | Use `ceil(...)` directly. |
| `conv(col, fromBase, toBase)` | yes | yes | `implemented` | `conv(value, *, from_base, to_base)` accepts String plus validated integer base literals and returns nullable String. | Supply literal bases in the inclusive range −36…−2 or 2…36. |
| `cos(col)` | yes | yes | `implemented` | `cos(value)` accepts numeric input and returns nullable Double. | Use `cos(...)` directly. |
| `cosh(col)` | yes | yes | `implemented` | `cosh(value)` accepts numeric input and returns nullable Double. | Use `cosh(...)` directly. |
| `cot(col)` | yes | yes | `implemented` | `cot(value)` accepts numeric input and returns nullable Double. | Use `cot(...)` directly. |
| `csc(col)` | yes | yes | `implemented` | `csc(value)` accepts numeric input and returns nullable Double. | Use `csc(...)` directly. |
| `degrees(col)` | yes | yes | `implemented` | `degrees(value)` accepts numeric radians and returns nullable Double degrees. | Use `degrees(...)` directly. |
| `e()` | yes | yes | `implemented` | `e()` returns a non-null Double expression for Euler's number. | Use `e()` directly. |
| `exp(col)` | yes | yes | `implemented` | `exp(value)` accepts numeric input and returns nullable Double. | Use `exp(...)` directly. |
| `expm1(col)` | yes | yes | `implemented` | `expm1(value)` accepts numeric input and returns nullable Double. | Use `expm1(...)` directly. |
| `factorial(col)` | yes | yes | `implemented` | `factorial(value)` accepts Integer/Long and returns nullable Long. | Use an integral expression. |
| `floor(col)` | yes | yes | `implemented` | `floor(value)` returns Spark's floor result type and preserves input nullability. | Use `floor(...)` directly. |
| `greatest(*cols)` | yes | yes | `implemented` | `greatest(*values)` requires at least two compatible expressions, skips nulls, and returns null only when all are null. | Use `greatest(...)` directly. |
| `hex(col)` | yes | yes | `implemented` | `hex(value)` accepts Integer/Long or Binary and returns nullable hexadecimal String. | Use `hex(...)` directly for supported types; cast or use native PySpark for other input types. |
| `hypot(col1, col2)` | yes | yes | `implemented` | `hypot(left, right)` accepts numeric expressions and returns nullable Double. | Use `hypot(...)` directly. |
| `least(*cols)` | yes | yes | `implemented` | `least(*values)` requires at least two compatible expressions, skips nulls, and returns null only when all are null. | Use `least(...)` directly. |
| `ln(col)` | yes | yes | `implemented` | `ln(value)` accepts numeric input and returns nullable Double. | Use `ln(...)` directly. |
| `log(arg1, arg2=None)` | yes | yes | `caller-owned-guided` | `log(value, *, base=None)` accepts a typed value and an optional positive finite literal base; generated calls preserve PySpark argument order. | Use `base=` for literal bases; use native PySpark for a row-dependent base. |
| `log10(col)` | yes | yes | `implemented` | `log10(value)` accepts numeric input and returns nullable Double. | Use `log10(...)` directly. |
| `log1p(col)` | yes | yes | `implemented` | `log1p(value)` accepts numeric input and returns nullable Double. | Use `log1p(...)` directly. |
| `log2(col)` | yes | yes | `implemented` | `log2(value)` accepts numeric input and returns nullable Double. | Use `log2(...)` directly. |
| `pi()` | yes | yes | `implemented` | `pi()` returns a non-null Double expression for π. | Use `pi()` directly. |
| `pmod(col1, col2)` | yes | yes | `implemented` | `pmod(left, right)` accepts numeric expressions, preserves their common type, and propagates nullability. | Use `pmod(...)` directly. |
| `pow(col1, col2)` | yes | yes | `implemented` | `pow(value, exponent)` accepts numeric expressions and returns nullable Double. | Use `pow(...)` directly. |
| `radians(col)` | yes | yes | `implemented` | `radians(value)` accepts numeric degrees and returns nullable Double radians. | Use `radians(...)` directly. |
| `rint(col)` | yes | yes | `implemented` | `rint(value)` accepts numeric input and returns nullable Double using nearest-integer rounding. | Use `rint(...)` directly. |
| `round(col, scale=0)` | yes | yes | `implemented` | `round(value, *, scale=0)` uses half-up rounding and preserves the typed numeric result. | Use a literal integer `scale`. |
| `sec(col)` | yes | yes | `implemented` | `sec(value)` accepts numeric radians and returns nullable Double. | Use `sec(...)` directly. |
| `sign(col)` | yes | yes | `implemented` | `sign(value)` returns nullable Double signum and preserves the PySpark spelling. | Use `sign(...)` directly. |
| `signum(col)` | yes | yes | `implemented` | `signum(value)` returns nullable Double signum. | Use `signum(...)` directly. |
| `sin(col)` | yes | yes | `implemented` | `sin(value)` accepts numeric input and returns nullable Double radians. | Use `sin(...)` directly. |
| `sinh(col)` | yes | yes | `implemented` | `sinh(value)` accepts numeric input and returns nullable Double. | Use `sinh(...)` directly. |
| `sqrt(col)` | yes | yes | `implemented` | `sqrt(value)` accepts numeric input and returns nullable Double. | Use `sqrt(...)` directly. |
| `tan(col)` | yes | yes | `implemented` | `tan(value)` accepts numeric input and returns nullable Double radians. | Use `tan(...)` directly. |
| `tanh(col)` | yes | yes | `implemented` | `tanh(value)` accepts numeric input and returns nullable Double. | Use `tanh(...)` directly. |
| `unhex(col)` | yes | yes | `implemented` | `unhex(value)` accepts String and returns nullable Binary. | Use `unhex(...)` directly. |
| `width_bucket(v, min, max, numBucket)` | yes | yes | `caller-owned-guided` | `width_bucket(value, minimum, maximum, *, num_buckets)` accepts numeric value/range expressions and a positive integer literal; it returns nullable Long, matching Spark's LongType output. | Use a literal bucket count; use native PySpark when the count is row-dependent. |

The 3.5.6 and 4.0.0 indexes list these names in the common numeric surface. See [PySpark 3.5.6 SQL functions](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html), [PySpark 4.0.0 SQL functions](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html), and the [`width_bucket` API](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.width_bucket.html).

## Writer Partition Transforms

The official indexes expose five partition-transform constructors on both target lines. They are included in the
intake so their scope is explicit, but their only supported use is with `DataFrameWriterV2.partitionedBy`; they do not
transform a relation and remain caller-owned output-layout policy. PySpark 3.5.6 documents them under
`pyspark.sql.functions`, while PySpark 4.0.0 places them under `pyspark.sql.functions.partitioning`.

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or boundary | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `years(col)` | yes | yes | `caller-owned-guided` | Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. | Use native PySpark writer APIs. |
| `months(col)` | yes | yes | `caller-owned-guided` | Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. | Use native PySpark writer APIs. |
| `days(col)` | yes | yes | `caller-owned-guided` | Output transform for Date/Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. | Use native PySpark writer APIs. |
| `hours(col)` | yes | yes | `caller-owned-guided` | Output transform for Timestamp values; only valid in `DataFrameWriterV2.partitionedBy`. | Use native PySpark writer APIs. |
| `bucket(numBuckets, col)` | yes | yes | `caller-owned-guided` | Hash-bucket output transform for any input type; only valid in `DataFrameWriterV2.partitionedBy`. | Use native PySpark writer APIs. |

Sources: the [PySpark 3.5.6 function index](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[PySpark 4.0.0 function index](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html), and
the [`bucket`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bucket.html)
and [`years`](https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.functions.years.html)
references.

## Bitwise, Hash, Encoding, and Crypto Functions

The following baseline helpers now have explicit per-function dispositions. SQL `bitwise_not(col)` complements
Structure's existing `Expression.bitwise_not()` method and preserves the PySpark spelling in generated code.

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or missing contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `bitwise_not(col)` | yes | yes | `implemented` | Typed SQL-function form preserves the input Integer/Long type and nullability; the Column method remains available. | Use `bitwise_not(...)` to preserve the PySpark function spelling. |
| `bit_count(col)` | yes | yes | `implemented` | `bit_count(value)` returns Long for Integer/Long input and preserves nullability. | Use `bit_count(...)` directly. |
| `bit_get(col, pos)` | yes | yes | `implemented` | `bit_get(value, position)` requires integral inputs and returns nullable Integer. | Use `bit_get(...)` with typed integral operands. |
| `getbit(col, pos)` | yes | yes | `implemented` | `getbit(value, position)` preserves the SQL spelling and shares the integral position contract. | Use `getbit(...)` directly. |
| `shiftleft(col, numBits)` | yes | yes | `caller-owned-guided` | `shiftleft(value, *, bits=int)` supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. | Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |
| `shiftright(col, numBits)` | yes | yes | `caller-owned-guided` | `shiftright(value, *, bits=int)` supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. | Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |
| `shiftrightunsigned(col, numBits)` | yes | yes | `caller-owned-guided` | `shiftrightunsigned(value, *, bits=int)` supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. | Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `hash(*cols)` | yes | yes | `implemented` | `hash(*values)` returns Integer and preserves Spark's multi-column hash behavior. | Use `hash(...)` directly. |
| `xxhash64(*cols)` | yes | yes | `implemented` | `xxhash64(*values)` returns Long and preserves Spark's 64-bit hash behavior. | Use `xxhash64(...)` directly. |
| `crc32(col)` | yes | yes | `implemented` | `crc32(value)` accepts String/Binary and returns Long. | Use `crc32(...)` directly. |
| `md5(col)` | yes | yes | `implemented` | `md5(value)` returns the lowercase hexadecimal digest String. | Use `md5(...)` directly. |
| `sha1(col)` | yes | yes | `implemented` | `sha1(value)` returns the hexadecimal digest String. | Use `sha1(...)` directly. |
| `sha2(col, numBits)` | yes | yes | `implemented` | `sha2(value, num_bits=...)` accepts supported digest-size literals and returns hexadecimal String. | Use `sha2(...)` with a compiler-visible supported digest size. |

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `base64(col)` | yes | yes | `implemented` | `base64(value)` encodes Binary as String. | Use `base64(...)` directly. |
| `unbase64(col)` | yes | yes | `implemented` | `unbase64(value)` decodes String to Binary. | Use `unbase64(...)` directly. |
| `encode(col, charset)` | yes | yes | `implemented` | `encode(value, charset=...)` accepts a compiler-visible charset literal and returns Binary. | Use `encode(...)` with a supported literal charset. |
| `decode(col, charset)` | yes | yes | `implemented` | `decode(value, charset=...)` accepts a compiler-visible charset literal and returns String. | Use `decode(...)` with a supported literal charset. |
| `to_binary(col, format=None)` | yes | yes | `implemented` | `to_binary(value, format=...)` validates the format literal and preserves Spark conversion errors. | Use `to_binary(...)` for strict conversion. |
| `try_to_binary(col, format=None)` | yes | yes | `implemented` | `try_to_binary(value, format=...)` returns null when conversion fails. | Use `try_to_binary(...)` when malformed input should become null. |

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or boundary | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `aes_encrypt(input, key, mode='GCM', padding='DEFAULT', iv=None, aad=None)` | yes | yes | `implemented` | `aes_encrypt(value, key=..., aad=None, iv=None)` is GCM-only; explicit IVs warn that the caller owns nonce uniqueness. | Use the typed GCM helper; omit `iv` unless a protocol requires interoperability. |
| `aes_decrypt(input, key, mode='GCM', padding='DEFAULT', aad=None)` | yes | yes | `implemented` | `aes_decrypt(value, key=..., aad=None)` is strict GCM decryption over typed String/Binary values. | Use the typed GCM helper; use native PySpark for CBC/ECB or padding selection. |
| `try_aes_decrypt(input, key, mode='GCM', padding='DEFAULT', aad=None)` | yes | yes | `implemented` | `try_aes_decrypt(value, key=..., aad=None)` returns nullable Binary on authentication/decryption failure. | Use the typed GCM helper when invalid ciphertext should become null. |

PySpark 3.5.6 also lists the camel-case `bitwiseNOT(col)` alias; it is not listed in 4.0.0, so it remains a
3.5.6-only target-line addition. The official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) indexes provide the
common name and signature evidence for these rows.

## Temporal and Query-Clock Functions

The common temporal and query-clock spellings are listed individually because their distinctions are migration
relevant: query-start clocks are not transform-start clocks, LTZ and NTZ values are different types, epoch helpers
have fixed units, and several Spark parameters are compiler-visible literals. The date arithmetic rows preserve
typed day counts; in particular, `date_sub` now accepts a row-dependent integral expression as PySpark does.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `add_months(start, months)` | yes | yes | `implemented` | `add_months(value, *, months)` accepts an Integer/Long literal or typed integral expression and returns Date. | Use a typed month count; the result remains Date. |
| `date_add(start, days)` | yes | yes | `implemented` | `date_add(value, *, days)` accepts an Integer/Long literal or typed integral expression and returns Date. | Use a typed day count; the result remains Date. |
| `date_sub(start, days)` | yes | yes | `implemented` | `date_sub(value, *, days)` accepts an Integer/Long literal or typed integral expression and returns Date. | Pass a compiler-visible integral expression for row-dependent offsets. |
| `date_diff(end, start)` | yes | yes | `implemented` | Exact-name alias of `datediff`; returns nullable Integer day difference. | Preserve PySpark's `end, start` order. |
| `dateadd(start, days)` | yes | yes | `implemented` | Exact-name alias of `date_add`; accepts typed integral day counts and returns Date. | Use `dateadd(...)` to preserve the PySpark spelling. |
| `datediff(end, start)` | yes | yes | `implemented` | Returns nullable Integer days between typed Date/Timestamp inputs. | Preserve PySpark's `end, start` order. |
| `date_trunc(format, timestamp)` | yes | yes | `implemented` | `date_trunc(value, *, unit)` returns Timestamp; unit is a validated literal or `Temporal` constant. | Use a supported unit such as `Temporal.DAY`; keep the value typed. |
| `date_format(date, format)` | yes | yes | `implemented` | `date_format(value, *, format)` returns nullable String and requires a non-empty format literal. | Keep the Spark pattern compiler-visible. |
| `date_from_unix_date(days)` | yes | yes | `implemented` | Integral days from 1970-01-01 convert to nullable Date. | Pass an Integer/Long day expression. |
| `unix_date(date)` | yes | yes | `implemented` | Typed Date converts to nullable Integer days since 1970-01-01. | Use Date input; convert other temporal types explicitly. |
| `from_unixtime(timestamp, format='yyyy-MM-dd HH:mm:ss')` | yes | yes | `implemented` | Numeric epoch seconds format to String in Spark's session time zone; format is a literal. | Set the Spark session zone and provide a literal pattern when needed. |
| `unix_timestamp(timestamp=None, format='yyyy-MM-dd HH:mm:ss')` | yes | yes | `implemented` | Parses typed temporal input to Long seconds; the no-input form is nondeterministic but query-stable. | Use an explicit input for deterministic parsing; no-input form uses query-start time. |
| `to_unix_timestamp(timestamp, format=None)` | yes | yes | `implemented` | Requires a typed String/Date/Timestamp input and returns Long epoch seconds; optional pattern may be typed String. | Use when an input is required; use `unix_timestamp()` for its query-clock form. |
| `to_utc_timestamp(timestamp, tz)` | yes | yes | `implemented` | Converts typed temporal input using a non-empty timezone literal and returns LTZ Timestamp. | Keep the timezone ID compiler-visible. |
| `from_utc_timestamp(timestamp, tz)` | yes | yes | `implemented` | Converts typed temporal input using a non-empty timezone literal and returns LTZ Timestamp. | Keep the timezone ID compiler-visible. |
| `convert_timezone(sourceTz, targetTz, sourceTs)` | yes | yes | `implemented` | Requires TimestampNTZ and typed String zone expressions; `sourceTz=None` uses Spark's session zone. | Keep wall-clock NTZ separate from LTZ instant conversion. |
| `make_date(year, month, day)` | yes | yes | `implemented` | Typed Integer/Long components produce nullable Date; invalid components follow Spark ANSI policy. | Use integral expressions and preserve Spark's ANSI configuration. |
| `make_dt_interval(days=None, hours=None, mins=None, secs=None)` | yes | yes | `implemented` | Typed DayTime interval; integral components and numeric seconds. | Keep the interval expression typed; Python Row conversion yields `timedelta`. |
| `make_interval(years=None, months=None, weeks=None, days=None, hours=None, mins=None, secs=None)` | yes | yes | `implemented` | Typed mixed Calendar interval with Spark's seven components. | Calendar Schema is unavailable on 3.5; 4.0 schema/arithmetic is verified, but Python Row conversion is not supported on tested paths. |
| `make_timestamp(year, month, day, hour, min, sec, timezone=None)` | yes | yes | `implemented` | Typed components; result follows resolved `spark.sql.timestampType`; invalid values follow ANSI policy. | Align the resolved Structure and Spark timestamp profiles; use an explicit LTZ/NTZ constructor when fixed type is needed. |
| `make_timestamp_ltz(year, month, day, hour, min, sec, timezone=None)` | yes | yes | `implemented` | Fixed LTZ result with typed components and optional typed String zone. | Use for an instant-valued timestamp. |
| `make_timestamp_ntz(year, month, day, hour, min, sec)` | yes | yes | `implemented` | Fixed NTZ wall-clock result with typed components. | Use for a timestamp without time zone. |
| `make_ym_interval(years=None, months=None)` | yes | yes | `implemented` | Typed YearMonth interval expression. | Keep interval values inside expressions; Python conversion is target-sensitive. |
| `date_part(field, source)` | yes | yes | `implemented` | Exact spelling with compiler-visible String/`Temporal` field; Date, Timestamp, and interval inputs. | Use a literal or `Temporal` constant; result is Integer except seconds (`Decimal(8,6)`). |
| `datepart(field, source)` | yes | yes | `implemented` | Exact alias of `date_part` retaining the PySpark spelling and typed-field contract. | Use a compiler-visible field constant or literal. |
| `extract(field, source)` | yes | yes | `implemented` | SQL-standard field extraction for typed Date, Timestamp, and interval values. | Use a compiler-visible field; result is Integer except seconds (`Decimal(8,6)`). |
| `day(date)` | yes | yes | `implemented` | Exact alias of day-of-month extraction; nullable Integer. | Use `day(...)` to preserve the PySpark spelling. |
| `last_day(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Date at that month's end. | Use for calendar-month boundaries. |
| `months_between(date1, date2, roundOff=True)` | yes | yes | `implemented` | Date/Timestamp inputs return nullable Double; `round_off` is a Boolean literal. | Pass `round_off=` explicitly when disabling Spark's rounding. |
| `trunc(date, format)` | yes | yes | `implemented` | Date-only truncation; `unit` is a validated literal or `Temporal` constant. | Use `date_trunc` for Timestamp values. |
| `year(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Integer year. | Use directly with a typed temporal expression. |
| `month(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Integer month. | Use directly with a typed temporal expression. |
| `dayofmonth(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Integer day of month. | Use `day(...)` when preserving that PySpark alias is preferred. |
| `dayofweek(date)` | yes | yes | `implemented` | Returns nullable Integer with Spark numbering: Sunday=1 through Saturday=7. | Do not substitute zero-based `weekday(...)`. |
| `dayofyear(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Integer day of year. | Use `Temporal.DAY_OF_YEAR` with `extract` for field-based code. |
| `hour(timestamp)` | yes | yes | `implemented` | Timestamp input returns nullable Integer hour. | Pass a typed Timestamp, not Date. |
| `minute(timestamp)` | yes | yes | `implemented` | Timestamp input returns nullable Integer minute. | Pass a typed Timestamp, not Date. |
| `next_day(date, dayOfWeek)` | yes | yes | `implemented` | Date/Timestamp input and validated weekday literal return nullable Date. | Use a recognized weekday string or constant. |
| `quarter(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable Integer quarter. | Use directly with a typed temporal expression. |
| `second(timestamp)` | yes | yes | `implemented` | Timestamp input returns nullable Integer second. | Use `extract(Temporal.SECOND, ...)` when fractional seconds are required. |
| `to_date(col, format=None)` | yes | yes | `implemented` | String/Date/Timestamp input converts to Date; optional pattern is a non-empty literal. | Supply a literal pattern for custom String parsing. |
| `to_timestamp(col, format=None)` | yes | yes | `implemented` | String/Date/Timestamp input converts to the resolved LTZ/NTZ type; typed String pattern is supported. | Align the timestamp profile; use explicit LTZ/NTZ parsing for a fixed type. |
| `to_timestamp_ltz(timestamp, format=None)` | yes | yes | `implemented` | Typed String plus optional typed String pattern yields nullable LTZ Timestamp. | Use for explicit instant parsing. |
| `to_timestamp_ntz(timestamp, format=None)` | yes | yes | `implemented` | Typed String plus optional typed String pattern yields nullable NTZ Timestamp. | Use for explicit wall-clock parsing. |
| `try_to_timestamp(col, format=None)` | yes | yes | `implemented` | Generic resolved timestamp type; invalid String input returns null regardless of ANSI mode. | Use when malformed text must become null. |
| `timestamp_micros(micros)` | yes | yes | `implemented` | Integral microseconds convert to fixed LTZ Timestamp. | Use for microsecond epoch values; NTZ is not inferred. |
| `timestamp_millis(millis)` | yes | yes | `implemented` | Integral milliseconds convert to fixed LTZ Timestamp. | Use for millisecond epoch values; NTZ is not inferred. |
| `timestamp_seconds(seconds)` | yes | yes | `implemented` | Numeric seconds, including fractions, convert to fixed LTZ Timestamp. | Use for fractional epoch seconds; use Decimal for exact fractions. |
| `unix_micros(timestamp)` | yes | yes | `implemented` | LTZ Timestamp converts to Long microseconds; NTZ is rejected. | Convert NTZ explicitly before requesting an epoch value. |
| `unix_millis(timestamp)` | yes | yes | `implemented` | LTZ Timestamp converts to Long milliseconds; NTZ is rejected. | Convert NTZ explicitly before requesting an epoch value. |
| `unix_seconds(timestamp)` | yes | yes | `implemented` | LTZ Timestamp converts to Long seconds; NTZ is rejected. | Convert NTZ explicitly before requesting an epoch value. |
| `weekday(date)` | yes | yes | `implemented` | Monday-first zero-based weekday Integer, unlike Spark's Sunday-first `dayofweek`. | Choose the helper whose indexing convention matches the caller. |
| `weekofyear(date)` | yes | yes | `implemented` | Date/Timestamp input returns nullable ISO week number as Integer. | Use directly with a typed temporal expression. |
| `current_date()` | yes | yes | `implemented` | Date value is fixed at the start of query evaluation. | Use for query-scoped current date, not transform invocation time. |
| `curdate()` | yes | yes | `implemented` | Exact alias of `current_date()` with the same query-start stability. | Preserve the alias when matching PySpark source spelling. |
| `current_timestamp()` | yes | yes | `implemented` | LTZ timestamp is fixed at the start of query evaluation. | Use for query-scoped current time, not per-row clock sampling. |
| `now()` | yes | yes | `implemented` | Exact alias of `current_timestamp()` with query-start stability. | Preserve the alias when matching PySpark source spelling. |
| `localtimestamp()` | yes | yes | `implemented` | NTZ wall-clock timestamp fixed at query start. | Keep its NTZ type distinct from `current_timestamp()`. |
| `current_timezone()` | yes | yes | `implemented` | Returns the session's current timezone as String. | Treat the value as session configuration, not a row-dependent timezone. |

All 59 names are present in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) indexes.
The date arithmetic signatures, including Column-valued day offsets, are also shown in the official
[PySpark 3.5.6 `dateadd` reference](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dateadd.html)
and its [3.5.7 public source](https://spark.apache.org/docs/3.5.7/api/python/_modules/pyspark/sql/functions.html).

## Newly Reconciled Array Lookup and Size Functions

The official 3.5.6 and 4.0.0 indexes include these common collection operations. Structure already exposes typed
helpers for the functions, but the helpers intentionally distinguish Spark's zero-based `get` from one-based
`element_at`, require compiler-visible types, and preserve the runtime null-size policy for `size`/`cardinality`.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `size(col)` | yes | yes | `implemented` | `size(value)` accepts Array or Map and returns nullable Integer. For null input, Spark's value depends on `spark.sql.legacy.sizeOfNull` and ANSI configuration. | Use `size(...)` when the configured Spark null-size behavior is intended. |
| `cardinality(col)` | yes | yes | `implemented` | `cardinality(value)` accepts Array or Map and returns nullable Integer; Spark owns its documented legacy/ANSI null policy. | Use `cardinality(...)` to preserve the SQL spelling and Spark configuration behavior. |
| `array_size(col)` | yes | yes | `implemented` | `array_size(value)` accepts Array only and returns nullable Integer; null input remains null. | Use `array_size(...)` when Map inputs should be rejected. |
| `array_contains(col, value)` | yes | yes | `implemented` | `array_contains(value, item)` requires compatible typed values and preserves Spark's nullable Boolean result for null arrays/elements. | Use typed expressions for row-valued items; use literals for constants. |
| `array_position(col, value)` | yes | yes | `implemented` | `arr_position(value, item)` requires a PySpark-3.5-compatible literal item and returns the first one-based position as nullable Long (zero means absent). | Use `arr_position(...)`; native PySpark is required for a row-valued search item on the 3.5 baseline. |
| `get(col, index)` | yes | yes | `implemented` | `get(value, index)` accepts an integral index expression and uses zero-based indexing; out-of-range access returns null. | Use `get(...)` for zero-based indexing; use `element_at(...)` for one-based indexing. |
| `element_at(col, extraction)` | yes | yes | `implemented` | `element_at(value, key)` accepts Array/Map. Array indexes are one-based (negative indexes count from the end); zero is invalid, and out-of-range behavior follows Spark ANSI mode. Map misses return null. | Use typed `element_at(...)`; provide a literal String for a literal map key. |
| `try_element_at(col, extraction)` | yes | yes | `implemented` | `try_element_at(value, key)` has the same typed Array/Map contract but returns null for missing/out-of-range lookups. Structure treats Python strings as literals; use a typed field expression when migrating PySpark's string-as-column-name form. | Use `try_element_at(...)` for null-on-miss behavior; pass a compiler-visible key expression. |
| `slice(x, start, length)` | yes | yes | `implemented` | `slice(value, start, length)` accepts integral expressions, uses one-based starts (negative starts count from the end), and rejects negative literal lengths. | Use `slice(...)` with a non-negative length and Spark's indexing convention. |

The common names are present in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) indexes.
Spark documents the zero-based `get` and array sizing aliases in the versioned API references for
[`get`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get.html),
[`array_size`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_size.html),
and [`try_element_at`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_element_at.html).

## Newly Reconciled Array Construction and Mutation Functions

These common constructors and mutations are typed in Structure. The per-function rows make the narrower contracts
explicit: numeric-only `sequence`, literal mutation operands required by the shared 3.5 baseline, and array element
type unification are not implied by a family-level "supported" entry.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `array(*cols)` | yes | yes | `implemented` | `array(*values)` requires at least one typed value, unifies compatible element types, and derives element nullability. | Use `array(...)` with values that have a common Structure type. |
| `array_repeat(col, count)` | yes | yes | `implemented` | `array_repeat(value, count)` requires an integral count expression and returns an Array with the input element type. | Use a typed Integer/Long count expression. |
| `sequence(start, stop, step=None)` | yes | yes | `caller-owned-guided` | `sequence(start, stop, step=None)` currently admits compatible Integer/Long expressions; zero literal step is rejected. PySpark also accepts Date/Timestamp ranges. | Use the typed helper for numeric ranges; use native PySpark for Date/Timestamp sequences. |
| `array_append(col, value)` | yes | yes | `implemented` | `arr_append(value, item)` appends one compatible typed item and carries element nullability. | Use `arr_append(...)` to preserve array type information. |
| `array_prepend(col, value)` | yes | yes | `implemented` | `arr_prepend(value, item)` prepends one compatible typed item and carries element nullability. | Use `arr_prepend(...)` to preserve array type information. |
| `array_insert(arr, pos, value)` | yes | yes | `implemented` | `arr_insert(value, position, item)` requires a nonzero integral Python literal position; positive positions are one-based and negative positions count from the end. | Use a literal nonzero position; use native PySpark if the position is row-dependent. |
| `array_remove(col, element)` | yes | yes | `caller-owned-guided` | `arr_remove(value, item)` requires a non-null compatible Python literal for the shared 3.5 baseline. | Use `arr_remove(...)` for literal items; use native PySpark for row-valued removal items. |
| `array_compact(col)` | yes | yes | `implemented` | `arr_compact(value)` removes null elements and narrows the result to `contains_null=False`. | Use `arr_compact(...)` when null elements should be removed. |

These names occur in both the [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) indexes; see also the
versioned [`array_insert`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_insert.html)
and [`array_compact`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_compact.html)
references.

## Newly Reconciled Array Composition and Ordering Functions

These common array operations preserve Structure's element types and nullability while retaining Spark's collection
semantics. The rows call out the type families and nondeterminism that the generic function names alone do not show.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `concat(*cols)` | yes | yes | `implemented` | `concat(*values)` accepts two or more homogeneous String, Binary, or compatible Array expressions. | Use a single family of compatible inputs; mixed families are rejected before Spark execution. |
| `array_join(col, delimiter, null_replacement=None)` | yes | yes | `implemented` | `array_join(value, delimiter, null_replacement=None)` accepts Array[String] and literal String options; nullability follows the input array. | Use literal delimiters/replacements and Array[String] input. |
| `array_max(col)` | yes | yes | `implemented` | `array_max(value)` returns the nullable maximum orderable scalar element, ignoring null elements. | Use for arrays with orderable scalar elements. |
| `array_min(col)` | yes | yes | `implemented` | `array_min(value)` returns the nullable minimum orderable scalar element, ignoring null elements. | Use for arrays with orderable scalar elements. |
| `arrays_overlap(a1, a2)` | yes | yes | `implemented` | `arrays_overlap(left, right)` requires compatible element types and preserves Spark's three-valued null semantics. | Use compatible typed arrays; expect nullable Boolean when null arrays/elements can affect the result. |
| `sort_array(col, asc=True)` | yes | yes | `implemented` | `sort_array(value, *, ascending=True)` accepts orderable scalar elements and a Boolean literal direction; null ordering follows Spark. | Use `ascending=` to make the direction explicit. |
| `shuffle(col)` | yes | yes | `implemented` | `shuffle(value)` preserves the array type and is marked nondeterministic. | Use only when the particular shuffled order is not part of a deterministic contract. |

All seven names are present in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) function indexes.

## Newly Reconciled Array Transform and Set Functions

Structure exposes typed helpers for common array set operations and higher-order transforms. Its symbolic callbacks
retain element types and, where supported, a zero-based index; sorting and zipped-struct schemas have narrower
contracts than PySpark's most general forms, so those boundaries are explicit below.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `array_distinct(col)` | yes | yes | `implemented` | `arr_distinct(value)` removes duplicate elements while preserving the typed array. | Use `arr_distinct(...)`. |
| `array_union(col1, col2)` | yes | yes | `implemented` | `array_union(left, right)` returns the union of compatible typed arrays with Spark's array semantics. | Use `array_union(...)`. |
| `array_intersect(col1, col2)` | yes | yes | `implemented` | `array_intersect(left, right)` returns the common elements of compatible typed arrays. | Use `array_intersect(...)`. |
| `array_except(col1, col2)` | yes | yes | `implemented` | `array_except(left, right)` returns left-side elements absent from the compatible right array. | Use `array_except(...)`. |
| `array_sort(col, comparator=None)` | yes | yes | `caller-owned-guided` | `arr_sort(value)` provides Spark's default ascending order; `arr_sort_by(value, key)` orders by a typed scalar key, but does not accept PySpark's arbitrary two-element comparator callback. | Use `arr_sort(...)` for default ordering or `arr_sort_by(...)` for key ordering; retain native PySpark for arbitrary comparator logic. |
| `reverse(col)` | yes | yes | `implemented` | `reverse(value)` handles String expressions and `arr_reverse(value)` handles Array expressions. | Keep `reverse(...)` for strings; migrate array inputs to `arr_reverse(...)`. |
| `flatten(col)` | yes | yes | `implemented` | `arr_flatten(value)` flattens one level of a typed nested array. | Use `arr_flatten(...)` for one-level flattening. |
| `transform(col, f)` | yes | yes | `implemented` | `arr_transform(value, function)` accepts symbolic element or element/index callbacks; the index is zero-based. | Express callback logic with typed `Expression` operations. |
| `filter(col, f)` | yes | yes | `implemented` | `arr_filter(value, function)` accepts symbolic Boolean element or element/index predicates; the optional index is zero-based. | Use a typed Boolean callback; preserve Spark's null behavior. |
| `exists(col, f)` | yes | yes | `implemented` | `arr_exists(value, function)` accepts a typed Boolean element predicate and preserves nullable Boolean semantics. | Use `arr_exists(...)` for existential tests. |
| `forall(col, f)` | yes | yes | `implemented` | `arr_forall(value, function)` accepts a typed Boolean element predicate and preserves nullable Boolean semantics. | Use `arr_forall(...)` for universal tests. |
| `aggregate(col, initialValue, merge, finish=None)` | yes | yes | `implemented` | `arr_aggregate(value, initial, merge, finish=None)` requires a type-stable accumulator and typed merge/finish callbacks. | Keep the accumulator type stable across every merge result. |
| `reduce(col, initialValue, merge, finish=None)` | yes | yes | `implemented` | `reduce(value, initial, merge, finish=None)` preserves the PySpark spelling and the typed accumulator contract. | Use the matching typed helper and a stable accumulator type. |
| `zip_with(left, right, f)` | yes | yes | `implemented` | `arr_zip_with(left, right, function)` types both callback arguments as nullable because Spark pads the shorter array with nulls. | Handle null callback arguments explicitly. |
| `arrays_zip(*cols)` | yes | yes | `caller-owned-guided` | `arrays_zip(*values)` returns a typed Array[Struct] with compiler-visible fields `array_0`, `array_1`, ...; Spark derives output field names from input columns/expressions. | Use it when positional typed fields are sufficient; use native PySpark when downstream code depends on Spark-derived field names. |

All names are listed in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) function indexes;
see the versioned [`array_sort`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_sort.html)
and [`arrays_zip`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.arrays_zip.html)
references for their comparator and output-shape contracts.

## Newly Reconciled Random Functions

Both target indexes expose seeded uniform and standard-normal random scalars. The typed API makes nondeterminism
explicit: an integer seed is required unless the caller sets `reproducible=False`; a seed supports auditing but does
not promise identical outputs across partitioning, retries, versions, or streaming restarts.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent or contract | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `rand(seed=None)` | yes | yes | `rand(*, seed, reproducible=True)` returns non-null Double uniformly distributed in `[0.0, 1.0)`. | `implemented` | Pass an integer seed for auditable use; opt into `reproducible=False` when no seed is intended. |
| `randn(seed=None)` | yes | yes | `randn(*, seed, reproducible=True)` returns non-null standard-normal Double under the same explicit nondeterminism policy. | `implemented` | Pass an integer seed or explicitly set `reproducible=False`; don't rely on stable output across repartitioning or retries. |

The public spellings and seed signatures appear in the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) indexes.
PySpark 4.0-only `randstr` and `uniform` remain target-gated outside the shared intersection.

## Newly Reconciled Boolean Aggregates

The public 3.5.6 and 4.0.0 function indexes list these common aggregate names. `bool_and` and `bool_or` were already
implemented but missing from the selected function inventory; `every` is admitted with the same all-values-true
contract as `bool_and` while preserving its PySpark spelling in generated code.

| PySpark function | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `bool_and(col)` | yes | yes | `bool_and(value, *, where=None)`; Boolean aggregate | `implemented` | Use `bool_and(...)`. |
| `bool_or(col)` | yes | yes | `bool_or(value, *, where=None)`; Boolean aggregate | `implemented` | Use `bool_or(...)`. |
| `every(col)` | yes | yes | `every(value, *, where=None)`; Boolean aggregate rendered as `F.every(...)` | `implemented` | Use `every(...)` to preserve the PySpark spelling. |

The helpers preserve Boolean result type and derive nullability from the input and optional aggregate filter. The
PySpark references document all three as Boolean aggregates: [3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html).

## Newly Reconciled Window Functions

The official PySpark 3.5.6 and 4.0.0 function indexes list all 11 names below in both target lines. Structure exposes
typed window specifications instead of raw `Column.over(...)`; the common value-function forms are available, while
PySpark's expression-valued `default`/`ignoreNulls` controls remain caller-owned.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `row_number()` | yes | yes | `row_number(*, partition_by, order_by, descending=False)`; non-null Long. | `implemented` | Supply the partition and ordering through the typed helper. |
| `rank()` | yes | yes | `rank(*, partition_by, order_by, descending=False)`; non-null Long with gaps after ties. | `implemented` | Supply the partition and ordering through the typed helper. |
| `dense_rank()` | yes | yes | `dense_rank(*, partition_by, order_by, descending=False)`; non-null Long without gaps after ties. | `implemented` | Supply the partition and ordering through the typed helper. |
| `percent_rank()` | yes | yes | `percent_rank(*, over=WindowSpec)`; non-null Double. | `implemented` | Build a typed `window(...)` and pass it as `over=`. |
| `cume_dist()` | yes | yes | `cume_dist(*, over=WindowSpec)`; non-null Double. | `implemented` | Build a typed `window(...)` and pass it as `over=`. |
| `ntile(n)` | yes | yes | `ntile(n, *, over=WindowSpec)`; positive literal bucket count, non-null Integer. | `implemented` | Use a positive integer literal for `n`. |
| `lag(col, offset=1, default=None)` | yes | yes | `lag(value, *, partition_by, order_by, offset=1, default=None, descending=False)` preserves the value type; `default` must be a compatible Python scalar literal. | `caller-owned-guided` | Use literal defaults with the typed helper; keep expression-valued defaults in native PySpark. |
| `lead(col, offset=1, default=None)` | yes | yes | `lead(value, *, partition_by, order_by, offset=1, default=None, descending=False)` preserves the value type; `default` must be a compatible Python scalar literal. | `caller-owned-guided` | Use literal defaults with the typed helper; keep expression-valued defaults in native PySpark. |
| `first_value(col, ignoreNulls=None)` | yes | yes | `first_value(value, *, over=WindowSpec, ignore_nulls=False)` preserves the value type; `ignore_nulls` is a Python Boolean. | `caller-owned-guided` | Use a Boolean option with the typed helper; keep an expression-valued `ignoreNulls` in native PySpark. |
| `last_value(col, ignoreNulls=None)` | yes | yes | `last_value(value, *, over=WindowSpec, ignore_nulls=False)` preserves the value type; `ignore_nulls` is a Python Boolean. | `caller-owned-guided` | Use a Boolean option with the typed helper; keep an expression-valued `ignoreNulls` in native PySpark. |
| `nth_value(col, offset, ignoreNulls=False)` | yes | yes | `nth_value(value, n, *, over=WindowSpec, ignore_nulls=False)` requires positive literal `n` and Boolean `ignore_nulls`; result preserves the value type and is nullable. | `caller-owned-guided` | Use literal options with the typed helper; keep expression-valued `ignoreNulls` in native PySpark. |

The function spellings and common signatures appear in both official indexes: [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html). Spark's API documents
`lag`/`lead` default values and value-function ignore-null controls separately; Structure limits these options to statically
typed literal controls.

## Newly Reconciled Generator Functions

PySpark's six common `explode`/`inline` names and its `stack` generator are present in both baseline indexes. Structure
maps each spelling to typed relation generators selected by the input family. The declared output `Schema` makes field names, types,
nullability, and row expansion cardinality explicit; use native PySpark for nested or otherwise unadmitted element
families.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `stack(n, *cols)` | yes | yes | `stack(rows, *values, as_=Schema, scope=None)` fixes row multiplication, position-wise common types, and trailing-NULL padding while keeping output schema explicit. | `implemented` | Declare the output Schema and nullable fields wherever input values or trailing padding can be null. |
| `explode(col)` | yes | yes | `explode_struct`, `explode_array`, or `explode_map`; array-of-struct, primitive scalar-array, or primitive scalar-map input with declared output `Schema`. | `implemented` | Choose the typed helper matching the input type and declare generated field names in `as_`. |
| `explode_outer(col)` | yes | yes | `explode_outer_struct`, `explode_outer_array`, or `explode_outer_map`; preserves null/empty input rows and requires nullable generated fields. | `implemented` | Choose an outer typed helper and declare nullable output fields. |
| `posexplode(col)` | yes | yes | `posexplode_struct`, `posexplode_array`, or `posexplode_map`; adds a zero-based Long ordinal field. | `implemented` | Choose the typed helper matching the input type and declare the ordinal/output Schema. |
| `posexplode_outer(col)` | yes | yes | `posexplode_outer_struct`, `posexplode_outer_array`, or `posexplode_outer_map`; adds a nullable ordinal and preserves null/empty input rows. | `implemented` | Choose an outer typed helper and declare nullable ordinal/output fields. |
| `inline(col)` | yes | yes | `inline_struct`; inlines declared fields from an `array<struct>` into the generated scope. | `implemented` | Use `inline_struct(..., as_=Schema)` to keep the output shape explicit. |
| `inline_outer(col)` | yes | yes | `inline_outer_struct`; inlines `array<struct>` fields while retaining null/empty input rows. | `implemented` | Use `inline_outer_struct(..., as_=Schema)` with nullable generated fields. |

The target indexes list these functions under Generator Functions: [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html). The typed helper
families and their array/map/struct input constraints are detailed in the [Collections API](../../api/Collections.api.md).

## Newly Reconciled JSON and CSV Functions

The official 3.5.6 and 4.0.0 function indexes contain these ten common names. Their typed forms preserve Structure's
declared-schema boundary: parsing schemas, literal JSON paths, immutable options, and literal-only schema inference are
all explicit rather than inferred during execution.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `from_json(col, schema, options=None)` | yes | yes | `from_json(value, *, as_, options=None)`; explicit Structure Schema for Struct output and immutable JSON options; nullable result. PySpark's Array/Map root schemas are not modeled. | `caller-owned-guided` | Use the typed helper for Struct output; retain Array/Map root parsing in native PySpark. |
| `to_json(col, options=None)` | yes | yes | `to_json(value, *, options=None)`; accepts typed Struct, Array, or Map and returns nullable String. | `implemented` | Use an expression with declared nested types and an immutable options record. |
| `from_csv(col, schema, options=None)` | yes | yes | `from_csv(value, *, as_, options=None)`; explicit Structure Schema and immutable CSV options; nullable result. | `implemented` | Declare the parsed Struct schema and literal parser options. |
| `to_csv(col, options=None)` | yes | yes | `to_csv(value, *, options=None)`; accepts a typed Struct and returns nullable String. | `implemented` | Use a declared Struct expression and immutable options. |
| `get_json_object(col, path)` | yes | yes | `get_json_object(value, path)`; non-empty literal JSON path, nullable String output. | `implemented` | Use a literal path; declare a schema with `from_json` for typed nested access. |
| `json_array_length(col)` | yes | yes | `json_array_length(value)`; nullable Integer count for the outermost JSON array. | `implemented` | Use for array-root JSON text; malformed or non-array input follows Spark's nullable behavior. |
| `json_object_keys(col)` | yes | yes | `json_object_keys(value)`; nullable `Array[String]` for the outermost object keys. | `implemented` | Use for object-root JSON text; key order is not a contract. |
| `json_tuple(col, *fields)` | yes | yes | `json_tuple(value, *, as_, fields=None)`; declared nullable-String output schema, top-level keys, row-preserving scope. | `implemented` | Declare the output field names and optional literal member mapping; use `from_json` for typed/nested data. |
| `schema_of_json(json, options=None)` | yes | yes | `schema_of_json(value, options=None)`; non-empty text literal plus immutable options; non-null SQL-format String. | `implemented` | Supply a compile-time JSON example; use `from_json` with an explicit Schema for row-dependent data. |
| `schema_of_csv(csv, options=None)` | yes | yes | `schema_of_csv(value, options=None)`; non-empty text literal plus immutable options; non-null SQL-format String. | `implemented` | Supply a compile-time CSV example; use `from_csv` with an explicit Schema for row-dependent data. |

Both official indexes list the same JSON/CSV names and signatures: [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html). Schema inference
from a runtime input remains intentionally unsupported because it cannot change a compiled output Schema.

## Newly Reconciled Variant Functions and TVFs

Variant was introduced after the 3.5 line. The functions below are therefore target-gated for the shared 3.5.6/4.0.0
baseline, even where PySpark 4.0.0 provides the operation. `variant_explode` and `variant_explode_outer` are table-valued
functions, not `pyspark.sql.functions` exports; they remain in the inventory only to make the TVF boundary explicit.
`variant_literal` is a Structure-only typed constructor and is tracked as a Structure extension, not as a PySpark API.

| PySpark function / TVF | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `parse_json(col)` | no | yes | `target-gated` | Structure has typed Variant construction and access, but does not admit PySpark 4.0 Variant parsing into the shared baseline. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `try_parse_json(col)` | no | yes | `target-gated` | Nullable Variant parse semantics are outside the shared baseline. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `schema_of_variant(col)` | no | yes | `target-gated` | Runtime Variant schema inference is not a shared-baseline contract. | Use native PySpark 4.0+ where Variant schema inference is required. |
| `schema_of_variant_agg(col)` | no | yes | `target-gated` | Aggregate Variant schema inference is not a shared-baseline contract. | Use native PySpark 4.0+ where aggregate inference is required. |
| `variant_get(col, path, schema=None)` | no | yes | `target-gated` | Typed path access exists only within the separately gated Variant surface. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `try_variant_get(col, path, schema=None)` | no | yes | `target-gated` | Nullable path access is not part of the shared-baseline contract. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `to_variant_object(col)` | no | yes | `target-gated` | Variant object conversion is not part of the shared-baseline contract. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `is_variant_null(col)` | no | yes | `target-gated` | Variant-specific null testing is not part of the shared-baseline contract. | Use native PySpark 4.0+ or a separately admitted Variant profile. |
| `is_valid_variant(col)` | no | no | `target-gated` | Not present in the reviewed 3.5.6/4.0.0 function indexes; later-version availability is outside this baseline. | Use only with a separately verified PySpark target profile. |
| `variant_explode(input)` | no | yes | `target-gated` | PySpark 4.0 TVF; a row-expanding result needs an explicit typed output contract. | Use native PySpark 4.0+ at a declared schema boundary. |
| `variant_explode_outer(input)` | no | yes | `target-gated` | PySpark 4.0 TVF; outer row/cardinality and nullable output contracts are not admitted here. | Use native PySpark 4.0+ at a declared schema boundary. |
| `variant_array_append(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; no released baseline profile or complete execution evidence is admitted. | Use native PySpark only on a separately verified target profile. |
| `try_variant_array_append(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. | Use native PySpark only on a separately verified target profile. |
| `variant_insert(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; mutation and path-conflict behavior remain profile-gated. | Use native PySpark only on a separately verified target profile. |
| `try_variant_insert(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. | Use native PySpark only on a separately verified target profile. |
| `variant_set(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; mutation and path-conflict behavior remain profile-gated. | Use native PySpark only on a separately verified target profile. |
| `try_variant_set(col, path, value)` | no | no | `target-gated` | Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. | Use native PySpark only on a separately verified target profile. |
| `variant_delete(col, path)` | no | no | `target-gated` | Absent from both reviewed function indexes; deletion/path behavior remains profile-gated. | Use native PySpark only on a separately verified target profile. |

The reviewed target sources are the official [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) function indexes;
the two explode forms are listed as Spark 4.0 TVFs. The separate Variant mutation gate above remains in force until a
released target profile and its classic, Connect, generated/online, and streaming evidence are recorded.

## Newly Reconciled Map and Struct Functions

The selected inventory's 14 common map/struct names are present in the official PySpark 3.5.6 and 4.0.0 function
indexes. Structure already exposes typed constructors, lookup/entry projections, callback helpers, and map composition;
the contracts below make their key, value, callback, and collision rules visible per function.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `create_map(*cols)` | yes | yes | `create_map(*keys_and_values)`; requires alternating typed key/value inputs and non-null keys. | `implemented` | Use alternating expressions; cast to a common key/value type when Spark coercion is needed. |
| `map_from_arrays(keys, values)` | yes | yes | `map_from_arrays(keys, values)`; typed arrays require compatible element families and non-null keys. | `implemented` | Ensure arrays have aligned lengths and keys are non-null. |
| `str_to_map(text, pairDelim=None, keyValueDelim=None)` | yes | yes | `str_to_map(value, pair_delimiter=",", key_value_delimiter=":")`; delimiters are non-empty String literals. | `implemented` | Supply literal delimiters when the input format differs from Spark defaults. |
| `named_struct(*cols)` | yes | yes | `named_struct(name, value, ...)`; field names are unique non-empty String literals and values preserve declared types/nullability. | `implemented` | Use literal field names and explicitly typed values. |
| `map_from_entries(col)` | yes | yes | `map_from_entries(entries)`; requires an array of two-field key/value structs with non-null keys. | `implemented` | Provide entries with the declared key/value field types. |
| `map_keys(col)` | yes | yes | `map_keys(value)` returns a typed array of the map key type. | `implemented` | Use when map-key order is not semantically significant. |
| `map_values(col)` | yes | yes | `map_values(value)` returns a typed array preserving value element nullability. | `implemented` | Use when map-value order is not semantically significant. |
| `map_entries(col)` | yes | yes | `map_entries(value)` returns a typed array of key/value structs. | `implemented` | Use when map-entry order is not semantically significant. |
| `map_contains_key(col, value)` | yes | yes | `map_contains_key(map, key)` returns nullable Boolean and validates key compatibility. | `implemented` | Use a key expression compatible with the declared map key type. |
| `map_concat(*cols)` | yes | yes | `map_concat(*maps, duplicates='error')` requires compatible key/value types and rejects duplicate runtime keys under Spark's exception policy. | `implemented` | Use compatible maps and retain `spark.sql.mapKeyDedupPolicy=EXCEPTION`; use a native boundary for alternate duplicate-key policy. |
| `map_filter(col, f)` | yes | yes | `map_filter(map, lambda key, value: ...)` accepts a symbolic Boolean predicate. | `implemented` | Keep callback logic within the typed expression DSL. |
| `transform_keys(col, f)` | yes | yes | `map_transform_keys(map, lambda key, value: ...)` returns a typed map with the callback result key type. | `implemented` | Keep callback logic symbolic and ensure the transformed key is non-null/unique. |
| `transform_values(col, f)` | yes | yes | `map_transform_values(map, lambda key, value: ...)` returns a typed map with the callback result value type. | `implemented` | Keep callback logic within the typed expression DSL. |
| `map_zip_with(col1, col2, f)` | yes | yes | `map_zip_with(left, right, lambda key, left_value, right_value: ...)` requires matching key types and returns typed merged values. | `implemented` | Use maps with compatible key types and a typed merge expression. |

The official function indexes list these names in the common baseline: [PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html). The
[Collections API](../../api/Collections.api.md) defines typed map construction and callback behavior.

## Newly Reconciled String Functions

The official PySpark 3.5.6 and 4.0.0 indexes include the following exact-name aliases and specialized functions.
The aliases already have typed equivalents; the remaining helpers are explicitly held at a design gate until their
type, locale, and failure contracts are compiler-visible.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Status | Structure equivalent or missing contract | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `ascii(col)` | yes | yes | `implemented` | `ascii(value)` returns nullable Integer for the first character's code. | Use `ascii(...)` directly. |
| `btrim(str, trim=None)` | yes | yes | `implemented` | `btrim(value, *, trim=" ")` accepts typed String expressions for both inputs and trims Spark's exact character set. | Use `btrim(...)` with a String expression or literal trim set. |
| `char(col)` | yes | yes | `implemented` | `char(value)` accepts integral input and returns String. | Use `char(...)` directly. |
| `char_length(col)` | yes | yes | `implemented` | `char_length(value)` returns nullable Integer character length. | Use `char_length(...)` directly. |
| `contains(left, right)` | yes | yes | `implemented` | `contains(value, search)` is a typed String predicate with nullable Boolean result. | Use `contains(...)` or the expression method. |
| `elt(n, *cols)` | yes | yes | `implemented` | `elt(index, *values)` uses Spark's one-based selection and common result type. | Use `elt(...)` with compiler-visible typed choices. |
| `find_in_set(str, str_array)` | yes | yes | `implemented` | `find_in_set(value, values)` returns nullable Integer position. | Use `find_in_set(...)` directly. |
| `format_number(col, d)` | yes | yes | `implemented` | `format_number(value, *, decimals)` returns a nullable formatted String; decimal count is a non-negative literal. | Use `format_number(...)` with a literal decimal count. |
| `format_string(format, *cols)` | yes | yes | `implemented` | `format_string(format, *values)` uses a literal format and typed scalar arguments. | Use `format_string(...)` with a literal format. |
| `left(str, len)` | yes | yes | `implemented` | `left(value, *, length)` returns the requested leading characters; length is a literal integer. | Use `left(...)` directly. |
| `lower(col)` | yes | yes | `implemented` | `lower(value)` returns lowercase String and preserves nullability. | Use `lower(...)` directly. |
| `upper(col)` | yes | yes | `implemented` | `upper(value)` returns uppercase String and preserves nullability. | Use `upper(...)` directly. |
| `trim(col)` | yes | yes | `implemented` | `trim(value)` trims spaces from both ends, matching the shared PySpark signature. | Use `trim(...)` directly. |
| `ltrim(col)` | yes | yes | `implemented` | `ltrim(value)` trims leading spaces, matching the shared PySpark signature. | Use `ltrim(...)` directly. |
| `rtrim(col)` | yes | yes | `implemented` | `rtrim(value)` trims trailing spaces, matching the shared PySpark signature. | Use `rtrim(...)` directly. |
| `lpad(col, len, pad=" ")` | yes | yes | `implemented` | `lpad(value, *, length, pad=" ")` requires compiler-visible length and pad literals. | Use `lpad(...)` with literal padding arguments. |
| `rpad(col, len, pad=" ")` | yes | yes | `implemented` | `rpad(value, *, length, pad=" ")` requires compiler-visible length and pad literals. | Use `rpad(...)` with literal padding arguments. |
| `length(col)` | yes | yes | `implemented` | `length(value)` returns nullable Integer length. | Use `length(...)` directly. |
| `locate(substr, str, pos=1)` | yes | yes | `implemented` | `locate(value, *, substring, position=1)` matches PySpark's literal substring and integer start contract. | Use `locate(...)` directly. |
| `mask(col, upperChar="X", lowerChar="x", digitChar="n", otherChar=" ")` | yes | yes | `implemented` | `mask(value, *, upper, lower, digit, other)` accepts optional single-character literals. | Use `mask(...)` with literal replacement characters. |
| `octet_length(col)` | yes | yes | `implemented` | `octet_length(value)` accepts String or Binary and returns nullable Integer byte length. | Use `octet_length(...)` directly. |
| `overlay(src, replace, pos, len=-1)` | yes | yes | `implemented` | `overlay(value, replace, *, pos, length=-1)` accepts same-family String/Binary expressions and typed integral position/length. | Use `overlay(...)` with compatible String or Binary operands. |
| `position(substr, str, start=1)` | yes | yes | `implemented` | `position(substring, value, *, start=1)` returns nullable Integer from a literal positive start. | Use `position(...)` directly. |
| `printf(format, *cols)` | yes | yes | `implemented` | `printf(format, *values)` uses a literal format and typed scalar arguments. | Use `printf(...)` with a literal format. |
| `regexp_count(str, regexp)` | yes | yes | `implemented` | `regexp_count(value, *, pattern)` returns nullable Integer match count for a literal pattern. | Use `regexp_count(...)` with a literal regex. |
| `regexp_extract(str, pattern, idx=1)` | yes | yes | `implemented` | `regexp_extract(value, *, pattern, group=1)` returns nullable String for a literal pattern/group. | Use `regexp_extract(...)` with compiler-visible regex metadata. |
| `regexp_extract_all(str, regexp, idx=1)` | yes | yes | `implemented` | `regexp_extract_all(value, *, pattern, group=1)` returns nullable Array[String] for a literal pattern/group. | Use `regexp_extract_all(...)` with compiler-visible regex metadata. |
| `regexp_instr(str, regexp)` | yes | yes | `implemented` | `regexp_instr(value, *, pattern, group=0)` returns nullable one-based Integer match position. | Use `regexp_instr(...)` with a literal regex. |
| `regexp_replace(str, pattern, replacement)` | yes | yes | `implemented` | `regexp_replace(value, *, pattern, replacement)` requires literal regex and replacement text. | Use `regexp_replace(...)` with literal regex metadata. |
| `regexp_substr(str, regexp)` | yes | yes | `implemented` | `regexp_substr(value, *, pattern)` returns nullable String for a literal regex. | Use `regexp_substr(...)` with a literal regex. |
| `repeat(col, n)` | yes | yes | `implemented` | `repeat(value, *, count)` matches PySpark's integer count parameter and rejects invalid negative counts before compilation. | Use `repeat(...)` with an integer count. |
| `replace(src, search, replace)` | yes | yes | `caller-owned-guided` | `replace(value, *, search, replacement)` accepts literal search and replacement strings. | Use literal replacements or native PySpark for row-dependent search/replacement values. |
| `right(str, len)` | yes | yes | `implemented` | `right(value, *, length)` returns the requested trailing characters; length is a literal integer. | Use `right(...)` directly. |
| `soundex(col)` | yes | yes | `implemented` | `soundex(value)` returns nullable SoundEx String. | Use `soundex(...)` directly. |
| `split_part(src, delimiter, partNum)` | yes | yes | `implemented` | `split_part(value, delimiter, part_num)` accepts typed String delimiter and integral part number; zero is rejected. | Use `split_part(...)` directly. |
| `substr(str, pos, len)` | yes | yes | `implemented` | `substr(value, *, start, length)` accepts typed integral expressions and preserves the PySpark spelling. | Use `substr(...)` directly. |
| `substring(str, pos, len)` | yes | yes | `implemented` | `substring(value, *, start, length)` accepts typed integral expressions and preserves Spark's one-based indexing. | Use `substring(...)` directly. |
| `substring_index(str, delim, count)` | yes | yes | `implemented` | `substring_index(value, *, delimiter, count)` uses literal delimiter/count arguments. | Use `substring_index(...)` with literals. |
| `split(str, pattern, limit=-1)` | yes | yes | `implemented` | `split(value, *, pattern, limit=-1)` requires compiler-visible pattern and limit literals. | Use `split(...)` with a literal pattern and limit. |
| `concat_ws(sep, *cols)` | yes | yes | `implemented` | `concat_ws(separator, *values)` accepts a literal separator and typed String or Array[String] values. | Use `concat_ws(...)` with a literal separator. |
| `initcap(col)` | yes | yes | `implemented` | `initcap(value)` returns nullable title-cased String. | Use `initcap(...)` directly. |
| `translate(src, matching, replace)` | yes | yes | `implemented` | `translate(value, *, matching, replacement)` uses literal character maps. | Use `translate(...)` with literal maps. |
| `instr(str, substr)` | yes | yes | `implemented` | `instr(value, *, substring)` matches PySpark's literal substring argument and returns nullable Integer. | Use `instr(...)` directly. |
| `levenshtein(left, right)` | yes | yes | `implemented` | `levenshtein(left, right)` returns nullable Integer edit distance for typed String operands. | Use `levenshtein(...)` directly. |
| `url_encode(col)` | yes | yes | `implemented` | `url_encode(value)` applies Spark's URL form-encoding rules to String. | Use `url_encode(...)` directly. |
| `url_decode(col)` | yes | yes | `implemented` | `url_decode(value)` applies Spark's strict URL decoding and preserves failure behavior. | Use `url_decode(...)` when malformed input should fail. |
| `try_url_decode(col)` | no | yes | `target-gated` | No shared-baseline helper; the 4.0 function returns null for malformed input, unlike strict `url_decode`. | Use native PySpark 4.0+ when malformed escapes must become null. |
| `character_length(str)` | yes | yes | `implemented` | `character_length(value)` preserves the spelling and the String/Binary result rules of `char_length`. | Use `character_length(...)` directly. |
| `bit_length(col)` | yes | yes | `implemented` | `bit_length(value)` accepts String or Binary and returns nullable Integer. | Use `bit_length(...)` directly. |
| `startswith(str, prefix)` | yes | yes | `implemented` | `startswith(value, prefix)` accepts String/Binary operands and preserves Spark's mixed-type coercion. | Use `startswith(...)` or the expression method. |
| `endswith(str, suffix)` | yes | yes | `implemented` | `endswith(value, suffix)` accepts String/Binary operands and preserves Spark's mixed-type coercion. | Use `endswith(...)` or the expression method. |
| `lcase(str)` | yes | yes | `implemented` | `lcase(value)` is the exact-name lowercase alias. | Use `lcase(...)` directly. |
| `ucase(str)` | yes | yes | `implemented` | `ucase(value)` is the exact-name uppercase alias. | Use `ucase(...)` directly. |
| `parse_url(url, partToExtract, key=None)` | yes | yes | `design-gated` | Typed String inputs are representable, but malformed-URL behavior and dynamic extraction-part/key semantics need an explicit contract. | Use native PySpark until the URL-part and error/null behavior is admitted. |
| `sentences(string, language=None, country=None)` | yes | yes | `design-gated` | Result is nested `Array[Array[String]]`; locale handling, nullability at both array levels, and Connect behavior need a contract. | Use native PySpark where sentence segmentation or locale choice matters. |
| `to_char(col, format)` | yes | yes | `design-gated` | Numeric picture formatting needs a typed mask grammar, supported input types, and invalid-mask behavior. | Use native PySpark for picture formatting. |
| `to_number(col, format)` | yes | yes | `design-gated` | Picture parsing needs a typed mask grammar, Decimal precision/scale inference, and strict failure behavior. | Use native PySpark for picture parsing. |
| `to_varchar(col, format)` | yes | yes | `design-gated` | It shares picture-format semantics with `to_char`; accepted input families and mask/error rules are not yet modeled. | Use native PySpark for picture formatting. |
| `try_to_number(col, format)` | yes | yes | `design-gated` | Same Decimal and picture-mask contract as `to_number`, plus null-on-format-mismatch behavior. | Use native PySpark when nullable conversion behavior is required. |

Signatures and target presence are drawn from the official
[PySpark 3.5.6 function index](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[PySpark 4.0.0 function index](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html), and
the [`parse_url` reference](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.parse_url.html).

## Newly Reconciled Grouping Metadata

The official 3.5.6 and 4.0.0 function indexes also list subtotal metadata helpers. Structure already represents
grouping sets, rollups, and cubes; `grouping(...)` now preserves PySpark's integer 1/0 result while
`is_grouped(...)` remains the Boolean convenience form.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `grouping(col)` | yes | yes | `grouping(key)`; non-null Integer 1 when the key is omitted from the active subtotal level, otherwise 0. | `implemented` | Use `grouping(...)` for PySpark's integer flag, or `is_grouped(...)` for a Boolean. |
| `grouping_id()` | yes | yes | `grouping_id()`; non-null Integer bitmask for the active subtotal level. | `implemented` | Use `grouping_id()` directly. |

The PySpark 3.5.6 and 4.0.0 references document `grouping(col)` as a 1/0 aggregate flag and support Spark Connect:
[3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html).

## Newly Reconciled Aggregate Gaps

The official 3.5.6 and 4.0.0 function indexes expose these aggregate names, which were absent from the selected
inventory or only indirectly covered by grouped sketch prose. Each row records the current typed contract or the
specific caller-owned boundary and migration remedy; the aggregate slice has focused one-row disposition coverage.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent or missing contract | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `hll_sketch_agg(col, lgConfigK=None)` | yes | yes | `hll_sketch_agg(value, *, lg_config_k=12, where=None)` returns branded HLL state; precision is a compile-time integer literal from 4 through 21. | `implemented` | Declare matching `hll_sketch(lg_config_k=...)` state; do not exchange it as ordinary Binary. |
| `hll_sketch_estimate(col)` | yes | yes | `hll_sketch_estimate(value)` accepts branded HLL state and returns nullable Long. | `implemented` | Use only on HLL values with a compatible Spark sketch profile. |
| `hll_union(col1, col2, allowDifferentLgConfigK=False)` | yes | yes | `hll_union(left, right, *, allow_different_lg_config_k=False)` accepts branded HLL states; mixed precision requires explicit opt-in and emits `SKETCH-W0802`. | `implemented` | Use matching declared precision or explicitly accept Spark's mixed-precision reduction warning. |
| `bitmap_bit_position(col)` | yes | yes | `bitmap_bit_position(value)` maps Integer/Long values to a non-null Long bitmap position. | `implemented` | Use the typed helper before Bitmap construction. |
| `bitmap_bucket_number(col)` | yes | yes | `bitmap_bucket_number(value)` maps Integer/Long values to a non-null Long bucket number. | `implemented` | Use the typed helper when selecting a Bitmap bucket. |
| `bitmap_construct_agg(col)` | yes | yes | `bitmap_construct_agg(value, *, where=None)` builds branded Bitmap state from Integer/Long positions. | `implemented` | Declare Bitmap state; keep serialized state within the compatible Spark profile. |
| `bitmap_or_agg(col)` | yes | yes | `bitmap_or_agg(value, *, where=None)` unions branded Bitmap state with a grouped aggregate. | `implemented` | Use typed Bitmap inputs; use native PySpark for raw Binary sketches. |
| `bitmap_count(col)` | yes | yes | `bitmap_count(value)` accepts branded Bitmap state and returns nullable Long. | `implemented` | Use a typed Bitmap rather than an unbranded Binary payload. |
| `covar_pop(col1, col2)` | yes | yes | `covar_pop(left, right, *, where=None)`; nullable Double population covariance. | `implemented` | Use `covar_pop(...)`; use `covar(...)` for sample covariance. |
| `covar_samp(col1, col2)` | yes | yes | `covar(left, right, *, where=None)` renders to PySpark `covar_samp`; nullable Double sample covariance. | `implemented` | Use `covar(...)` for Structure's sample-covariance spelling. |
| `histogram_numeric(col, nBins)` | yes | yes | `histogram_numeric(value, n_bins, *, as_, where=None)` accepts Integer, Long, Float, and Double values on PySpark 3.5/4.0; Decimal input requires the exact `>=4.0,<4.1` profile because live Spark 3.5.0 execution fails with `ClassCastException`. `n_bins` is a foldable Integer literal from 2 through 2,147,483,647. `as_` declares exactly nullable `x` matching the input and nullable Double `y`; the result is a nullable array with nullable elements and fields. | `implemented` | Declare the bucket Schema explicitly. Cast Decimal to Double only when precision loss is acceptable on Spark 3.5; otherwise use the 4.0 target profile or native PySpark. |
| `hll_union_agg(col, allowDifferentLgConfigK=False)` | yes | yes | `hll_union_agg(value, *, allow_different_lg_config_k=False, where=None)` accepts only branded HLL state and returns nullable HLL with the declared precision. The mixed-precision opt-in warns with `SKETCH-W0802`; Spark may reduce precision. | `implemented` | Use `hll_union_agg(...)`; pin compatible Spark profiles because serialized state is not portable Binary. |
| `count_min_sketch(col, eps, confidence, seed)` / `(col, eps, confidence[, seed])` | yes | yes | Count-Min serialized bytes have no branded type, estimator, union operation, or interoperability contract in Structure. | `caller-owned-guided` | Keep this sketch at a native PySpark boundary; treat its Binary payload as Spark-specific opaque state. |
| `mean(col)` | yes | yes | `mean(value, *, where=None)`; same Spark numeric widening as `avg`, preserving PySpark spelling. | `implemented` | Use `mean(...)` to preserve the PySpark name. |
| `some(col)` | yes | yes | `some(value, *, where=None)`; Boolean any-true aggregate with preserved PySpark spelling. | `implemented` | Use `some(...)`; `bool_or(...)` has equivalent any-true semantics. |
| `std(col)` | yes | yes | `std(value, *, where=None)`; nullable Double alias for sample standard deviation, preserving PySpark spelling. | `implemented` | Use `std(...)` or `stddev_samp(...)`. |
| `percentile_approx(col, percentage[, accuracy])` | yes | yes | `approx_percentile(value, percentage, *, accuracy=...)` renders to `F.percentile_approx`. | `implemented` | Use `approx_percentile(...)` in Structure. |
| `count_distinct(col, *cols)` | yes | yes | `count_distinct(value)` returns a non-null Long for one scalar expression; Structure does not yet accept additional columns. | `caller-owned-guided` | Use `count_distinct(...)` for one value; keep tuple/multi-column distinct counting in native PySpark until its null and schema contract is modeled. |
| `count_if(col)` | yes | yes | `count_if(condition, *, where=None)` accepts a typed Boolean and returns non-null Long. | `implemented` | Use a Boolean expression; only true values are counted. |
| `approx_count_distinct(col, rsd=None)` | yes | yes | `approx_count_distinct(value, *, relative_sd=None, where=None)` returns non-null Long; relative standard deviation is a validated literal in `(0, 0.39]`. | `implemented` | Use `relative_sd=` to preserve PySpark's `rsd` tuning semantics. |
| `approx_percentile(col, percentage, accuracy=10000)` | yes | yes | `approx_percentile(value, percentage, *, accuracy=None, where=None)` accepts one literal scalar percentage and optional positive literal accuracy; result preserves the typed input. | `caller-owned-guided` | Use for one fixed percentile; use native PySpark for a row-valued or array-valued percentile/accuracy. |
| `median(col)` | yes | yes | `median(value, *, where=None)` accepts numeric expressions and returns nullable Double. | `implemented` | Use for an exact median. |
| `percentile(col, percentage, frequency=1)` | yes | yes | `percentile(value, percentage, *, frequency=1, where=None)` returns nullable Double for one literal scalar percentage and positive integer frequency. | `caller-owned-guided` | Use for a fixed scalar percentile; use native PySpark for percentage arrays or row-valued percentage/frequency. |
| `stddev(col)` | yes | yes | `stddev(value, *, where=None)` returns nullable Double sample standard deviation. | `implemented` | Use `stddev(...)` or the equivalent `stddev_samp(...)`. |
| `stddev_pop(col)` | yes | yes | `stddev_pop(value, *, where=None)` returns nullable Double population standard deviation. | `implemented` | Use `stddev_pop(...)` for population statistics. |
| `stddev_samp(col)` | yes | yes | `stddev_samp(value, *, where=None)` returns nullable Double sample standard deviation. | `implemented` | Use `stddev_samp(...)` for sample statistics. |
| `variance(col)` | yes | yes | `variance(value, *, where=None)` returns nullable Double sample variance. | `implemented` | Use `variance(...)` or the equivalent `var_samp(...)`. |
| `var_pop(col)` | yes | yes | `var_pop(value, *, where=None)` returns nullable Double population variance. | `implemented` | Use `var_pop(...)` for population variance. |
| `var_samp(col)` | yes | yes | `var_samp(value, *, where=None)` returns nullable Double sample variance. | `implemented` | Use `var_samp(...)` for sample variance. |
| `corr(col1, col2)` | yes | yes | `corr(left, right, *, where=None)` accepts numeric pairs and returns nullable Double Pearson correlation. | `implemented` | Keep the paired numeric inputs aligned. |
| `skewness(col)` | yes | yes | `skewness(value, *, where=None)` returns nullable Double skewness. | `implemented` | Use for Spark's third-moment skewness statistic. |
| `kurtosis(col)` | yes | yes | `kurtosis(value, *, where=None)` returns nullable Double kurtosis. | `implemented` | Use for Spark's kurtosis statistic. |
| `mode(col)` | yes | yes | `mode(value, *, deterministic=False, where=None)` preserves Spark's nullable input type and arbitrary tie choice by default; optional deterministic ties choose the lowest orderable value. | `implemented` | Leave `deterministic=False` for PySpark's default; opt into the portable deterministic tie rule when required. |
| `any_value(col, ignoreNulls=False)` | yes | yes | `any_value(value, *, ignore_nulls=False, where=None)` preserves input type and has nullable result. | `implemented` | Set `ignore_nulls=True` when a null candidate must not be selected. |
| `array_agg(col)` | yes | yes | `array_agg(value, *, element_type=None, where=None)` returns a typed Array and omits null candidates like Spark's aggregate collection behavior. | `implemented` | Supply `element_type=` only when the input expression does not carry enough type information. |
| `bit_and(col)` | yes | yes | `bit_and(value, *, where=None)` accepts Integer/Long and returns nullable Long. | `implemented` | Use only integral input. |
| `bit_or(col)` | yes | yes | `bit_or(value, *, where=None)` accepts Integer/Long and returns nullable Long. | `implemented` | Use only integral input. |
| `bit_xor(col)` | yes | yes | `bit_xor(value, *, where=None)` accepts Integer/Long and returns nullable Long. | `implemented` | Use only integral input. |
| `first(col, ignorenulls=False)` | yes | yes | `first(value, *, ignore_nulls=False, where=None)` preserves input type and is nullable; row choice follows Spark's aggregate input order. | `implemented` | Do not infer a stable order; use an ordered selection API when a deterministic first row is required. |
| `last(col, ignorenulls=False)` | yes | yes | `last(value, *, ignore_nulls=False, where=None)` preserves input type and is nullable; row choice follows Spark's aggregate input order. | `implemented` | Do not infer a stable order; use an ordered selection API when a deterministic last row is required. |
| `max_by(col, ord)` | yes | yes | `max_by(value, order, *, where=None)` returns the candidate associated with the greatest order key; tied keys retain Spark's unspecified choice. | `implemented` | Use an explicit selected-row operation when tie resolution must be deterministic. |
| `min_by(col, ord)` | yes | yes | `min_by(value, order, *, where=None)` returns the candidate associated with the smallest order key; tied keys retain Spark's unspecified choice. | `implemented` | Use an explicit selected-row operation when tie resolution must be deterministic. |
| `product(col)` | yes | yes | `product(value, *, where=None)` accepts numeric input and returns nullable Double. | `implemented` | Use for a numeric product aggregate. |
| `sum_distinct(col)` | yes | yes | `sum_distinct(value, *, where=None)` sums distinct numeric values using Spark's numeric widening. | `implemented` | Use when duplicate input values must contribute once. |
| `regr_avgx(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double mean of the independent (`x`) values. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_avgy(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double mean of the dependent (`y`) values. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_count(y, x)` | yes | yes | Counts paired non-null numeric observations and returns non-null Long. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_intercept(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double intercept. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_r2(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double coefficient of determination. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_slope(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double slope. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_sxx(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of squared independent-variable deviations. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_sxy(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of paired deviations. | `implemented` | Preserve PySpark's `(y, x)` argument order. |
| `regr_syy(y, x)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of squared dependent-variable deviations. | `implemented` | Preserve PySpark's `(y, x)` argument order. |

The source index documents `count_min_sketch` with required `seed` on 3.5.6 and optional `seed` on 4.0.0. It also
documents `some`, `std`, `mean`, and `percentile_approx` in both versions. Spark's built-in function contract says the
histogram `x` field inherits the input numeric type, while `y` contains the bin frequency; it is not a fixed
Double/Double schema. See [PySpark 3.5.6 SQL functions](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html),
[PySpark 4.0.0 SQL functions](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html), and
[Spark 3.5.6 built-in histogram contract](https://spark.apache.org/docs/3.5.6/api/sql/).

## Remaining Shared Function-Index Dispositions

The official function indexes also include common constructors, aliases, event-time helpers, a relation marker, and
the XPath family. These are explicitly classified here rather than left as unexplained source-list differences.

| PySpark function and signature | PySpark 3.5.6 | PySpark 4.0.0 | Structure equivalent or missing contract | Status | Migration remedy |
| --- | --- | --- | --- | --- | --- |
| `lit(value)` | yes | yes | `literal(value)` builds the compiler-visible typed constant. | `implemented` | Use `literal(...)`; its inferred/declared type replaces PySpark's untyped Column literal. |
| `sha(col)` | yes | yes | `sha1(value)` is the equivalent SHA-1 digest helper. | `implemented` | Use `sha1(...)` where preserving the digest semantics matters. |
| `broadcast(df)` | yes | yes | Relation-level broadcast choice is expressed as `lookup_join(..., hint="broadcast")`, not as a row expression. | `caller-owned-guided` | Attach the broadcast hint to the Structure join; retain native PySpark for standalone relation-marker use. |
| `window(timeColumn, windowDuration, slideDuration=None, startTime=None)` | yes | yes | `window(event_time, duration, slide=None, start=None)` builds a typed event-time `TimeWindow`; the keyword-only analytic `window(partition_by=..., order_by=...)` is a separate Structure overload. | `implemented` | Use the positional event-time form for grouped windows and keyword-only arguments for analytic windows. |
| `window_time(window)` | yes | yes | `window_time(window_value)` returns the end-time expression for a typed `TimeWindow`. | `implemented` | Use only with the `TimeWindow` value produced by `window(...)`; chained-state boundaries remain in the Streaming API contract. |
| `xpath(xml, path)` | yes | yes | XML input/output schema, malformed-document behavior, and null semantics are not specified. | `design-gated` | Use native PySpark until the XML result contract is designed. |
| `xpath_boolean(xml, path)` | yes | yes | XPath boolean coercion and XML parse failures lack a typed Structure contract. | `design-gated` | Use native PySpark until XML coercion/failure semantics are specified. |
| `xpath_double(xml, path)` | yes | yes | XPath numeric conversion, nullability, and malformed-document behavior are unspecified. | `design-gated` | Use native PySpark until XML numeric semantics are specified. |
| `xpath_float(xml, path)` | yes | yes | XPath numeric narrowing, nullability, and malformed-document behavior are unspecified. | `design-gated` | Use native PySpark until XML numeric semantics are specified. |
| `xpath_int(xml, path)` | yes | yes | XPath integer narrowing, overflow, nullability, and parse failure behavior are unspecified. | `design-gated` | Use native PySpark until XML integer semantics are specified. |
| `xpath_long(xml, path)` | yes | yes | XPath long conversion, overflow, nullability, and parse failure behavior are unspecified. | `design-gated` | Use native PySpark until XML integer semantics are specified. |
| `xpath_number(xml, path)` | yes | yes | XPath number conversion and its return-type/null behavior need an explicit contract. | `design-gated` | Use native PySpark until XML numeric semantics are specified. |
| `xpath_short(xml, path)` | yes | yes | XPath short narrowing, overflow, nullability, and parse failure behavior are unspecified. | `design-gated` | Use native PySpark until XML integer semantics are specified. |
| `xpath_string(xml, path)` | yes | yes | XPath string extraction and XML parse/null behavior lack a typed Structure contract. | `design-gated` | Use native PySpark until XML string semantics are specified. |

The XML decisions preserve the existing declared-schema boundary; this index reconciliation does not admit XML execution.
The PySpark names are listed in the [3.5.6](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [4.0.0](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html) function indexes.

## Implementation Candidates

The current register has no unreviewed candidate bucket: every selected spelling has a per-function disposition, and
every non-implemented item is explicitly gated or caller-owned. The following rows summarize those boundaries; they
do not replace the function-level rows above or imply that a family-level candidate is ready to implement.

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| UTF-8 validation | target-gated | PySpark 4.0-only validation helpers are outside the default intersection baseline. | Use native PySpark or add a versioned UTF-8 profile. |
| Temporal API additions | implemented / individually gated | The common temporal names have individual target presence, contract, status, and remedy rows in [Newly Reconciled Temporal Functions](#newly-reconciled-temporal-functions). | Follow the relevant row; target-only forms remain in the versioned section. |
| Current-time | implemented | Typed query-clock calls preserve six spellings and same-query equality metadata. | Use native PySpark for unmodeled timestamp variants. |
| AES-GCM | implemented | Typed GCM calls, symbolic keys, and nonce-risk warning. | Use native PySpark for excluded AES forms. |
| String aggregation | target-gated | PySpark 4.0-only `string_agg`/`listagg` are outside the 3.5/4.0 intersection baseline. | Use native PySpark on the 4.0 target or await a target-profile admission. |

## Target-Line Additions Outside the Default Baseline

These documented APIs exist in only one reviewed PySpark target line, so they are not default-baseline obligations.
The 3.5-only rows are retained because they appear in the public 3.5.6 index; undocumented compatibility attributes
from a 3.5 module are not treated as public API. The 4.0-only rows also distinguish APIs that are version-gated from
APIs whose additional design boundary is recorded below.

| PySpark API | Present in | Status | Migration remedy |
| --- | --- | --- | --- |
| `approxCountDistinct` | 3.5.6 | `target-gated` | Migrate to `approx_count_distinct(...)`, whose snake-case spelling is present in both target lines. |
| `bitwiseNOT` | 3.5.6 | `target-gated` | Migrate to `bitwise_not(...)`, the common snake-case SQL function; `Column.bitwise_not()` is also available. |
| `countDistinct` | 3.5.6 | `target-gated` | Migrate to `count_distinct(...)`, whose snake-case spelling is present in both target lines. |
| `sumDistinct` | 3.5.6 | `target-gated` | Migrate to `sum_distinct(...)`, whose snake-case spelling is present in both target lines. |
| `toDegrees` | 3.5.6 | `target-gated` | Migrate to `degrees(...)`, which is present in both target lines. |
| `toRadians` | 3.5.6 | `target-gated` | Migrate to `radians(...)`, which is present in both target lines. |
| `collate` | 4.0.0 | `target-gated` | Use native PySpark; collation and comparison semantics require a separately designed profile. |
| `collation` | 4.0.0 | `target-gated` | Use native PySpark; collation metadata has no baseline typed equivalent. |
| `dayname` | 4.0.0 | `target-gated` | Use native PySpark until locale and formatting semantics have a typed contract. |
| `from_xml` | 4.0.0 | `target-gated` | Use native PySpark at a declared-schema boundary; see the XML design gate below. |
| `is_valid_utf8` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted UTF-8 profile. |
| `listagg` | 4.0.0 | `target-gated` | Use native PySpark; aggregate ordering and output semantics need a versioned profile. |
| `listagg_distinct` | 4.0.0 | `target-gated` | Use native PySpark; distinct aggregation semantics need a versioned profile. |
| `make_valid_utf8` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted UTF-8 profile. |
| `monthname` | 4.0.0 | `target-gated` | Use native PySpark until locale and formatting semantics have a typed contract. |
| `nullifzero` | 4.0.0 | `target-gated` | Use native PySpark or express the operation with baseline `nullif(value, 0)`. |
| `randstr` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted random-function profile with explicit seed semantics. |
| `schema_of_xml` | 4.0.0 | `target-gated` | Use native PySpark at a declared-schema boundary; see the XML design gate below. |
| `session_user` | 4.0.0 | `target-gated` | Use native PySpark; runtime identity reads remain caller-owned. |
| `string_agg` | 4.0.0 | `target-gated` | Use native PySpark; aggregate ordering and output semantics need a versioned profile. |
| `string_agg_distinct` | 4.0.0 | `target-gated` | Use native PySpark; distinct aggregation semantics need a versioned profile. |
| `timestamp_add` | 4.0.0 | `target-gated` | Use native PySpark or compose baseline timestamp/interval operations where their semantics match. |
| `timestamp_diff` | 4.0.0 | `target-gated` | Use native PySpark or compose baseline extraction/arithmetic where their semantics match. |
| `to_xml` | 4.0.0 | `target-gated` | Use native PySpark at a declared-schema boundary; see the XML design gate below. |
| `try_make_interval` | 4.0.0 | `target-gated` | Use native PySpark until nullable invalid-component behavior is specified for an admitted profile. |
| `try_make_timestamp` | 4.0.0 | `target-gated` | Use native PySpark until nullable invalid-component behavior is specified for an admitted profile. |
| `try_make_timestamp_ltz` | 4.0.0 | `target-gated` | Use native PySpark until nullable invalid-component behavior is specified for an admitted profile. |
| `try_make_timestamp_ntz` | 4.0.0 | `target-gated` | Use native PySpark until nullable invalid-component behavior is specified for an admitted profile. |
| `try_mod` | 4.0.0 | `target-gated` | Use native PySpark; overflow and invalid-divisor behavior need a versioned numeric contract. |
| `try_parse_url` | 4.0.0 | `target-gated` | Use native PySpark; nullable malformed-URL behavior needs a typed contract. |
| `try_reflect` | 4.0.0 | `target-gated` | Use native PySpark; runtime reflection remains caller-owned. |
| `try_url_decode` | 4.0.0 | `target-gated` | Live check: absent in PySpark 3.5.0; present in 4.0.0 and returns null for malformed `%G1`. It remains outside the shared 3.5/4.0 baseline; use native PySpark on 4.0. |
| `try_validate_utf8` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted UTF-8 profile. |
| `uniform` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted random-function profile with explicit seed semantics. |
| `validate_utf8` | 4.0.0 | `target-gated` | Use native PySpark or a separately admitted UTF-8 profile. |
| `random`, `uuid`, and related random helpers | 4.1 | `target-gated` | Track in the V11 adoption ledger with explicit version and seed evidence. |
| Native `st_geomfromwkb`, `st_geogfromwkb`, `st_asbinary`, `st_srid`, `st_setsrid` | 4.1 | `target-gated` | Track native Geometry/Geography in P10012602 with provider and mode evidence. |

Source comparison: [PySpark 3.5.6 SQL functions](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html)
and [PySpark 4.0.0 SQL functions](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html).

## Design-Gated or Boundary Items

These entries remain visible for migration planning, but are not implementation promises. The owner documents the
missing contract or native-PySpark remedy before the status can change.

| Scope | Status | Missing contract or boundary | Migration remedy |
| --- | --- | --- | --- |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed ownership. | Use a native PySpark boundary. |
| Dynamic JSON | `caller-owned-guided` | Runtime inference cannot alter Schema. | Use declared parsing or native code. |
| Sketch/bitmap | `implemented` | Baseline HLL/Bitmap opaque types and consumers are implemented and live-verified; KLL/Theta remain profile-gated, with profile-specific runtime evidence pending. | Use native PySpark for unsupported profiles or Count-Min/observation metrics. |
| Generic generators | `caller-owned-guided` | Only `stack` has a fixed typed result contract; raw generators lack static schema and cardinality. | Use native PySpark at a declared result boundary. |
| Writer partition transforms | `caller-owned-guided` | Output file layout remains separate from relation distribution and caller-owned. | Use native PySpark writer partition transforms. |
| Variant mutation | `target-gated` | Released profile plus classic, Connect, generated/online, and streaming evidence. | Use the profile or native PySpark. |
| XML | `design-gated` | Declared schema, normalized options, malformed-record policy, and parse/serialize evidence. | Use a native PySpark boundary. |
| URL | `partial` | `url_encode` and strict `url_decode` are baseline row-local; `try_url_decode` is 4.0 target-gated. | Use native PySpark for safe decoding or unsupported profiles. |
| Geospatial providers | `target-gated` | Native root `st_*` is 4.1+; external providers are namespaced and scope-matched. | Use native PySpark or an explicit Binary boundary. |
| Runtime metadata and reflection | `caller-owned-guided` | Query-clock expressions are the only admitted symbolic runtime reads. | Use an explicit native PySpark boundary. |
| UDTFs, pandas UDFs, callbacks | `caller-owned-guided` | Arbitrary runtime behavior. | Use explicit native PySpark. |

## Reconciliation Rules

Every baseline function receives exactly one row in the completed register. Each row records PySpark 3.5.x and 4.0.x
presence, PySpark signature, Structure symbol or equivalent, status, evidence, and migration remedy. A name absent from
either target line is not a default-baseline implementation task; it belongs in the target adoption ledger.

Parity means behavior, not merely name parity. Reconciliation covers argument order and literal rules, result type,
nullability, determinism, row cardinality, error behavior, ordering, target profile, and streaming classification.
Structure may use a more explicit typed API, but the gap row must show how a PySpark user maps the operation.

## Ownership and Updates

The owning ExecPlan is [P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md). Update this table
with [Parity](../../compatibility/APITracker.md), [Function Gates](../gated/Functions.gates.md),
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md), the public catalog, capability ledgers, API references,
and tests whenever a disposition changes.
