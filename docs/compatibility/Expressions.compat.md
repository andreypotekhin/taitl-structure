# Expressions Compatibility

This is the compatibility companion to the [API reference](../api/Expressions.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Conditional, Assertion, Predicate, and Null Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `when(...)` | `when` | `when(o.total > 0, "paid").otherwise("free")` | yes | yes | Builds a typed conditional expression; chain `.when(...)` and finish with `.otherwise(...)` as needed. |
| `coalesce(...)` | `coalesce` | `coalesce(order.primary, order.fallback)` | yes | yes | Scalar `coalesce(*values)` requires at least two expressions; relation partition coalescing uses keyword-only `coalesce(partitions=...)`. |
| `nullif(...)` | `nullif` | `nullif(o.status, "unknown")` | yes | yes | Returns the first typed value unless equal, and is nullable because a match yields null. |
| `nvl(...)` | `nvl` | `nvl(o.discount, 0)` | yes | yes | Is the exact-name null fallback. |
| `nvl2(...)` | `nvl2` | `nvl2(o.code, "known", "missing")` | yes | yes | Preserves the typed branch result contract. |
| `ifnull(...)` | `ifnull` | `ifnull(o.discount, 0)` | yes | yes | Is the exact-name null fallback. |
| `zeroifnull(...)` | `zeroifnull` | `zeroifnull(o.total)` | no | yes | Preserves the input numeric type and substitutes zero only for null; the 3.5 renderer/evaluator use typed `coalesce(value, 0)` because PySpark added the named wrapper in 4.0.0. |
| `assert_true(...)` | `assert_true` | `where(assert_true(o.total > 0, message="total must be positive"))` | yes | yes | Is a typed Boolean guard; use `.isNull()` when composing PySpark's null-on-success contract as a predicate. |
| `raise_error(...)` | `raise_error` | `where(raise_error("unexpected row"))` | yes | yes | Is a typed error expression with a compiler-visible message. |
| `equal_null(...)` | `equal_null` | `equal_null(o.code, "A")` | yes | yes | Returns non-null Boolean and treats two nulls as equal. |
| `like(...)` | `like` | `like(o.name, "A%")` | yes | yes | Accepts typed String operands and returns nullable Boolean. |
| `ilike(...)` | `ilike` | `ilike(o.name, "A%")` | yes | yes | Accepts typed String operands and preserves Spark's case-insensitive matching semantics. |
| `regexp(...)` | `regexp` | `regexp(o.name, "^A")` | yes | yes | Is the exact-name regular-expression predicate. |
| `regexp_like(...)` | `regexp_like` | `regexp_like(o.name, "^A")` | yes | yes | Preserves the PySpark spelling and accepts typed String operands. |
| `rlike(...)` | `rlike` | `rlike(o.name, "^A")` | yes | yes | Is the regular-expression predicate alias. |
| `isnull(...)` | `isnull` | `isnull(o.score)` | yes | yes | Returns a non-null Boolean null test. |
| `isnotnull(...)` | `isnotnull` | `isnotnull(o.score)` | yes | yes | Returns a non-null Boolean null test. |
| `isnan(...)` | `isnan` | `isnan(o.score)` | yes | yes | Distinguishes floating NaN from SQL null. |
| `nanvl(...)` | `nanvl` | `nanvl(o.score, 0.0)` | yes | yes | Substitutes only for NaN; SQL null remains distinct. |

## Numeric Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `ceiling(...)` | `ceiling` | `ceiling(order.value)` | yes | yes | Typed numeric alias of `ceil`, rendered as `F.ceiling(...)`. |
| `negate(...)` | `negate` | `negate(order.value)` | yes | yes | Typed numeric unary negation, rendered as `F.negate(...)`. |
| `negative(...)` | `negative` | `negative(order.value)` | yes | yes | Typed numeric unary negation, rendered as `F.negative(...)`. |
| `positive(...)` | `positive` | `positive(order.value)` | yes | yes | Typed numeric unary plus, rendered as `F.positive(...)`. |
| `power(...)` | `power` | `power(o.value, o.exponent)` | yes | yes | Typed Double result alias of `pow`, rendered as `F.power(...)`. |
| — | `try_add` | — | yes | yes | Status: `design-gated`. Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. Migration: Use native PySpark until the safe-arithmetic contract is closed. |
| — | `try_divide` | — | yes | yes | Status: `design-gated`. Always floating-point division; zero divisor yields null; Spark also accepts interval operands. Migration: Use native PySpark until numeric and interval overloads are specified. |
| — | `try_multiply` | — | yes | yes | Status: `design-gated`. Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. Migration: Use native PySpark until the safe-arithmetic contract is closed. |
| — | `try_subtract` | — | yes | yes | Status: `design-gated`. Spark returns null on overflow; mixed numeric and ANSI-specific behavior needs a Structure contract. Migration: Use native PySpark until the safe-arithmetic contract is closed. |
| — | `try_avg` | — | yes | yes | Status: `design-gated`. Aggregate returns null on overflow; aggregate widening, filtering, and streaming semantics need a Structure contract. Migration: Use native PySpark until the safe-aggregate contract is closed. |
| — | `try_sum` | — | yes | yes | Status: `design-gated`. Aggregate returns null on overflow; aggregate widening, filtering, and streaming semantics need a Structure contract. Migration: Use native PySpark until the safe-aggregate contract is closed. |

## Numeric Functions — Common numeric expressions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `abs(...)` | `abs` | `abs(o.total)` | yes | yes | Accepts typed numeric input, preserves its type, and propagates nullability. |
| `acos(...)` | `acos` | `acos(o.total)` | yes | yes | Accepts numeric input and returns nullable Double radians. |
| `acosh(...)` | `acosh` | `acosh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `asin(...)` | `asin` | `asin(o.value)` | yes | yes | Accepts numeric input and returns nullable Double radians. |
| `asinh(...)` | `asinh` | `asinh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `atan(...)` | `atan` | `atan(o.angle)` | yes | yes | Accepts numeric input and returns nullable Double radians. |
| `atan2(...)` | `atan2` | `atan2(o.y, o.x)` | yes | yes | Accepts numeric expressions and returns nullable Double radians. |
| `atanh(...)` | `atanh` | `atanh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `bin(...)` | `bin` | `bin(o.flags)` | yes | yes | Accepts Integer/Long and returns nullable Binary-format String. |
| `bround(...)` | `bround` | `bround(o.total, scale=2)` | yes | yes | Uses half-even rounding and preserves the typed numeric result. |
| `cbrt(...)` | `cbrt` | `cbrt(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `ceil(...)` | `ceil` | `ceil(o.total)` | yes | yes | Returns Spark's ceiling result type and preserves input nullability. |
| `conv(...)` | `conv` | `conv(o.digits, from_base=2, to_base=16)` | yes | yes | Accepts String plus validated integer base literals and returns nullable String. |
| `cos(...)` | `cos` | `cos(o.angle)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `cosh(...)` | `cosh` | `cosh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `cot(...)` | `cot` | `cot(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `csc(...)` | `csc` | `csc(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `degrees(...)` | `degrees` | `degrees(o.angle)` | yes | yes | Accepts numeric radians and returns nullable Double degrees. |
| `e(...)` | `e` | `e()` | yes | yes | Returns a non-null Double expression for Euler's number. |
| `exp(...)` | `exp` | `exp(o.total)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `expm1(...)` | `expm1` | `expm1(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `factorial(...)` | `factorial` | `factorial(o.count)` | yes | yes | Accepts Integer/Long and returns nullable Long. |
| `floor(...)` | `floor` | `floor(o.total)` | yes | yes | Returns Spark's floor result type and preserves input nullability. |
| `greatest(...)` | `greatest` | `greatest(o.left, o.right)` | yes | yes | Requires at least two compatible expressions, skips nulls, and returns null only when all are null. |
| `hex(...)` | `hex` | `hex(o.id)`; `unhex(o.value)` | yes | yes | Accepts Integer/Long or Binary and returns nullable hexadecimal String. |
| `hypot(...)` | `hypot` | `hypot(o.x, o.y)` | yes | yes | Accepts numeric expressions and returns nullable Double. |
| `least(...)` | `least` | `least(o.left, o.right)` | yes | yes | Requires at least two compatible expressions, skips nulls, and returns null only when all are null. |
| `ln(...)` | `ln` | `ln(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `log(...)` | `log` | `log(o.value, base=10)` | yes | yes | Status: `caller-owned-guided`. accepts a typed value and an optional positive finite literal base; generated calls preserve PySpark argument order. Migration: Use `base=` for literal bases; use native PySpark for a row-dependent base. |
| `log10(...)` | `log10` | `log10(o.amount)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `log1p(...)` | `log1p` | `log1p(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `log2(...)` | `log2` | `log2(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `pi(...)` | `pi` | `pi()` | yes | yes | Returns a non-null Double expression for π. |
| `pmod(...)` | `pmod` | `pmod(o.value, 7)` | yes | yes | Accepts numeric expressions, preserves their common type, and propagates nullability. |
| `pow(...)` | `pow` | `pow(o.total, 2)` | yes | yes | Accepts numeric expressions and returns nullable Double. |
| `radians(...)` | `radians` | `radians(order.value)` | yes | yes | Accepts numeric degrees and returns nullable Double radians. |
| `rint(...)` | `rint` | `rint(order.value)` | yes | yes | Accepts numeric input and returns nullable Double using nearest-integer rounding. |
| `round(...)` | `round` | `round(o.total, scale=2)` | yes | yes | Uses half-up rounding and preserves the typed numeric result. |
| `sec(...)` | `sec` | `sec(order.value)` | yes | yes | Accepts numeric radians and returns nullable Double. |
| `sign(...)` | `sign` | `sign(order.value)` | yes | yes | Returns nullable Double signum and preserves the PySpark spelling. |
| `signum(...)` | `signum` | `signum(o.total)` | yes | yes | Returns nullable Double signum. |
| `sin(...)` | `sin` | `sin(o.angle)` | yes | yes | Accepts numeric input and returns nullable Double radians. |
| `sinh(...)` | `sinh` | `sinh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `sqrt(...)` | `sqrt` | `sqrt(o.total)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `tan(...)` | `tan` | `tan(order.value)` | yes | yes | Accepts numeric input and returns nullable Double radians. |
| `tanh(...)` | `tanh` | `tanh(order.value)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `unhex(...)` | `unhex` | `hex(o.id)`; `unhex(o.value)` | yes | yes | Accepts String and returns nullable Binary. |
| `width_bucket(...)` | `width_bucket` | `width_bucket(o.value, 0, 100, num_buckets=10)` | yes | yes | Status: `caller-owned-guided`. accepts numeric value/range expressions and a positive integer literal; it returns nullable Long, matching Spark's LongType output. Migration: Use a literal bucket count; use native PySpark when the count is row-dependent. |

## Bitwise, Hash, Encoding, and Crypto Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `bitwise_not(...)` | `bitwise_not` | `bitwise_not(o.flags)` | yes | yes | Typed SQL-function form preserves the input Integer/Long type and nullability; the Column method remains available. |
| `bit_count(...)` | `bit_count` | `bit_count(o.flags)` | yes | yes | Returns Long for Integer/Long input and preserves nullability. |
| `bit_get(...)` | `bit_get` | `bit_get(o.flags, o.position)` | yes | yes | Requires integral inputs and returns nullable Integer. |
| `getbit(...)` | `getbit` | `getbit(order.value)` | yes | yes | Preserves the SQL spelling and shares the integral position contract. |
| `shiftleft(...)` | `shiftleft` | `shiftleft(o.flags, bits=2)` | yes | yes | Status: `caller-owned-guided`. supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. Migration: Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |
| `shiftright(...)` | `shiftright` | `shiftright(order.value)` | yes | yes | Status: `caller-owned-guided`. supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. Migration: Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |
| `shiftrightunsigned(...)` | `shiftrightunsigned` | `shiftrightunsigned(order.value)` | yes | yes | Status: `caller-owned-guided`. supports a compiler-visible integer literal; PySpark also permits dynamic shift expressions. Migration: Use a literal `bits=` value, or native PySpark when the shift count is row-dependent. |
| `hash(...)` | `hash` | `hash(o.tenant, o.id)` | yes | yes | Returns Integer and preserves Spark's multi-column hash behavior. |
| `xxhash64(...)` | `xxhash64` | `xxhash64(o.tenant, o.id)` | yes | yes | Returns Long and preserves Spark's 64-bit hash behavior. |
| `crc32(...)` | `crc32` | `crc32(o.payload)` | yes | yes | Accepts String/Binary and returns Long. |
| `md5(...)` | `md5` | `md5(o.name)` | yes | yes | Returns the lowercase hexadecimal digest String. |
| `sha1(...)` | `sha1` | `sha1(o.name)` | yes | yes | Returns the hexadecimal digest String. |
| `sha2(...)` | `sha2` | `sha2(o.name, bits=256)` | yes | yes | Accepts supported digest-size literals and returns hexadecimal String. |
| `base64(...)` | `base64` | `base64(o.payload)` | yes | yes | Encodes Binary as String. |
| `unbase64(...)` | `unbase64` | `unbase64(order.value)` | yes | yes | Decodes String to Binary. |
| `encode(...)` | `encode` | `decode(encode(o.name, charset="UTF-8"), charset="UTF-8")` | yes | yes | Accepts a compiler-visible charset literal and returns Binary. |
| `decode(...)` | `decode` | `decode(encode(o.name, charset="UTF-8"), charset="UTF-8")` | yes | yes | Accepts a compiler-visible charset literal and returns String. |
| `to_binary(...)` | `to_binary` | `to_binary(o.token, format="utf-8")` | yes | yes | Validates the format literal and preserves Spark conversion errors. |
| `try_to_binary(...)` | `try_to_binary` | `try_to_binary(order.value)` | yes | yes | Returns null when conversion fails. |
| `aes_encrypt(...)` | `aes_encrypt` | `aes_encrypt(o.payload, key=o.key)` | yes | yes | GCM-only encryption accepts typed String/Binary inputs and symbolic keys. A caller-supplied IV remains available for determinism and external interoperability but warns that the caller owns nonce uniqueness. |
| `aes_decrypt(...)` | `aes_decrypt` | `aes_decrypt(o.payload, key=o.key)` | yes | yes | Strict GCM decryption over typed String/Binary values and symbolic keys. |
| `try_aes_decrypt(...)` | `try_aes_decrypt` | `try_aes_decrypt(o.payload, key=o.key)` | yes | yes | Returns nullable Binary on authentication/decryption failure; accepts typed String/Binary inputs and symbolic keys. |
| `bitwise_and(...)` | `Column.bitwiseAND` | `bitwise_and(order.value)` | yes | yes | Applies integral bitwise AND to compatible Integer/Long expressions and propagates nullability. |
| `bitwise_or(...)` | `Column.bitwiseOR` | `bitwise_or(order.value)` | yes | yes | Applies integral bitwise OR to compatible Integer/Long expressions and propagates nullability. |
| `bitwise_xor(...)` | `Column.bitwiseXOR` | `bitwise_xor(order.value)` | yes | yes | Applies integral bitwise XOR to compatible Integer/Long expressions and propagates nullability. |
| `bitmap_count(...)` | Bitmap equivalents | `bitmap_count(o.bitmap)` | yes | yes | Accepts only branded Bitmap state and returns its nullable Long cardinality. |

## Temporal and Query-Clock Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `add_months(...)` | `add_months` | `add_months(o.day, months=1)` | yes | yes | Accepts an Integer/Long literal or typed integral expression and returns Date. |
| `date_add(...)` | `date_add` | `date_add(o.day, days=1)` | yes | yes | Accepts an Integer/Long literal or typed integral expression and returns Date. |
| `date_sub(...)` | `date_sub` | `date_sub(o.day, days=o.day_offset)` | yes | yes | Accepts an Integer/Long literal or typed integral expression and returns Date. |
| `date_diff(...)` | `date_diff` | `date_diff(o.end_date, o.start_date)` | yes | yes | Exact-name alias of `datediff`; returns nullable Integer day difference. |
| `dateadd(...)` | `dateadd` | `dateadd(o.day, days=1)` | yes | yes | Exact-name alias of `date_add`; accepts typed integral day counts and returns Date. |
| `datediff(...)` | `datediff` | `datediff(o.end_date, o.start_date)` | yes | yes | Returns nullable Integer days between typed Date/Timestamp inputs. |
| `date_trunc(...)` | `date_trunc` | `date_trunc(o.at, unit="month")` | yes | yes | Returns Timestamp; unit is a validated literal or `Temporal` constant. |
| `date_format(...)` | `date_format` | `date_format(order.value)` | yes | yes | Returns nullable String and requires a non-empty format literal. |
| `date_from_unix_date(...)` | `date_from_unix_date` | `date_from_unix_date(o.epoch_days)` | yes | yes | Integral days from 1970-01-01 convert to nullable Date. |
| `unix_date(...)` | `unix_date` | `unix_date(o.day)` | yes | yes | Typed Date converts to nullable Integer days since 1970-01-01. |
| `from_unixtime(...)` | `from_unixtime` | `from_unixtime(o.epoch_seconds, format="yyyy-MM-dd")` | yes | yes | Numeric epoch seconds format to String in Spark's session time zone; format is a literal. |
| `unix_timestamp(...)` | `unix_timestamp` | `unix_timestamp(o.timestamp)` | yes | yes | Parses typed temporal input to Long seconds; the no-input form is nondeterministic but query-stable. |
| `to_unix_timestamp(...)` | `to_unix_timestamp` | `to_unix_timestamp(o.text, format="yyyy-MM-dd")` | yes | yes | Requires a typed String/Date/Timestamp input and returns Long epoch seconds; optional pattern may be typed String. |
| `to_utc_timestamp(...)` | `to_utc_timestamp` | `to_utc_timestamp(o.timestamp, timezone="UTC")` | yes | yes | Converts typed temporal input using a non-empty timezone literal and returns LTZ Timestamp. |
| `from_utc_timestamp(...)` | `from_utc_timestamp` | `from_utc_timestamp(o.timestamp, timezone="UTC")` | yes | yes | Converts typed temporal input using a non-empty timezone literal and returns LTZ Timestamp. |
| `convert_timezone(...)` | `convert_timezone` | `convert_timezone(source_tz="UTC", target_tz="America/Los_Angeles", timestamp_ntz=o.local_time)` | yes | yes | Requires TimestampNTZ and typed String zone expressions; `sourceTz=None` uses Spark's session zone. |
| `make_date(...)` | `make_date` | `make_date(o.year, o.month, o.day)` | yes | yes | Typed Integer/Long components produce nullable Date; invalid components follow Spark ANSI policy. |
| `make_dt_interval(...)` | `make_dt_interval` | `make_dt_interval(days=1, hours=2)` | yes | yes | Typed DayTime interval; integral components and numeric seconds. |
| `make_interval(...)` | `make_interval` | `make_interval(years=1, months=2, days=3)` | yes | yes | Typed mixed Calendar interval with Spark's seven components. |
| `make_timestamp(...)` | `make_timestamp` | `make_timestamp(o.year, o.month, o.day, o.hour, o.minute, o.second)` | yes | yes | Typed components; result follows resolved `spark.sql.timestampType`; invalid values follow ANSI policy. |
| `make_timestamp_ltz(...)` | `make_timestamp_ltz` | `make_timestamp_ltz(o.year, o.month, o.day, o.hour, o.minute, o.second, timezone="UTC")` | yes | yes | Fixed LTZ result with typed components and optional typed String zone. |
| `make_timestamp_ntz(...)` | `make_timestamp_ntz` | `make_timestamp_ntz(o.year, o.month, o.day, o.hour, o.minute, o.second)` | yes | yes | Fixed NTZ wall-clock result with typed components. |
| `make_ym_interval(...)` | `make_ym_interval` | `make_ym_interval(years=1, months=2)` | yes | yes | Typed YearMonth interval expression. |
| `date_part(...)` | `date_part` | `date_part("year", o.day)` | yes | yes | Exact spelling with compiler-visible String/`Temporal` field; Date, Timestamp, and interval inputs. |
| `datepart(...)` | `datepart` | `datepart("year", o.day)` | yes | yes | Exact alias of `date_part` retaining the PySpark spelling and typed-field contract. |
| `extract(...)` | `extract` | `extract("year", o.timestamp)` | yes | yes | SQL-standard field extraction for typed Date, Timestamp, and interval values. |
| `day(...)` | `day` | `day(o.day)` | yes | yes | Exact alias of day-of-month extraction; nullable Integer. |
| `last_day(...)` | `last_day` | `last_day(order.value)` | yes | yes | Date/Timestamp input returns nullable Date at that month's end. |
| `months_between(...)` | `months_between` | `months_between(o.end_date, o.start_date)` | yes | yes | Date/Timestamp inputs return nullable Double; `round_off` is a Boolean literal. |
| `trunc(...)` | `trunc` | `trunc(o.day, unit="month")` | yes | yes | Date-only truncation; `unit` is a validated literal or `Temporal` constant. |
| `year(...)` | `year` | `year(o.day)` | yes | yes | Date/Timestamp input returns nullable Integer year. |
| `month(...)` | `month` | `month(o.day)` | yes | yes | Date/Timestamp input returns nullable Integer month. |
| `dayofmonth(...)` | `dayofmonth` | `dayofmonth(o.day)` | yes | yes | Date/Timestamp input returns nullable Integer day of month. |
| `dayofweek(...)` | `dayofweek` | `dayofweek(o.day)` | yes | yes | Returns nullable Integer with Spark numbering: Sunday=1 through Saturday=7. |
| `dayofyear(...)` | `dayofyear` | `dayofyear(o.day)` | yes | yes | Date/Timestamp input returns nullable Integer day of year. |
| `hour(...)` | `hour` | `hour(o.timestamp)` | yes | yes | Timestamp input returns nullable Integer hour. |
| `minute(...)` | `minute` | `minute(o.timestamp)` | yes | yes | Timestamp input returns nullable Integer minute. |
| `next_day(...)` | `next_day` | `next_day(o.day, day_of_week="Mon")` | yes | yes | Date/Timestamp input and validated weekday literal return nullable Date. |
| `quarter(...)` | `quarter` | `quarter(o.day)` | yes | yes | Date/Timestamp input returns nullable Integer quarter. |
| `second(...)` | `second` | `second(o.timestamp)` | yes | yes | Timestamp input returns nullable Integer second. |
| `to_date(...)` | `to_date` | `to_date(o.date_text)` | yes | yes | String/Date/Timestamp input converts to Date; optional pattern is a non-empty literal. |
| `to_timestamp(...)` | `to_timestamp` | `to_timestamp(o.timestamp_text)` | yes | yes | String/Date/Timestamp input converts to the resolved LTZ/NTZ type; typed String pattern is supported. |
| `to_timestamp_ltz(...)` | `to_timestamp_ltz` | `to_timestamp_ltz(o.timestamp_text)` | yes | yes | Typed String plus optional typed String pattern yields nullable LTZ Timestamp. |
| `to_timestamp_ntz(...)` | `to_timestamp_ntz` | `to_timestamp_ntz(o.wall_time_text)` | yes | yes | Typed String plus optional typed String pattern yields nullable NTZ Timestamp. |
| `try_to_timestamp(...)` | `try_to_timestamp` | `try_to_timestamp(o.timestamp_text)` | yes | yes | Generic resolved timestamp type; invalid String input returns null regardless of ANSI mode. |
| `timestamp_micros(...)` | `timestamp_micros` | `timestamp_micros(order.value)` | yes | yes | Integral microseconds convert to fixed LTZ Timestamp. |
| `timestamp_millis(...)` | `timestamp_millis` | `timestamp_millis(order.value)` | yes | yes | Integral milliseconds convert to fixed LTZ Timestamp. |
| `timestamp_seconds(...)` | `timestamp_seconds` | `timestamp_seconds(order.value)` | yes | yes | Numeric seconds, including fractions, convert to fixed LTZ Timestamp. |
| `unix_micros(...)` | `unix_micros` | `unix_micros(order.value)` | yes | yes | LTZ Timestamp converts to Long microseconds; NTZ is rejected. |
| `unix_millis(...)` | `unix_millis` | `unix_millis(order.value)` | yes | yes | LTZ Timestamp converts to Long milliseconds; NTZ is rejected. |
| `unix_seconds(...)` | `unix_seconds` | `unix_seconds(order.value)` | yes | yes | LTZ Timestamp converts to Long seconds; NTZ is rejected. |
| `weekday(...)` | `weekday` | `weekday(order.value)` | yes | yes | Monday-first zero-based weekday Integer, unlike Spark's Sunday-first `dayofweek`. |
| `weekofyear(...)` | `weekofyear` | `weekofyear(order.value)` | yes | yes | Date/Timestamp input returns nullable ISO week number as Integer. |
| `current_date(...)` | `current_date` | `current_date()` | yes | yes | Date value is fixed at the start of query evaluation. |
| `curdate(...)` | `curdate` | `curdate()` | yes | yes | Exact alias of `current_date()` with the same query-start stability. |
| `current_timestamp(...)` | `current_timestamp` | `current_timestamp()` | yes | yes | LTZ timestamp is fixed at the start of query evaluation. |
| `now(...)` | `now` | `now()` | yes | yes | Exact alias of `current_timestamp()` with query-start stability. |
| `localtimestamp(...)` | `localtimestamp` | `localtimestamp()` | yes | yes | NTZ wall-clock timestamp fixed at query start. |
| `current_timezone(...)` | `current_timezone` | `current_timezone()` | yes | yes | Returns the session's current timezone as String. |

## Random Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `rand(...)` | `rand` | `rand(seed=42)`; `randn(seed=42)` | yes | yes | Returns non-null Double uniformly distributed in `[0.0, 1.0)`. |
| `randn(...)` | `randn` | `rand(seed=42)`; `randn(seed=42)` | yes | yes | Returns non-null standard-normal Double under the same explicit nondeterminism policy. |

## JSON and CSV Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `from_json(...)` | `from_json` | `from_json(o.payload_json, to=Payload)` | yes | yes | Status: `caller-owned-guided`. explicit Structure Schema for Struct output and immutable JSON options; nullable result. PySpark's Array/Map root schemas are not modeled. Migration: Use the typed helper for Struct output; retain Array/Map root parsing in native PySpark. |
| `to_json(...)` | `to_json` | `to_json(order.value)` | yes | yes | Accepts typed Struct, Array, or Map and returns nullable String. |
| `from_csv(...)` | `from_csv` | `from_csv(o.payload_csv, to=Payload)` | yes | yes | Explicit Structure Schema and immutable CSV options; nullable result. |
| `to_csv(...)` | `to_csv` | `to_csv(order.value)` | yes | yes | Accepts a typed Struct and returns nullable String. |
| `get_json_object(...)` | `get_json_object` | `get_json_object(o.payload_json, "$.customer.id")` | yes | yes | Non-empty literal JSON path, nullable String output. |
| `json_array_length(...)` | `json_array_length` | `json_array_length(o.payload_json)` | yes | yes | Nullable Integer count for the outermost JSON array. |
| `json_object_keys(...)` | `json_object_keys` | `json_object_keys(o.payload_json)` | yes | yes | Nullable `Array[String]` for the outermost object keys. |
| `json_tuple(...)` | `json_tuple` | `json_tuple(o.payload_json, to=LegacyFields, fields={"customer_id": "customerId"})` | yes | yes | Declared nullable-String output schema, top-level keys, row-preserving scope. |
| `schema_of_json(...)` | `schema_of_json` | `schema_of_json('{"id": 1}')` | yes | yes | Non-empty text literal plus immutable options; non-null SQL-format String. |
| `schema_of_csv(...)` | `schema_of_csv` | `schema_of_csv(order.value)` | yes | yes | Non-empty text literal plus immutable options; non-null SQL-format String. |

## Variant Functions and TVFs

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | `parse_json` | — | no | yes | Status: `target-gated`. Structure has typed Variant construction and access, but does not admit PySpark 4.0 Variant parsing into the shared baseline. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `try_parse_json` | — | no | yes | Status: `target-gated`. Nullable Variant parse semantics are outside the shared baseline. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `schema_of_variant` | — | no | yes | Status: `target-gated`. Runtime Variant schema inference is not a shared-baseline contract. Migration: Use native PySpark 4.0+ where Variant schema inference is required. |
| — | `schema_of_variant_agg` | — | no | yes | Status: `target-gated`. Aggregate Variant schema inference is not a shared-baseline contract. Migration: Use native PySpark 4.0+ where aggregate inference is required. |
| — | `variant_get` | — | no | yes | Status: `target-gated`. Typed path access exists only within the separately gated Variant surface. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `try_variant_get` | — | no | yes | Status: `target-gated`. Nullable path access is not part of the shared-baseline contract. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `to_variant_object` | — | no | yes | Status: `target-gated`. Variant object conversion is not part of the shared-baseline contract. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `is_variant_null` | — | no | yes | Status: `target-gated`. Variant-specific null testing is not part of the shared-baseline contract. Migration: Use native PySpark 4.0+ or a separately admitted Variant profile. |
| — | `is_valid_variant` | — | no | no | Status: `target-gated`. Not present in the reviewed 3.5.6/4.0.0 function indexes; later-version availability is outside this baseline. Migration: Use only with a separately verified PySpark target profile. |
| — | `variant_explode` | — | no | yes | Status: `target-gated`. PySpark 4.0 TVF; a row-expanding result needs an explicit typed output contract. Migration: Use native PySpark 4.0+ at a declared schema boundary. |
| — | `variant_explode_outer` | — | no | yes | Status: `target-gated`. PySpark 4.0 TVF; outer row/cardinality and nullable output contracts are not admitted here. Migration: Use native PySpark 4.0+ at a declared schema boundary. |
| — | `variant_array_append` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; no released baseline profile or complete execution evidence is admitted. Migration: Use native PySpark only on a separately verified target profile. |
| — | `try_variant_array_append` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. Migration: Use native PySpark only on a separately verified target profile. |
| — | `variant_insert` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; mutation and path-conflict behavior remain profile-gated. Migration: Use native PySpark only on a separately verified target profile. |
| — | `try_variant_insert` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. Migration: Use native PySpark only on a separately verified target profile. |
| — | `variant_set` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; mutation and path-conflict behavior remain profile-gated. Migration: Use native PySpark only on a separately verified target profile. |
| — | `try_variant_set` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; nullable mutation behavior remains profile-gated. Migration: Use native PySpark only on a separately verified target profile. |
| — | `variant_delete` | — | no | no | Status: `target-gated`. Absent from both reviewed function indexes; deletion/path behavior remains profile-gated. Migration: Use native PySpark only on a separately verified target profile. |

## String Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `ascii(...)` | `ascii` | `ascii(order.value)` | yes | yes | Returns nullable Integer for the first character's code. |
| `btrim(...)` | `btrim` | `btrim(o.name, trim=o.trim_chars)` | yes | yes | Accepts typed String expressions for both inputs and trims Spark's exact character set. |
| `char(...)` | `char` | `char(o.code_point)` | yes | yes | Accepts integral input and returns String. |
| `char_length(...)` | `char_length` | `char_length(order.value)` | yes | yes | Returns nullable Integer character length. |
| `contains(...)` | `contains` | `contains(o.name, "Ada")` | yes | yes | Is a typed String predicate with nullable Boolean result. |
| `elt(...)` | `elt` | `elt(2, o.primary, o.fallback)` | yes | yes | Uses Spark's one-based selection and common result type. |
| `find_in_set(...)` | `find_in_set` | `find_in_set(o.name, o.candidates)` | yes | yes | Returns nullable Integer position. |
| `format_number(...)` | `format_number` | `format_number(o.amount, decimals=2)` | yes | yes | Returns a nullable formatted String; decimal count is a non-negative literal. |
| `format_string(...)` | `format_string` | `format_string("id=%s", o.id)` | yes | yes | Uses a literal format and typed scalar arguments. |
| `left(...)` | `left` | `left(o.name, length=3)` | yes | yes | Returns the requested leading characters; length is a literal integer. |
| `lower(...)` | `lower` | `lower(order.value)` | yes | yes | Returns lowercase String and preserves nullability. |
| `upper(...)` | `upper` | `upper(order.value)` | yes | yes | Returns uppercase String and preserves nullability. |
| `trim(...)` | `trim` | `trim(o.name)` | yes | yes | Trims spaces from both ends, matching the shared PySpark signature. |
| `ltrim(...)` | `ltrim` | `ltrim(o.name)` | yes | yes | Trims leading spaces, matching the shared PySpark signature. |
| `rtrim(...)` | `rtrim` | `rtrim(o.name)` | yes | yes | Trims trailing spaces, matching the shared PySpark signature. |
| `lpad(...)` | `lpad` | `lpad(o.code, length=8, pad="0")` | yes | yes | Requires compiler-visible length and pad literals. |
| `rpad(...)` | `rpad` | `rpad(order.value)` | yes | yes | Requires compiler-visible length and pad literals. |
| `length(...)` | `length` | `length(o.name)` | yes | yes | Returns nullable Integer length. |
| `locate(...)` | `locate` | `locate(o.name, substring="Ada", position=1)` | yes | yes | Matches PySpark's literal substring and integer start contract. |
| `mask(...)` | `mask` | `mask(order.value)` | yes | yes | Accepts optional single-character literals. |
| `octet_length(...)` | `octet_length` | `octet_length(o.name)` | yes | yes | Accepts String or Binary and returns nullable Integer byte length. |
| `overlay(...)` | `overlay` | `overlay(order.value)` | yes | yes | Accepts same-family String/Binary expressions and typed integral position/length. |
| `position(...)` | `position` | `position("Ada", o.name, start=1)` | yes | yes | Returns nullable Integer from a literal positive start. |
| `printf(...)` | `printf` | `printf(order.value)` | yes | yes | Uses a literal format and typed scalar arguments. |
| `regexp_count(...)` | `regexp_count` | `regexp_count(o.code, pattern="Ada")` | yes | yes | Returns nullable Integer match count for a literal pattern. |
| `regexp_extract(...)` | `regexp_extract` | `regexp_extract(o.code, pattern="(.*)", group=1)` | yes | yes | Returns nullable String for a literal pattern/group. |
| `regexp_extract_all(...)` | `regexp_extract_all` | `regexp_extract_all(o.code, pattern="(Ada)", group=1)` | yes | yes | Returns nullable Array[String] for a literal pattern/group. |
| `regexp_instr(...)` | `regexp_instr` | `regexp_instr(o.code, pattern="Ada", group=0)` | yes | yes | Returns nullable one-based Integer match position. |
| `regexp_replace(...)` | `regexp_replace` | `regexp_replace(o.code, pattern="-", replacement="")` | yes | yes | Requires literal regex and replacement text. |
| `regexp_substr(...)` | `regexp_substr` | `regexp_substr(o.code, pattern="Ada")` | yes | yes | Returns nullable String for a literal regex. |
| `repeat(...)` | `repeat` | `repeat(o.code, count=2)` | yes | yes | Matches PySpark's integer count parameter and rejects invalid negative counts before compilation. |
| `replace(...)` | `replace` | `replace(o.name, search="-", replacement="_")` | yes | yes | Status: `caller-owned-guided`. accepts literal search and replacement strings. Migration: Use literal replacements or native PySpark for row-dependent search/replacement values. |
| `right(...)` | `right` | `right(order.value)` | yes | yes | Returns the requested trailing characters; length is a literal integer. |
| `soundex(...)` | `soundex` | `soundex(o.name)` | yes | yes | Returns nullable SoundEx String. |
| `split_part(...)` | `split_part` | `split_part(o.path, "/", 2)` | yes | yes | Accepts typed String delimiter and integral part number; zero is rejected. |
| `substr(...)` | `substr` | `substr(o.code, start=1, length=3)` | yes | yes | Accepts typed integral expressions and preserves the PySpark spelling. |
| `substring(...)` | `substring` | `substring(o.code, start=1, length=3)` | yes | yes | Accepts typed integral expressions and preserves Spark's one-based indexing. |
| `substring_index(...)` | `substring_index` | `substring_index(o.path, delimiter="/", count=2)` | yes | yes | Uses literal delimiter/count arguments. |
| `split(...)` | `split` | `split(o.code, pattern="-")` | yes | yes | Requires compiler-visible pattern and limit literals. |
| `concat_ws(...)` | `concat_ws` | `concat_ws("-", o.region, o.code)`; `concat_ws("\u001f", o.path_ids)` for `array<string>` | yes | yes | Accepts a literal separator and typed String or Array[String] values. |
| `initcap(...)` | `initcap` | `initcap(o.name)` | yes | yes | Returns nullable title-cased String. |
| `translate(...)` | `translate` | `translate(o.name, matching="-", replacement="_")` | yes | yes | Uses literal character maps. |
| `instr(...)` | `instr` | `instr(o.name, substring="A")` | yes | yes | Matches PySpark's literal substring argument and returns nullable Integer. |
| `levenshtein(...)` | `levenshtein` | `levenshtein(o.name, "Ada")` | yes | yes | Returns nullable Integer edit distance for typed String operands. |
| `url_encode(...)` | `url_encode` | `url_encode(o.url)` | yes | yes | Applies Spark's URL form-encoding rules to String. |
| `url_decode(...)` | `url_decode` | `url_decode(order.value)` | yes | yes | Applies Spark's strict URL decoding and preserves failure behavior. |
| `character_length(...)` | `character_length` | `character_length(o.name)` | yes | yes | Preserves the spelling and the String/Binary result rules of `char_length`. |
| `bit_length(...)` | `bit_length` | `bit_length(order.value)` | yes | yes | Accepts String or Binary and returns nullable Integer. |
| `startswith(...)` | `startswith` | `startswith(o.name, o.prefix)` | yes | yes | Accepts String/Binary operands and preserves Spark's mixed-type coercion. |
| `endswith(...)` | `endswith` | `endswith(o.name, o.suffix)` | yes | yes | Accepts String/Binary operands and preserves Spark's mixed-type coercion. |
| `lcase(...)` | `lcase` | `lcase(o.name)` | yes | yes | Is the exact-name lowercase alias. |
| `ucase(...)` | `ucase` | `ucase(o.name)` | yes | yes | Is the exact-name uppercase alias. |
| — | `parse_url` | — | yes | yes | Status: `design-gated`. Typed String inputs are representable, but malformed-URL behavior and dynamic extraction-part/key semantics need an explicit contract. Migration: Use native PySpark until the URL-part and error/null behavior is admitted. |
| — | `sentences` | — | yes | yes | Status: `design-gated`. Result is nested `Array[Array[String]]`; locale handling, nullability at both array levels, and Connect behavior need a contract. Migration: Use native PySpark where sentence segmentation or locale choice matters. |
| — | `to_char` | — | yes | yes | Status: `design-gated`. Numeric picture formatting needs a typed mask grammar, supported input types, and invalid-mask behavior. Migration: Use native PySpark for picture formatting. |
| — | `to_number` | — | yes | yes | Status: `design-gated`. Picture parsing needs a typed mask grammar, Decimal precision/scale inference, and strict failure behavior. Migration: Use native PySpark for picture parsing. |
| — | `to_varchar` | — | yes | yes | Status: `design-gated`. It shares picture-format semantics with `to_char`; accepted input families and mask/error rules are not yet modeled. Migration: Use native PySpark for picture formatting. |
| — | `try_to_number` | — | yes | yes | Status: `design-gated`. Same Decimal and picture-mask contract as `to_number`, plus null-on-format-mismatch behavior. Migration: Use native PySpark when nullable conversion behavior is required. |
| `reverse(...)` | `reverse` | `reverse(o.name)` | yes | yes | Reverses the characters of a typed String expression; array reversal is provided separately by `arr_reverse(...)`. |

## Function-index dispositions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `literal(...)` | `lit` | `literal(order.value)` | yes | yes | Builds the compiler-visible typed constant. |
| `sha1(...)` | `sha` | `sha1(o.name)` | yes | yes | Is the equivalent SHA-1 digest helper. |
| — | `xpath` | — | yes | yes | Status: `design-gated`. XML input/output schema, malformed-document behavior, and null semantics are not specified. Migration: Use native PySpark until the XML result contract is designed. |
| — | `xpath_boolean` | — | yes | yes | Status: `design-gated`. XPath boolean coercion and XML parse failures lack a typed Structure contract. Migration: Use native PySpark until XML coercion/failure semantics are specified. |
| — | `xpath_double` | — | yes | yes | Status: `design-gated`. XPath numeric conversion, nullability, and malformed-document behavior are unspecified. Migration: Use native PySpark until XML numeric semantics are specified. |
| — | `xpath_float` | — | yes | yes | Status: `design-gated`. XPath numeric narrowing, nullability, and malformed-document behavior are unspecified. Migration: Use native PySpark until XML numeric semantics are specified. |
| — | `xpath_int` | — | yes | yes | Status: `design-gated`. XPath integer narrowing, overflow, nullability, and parse failure behavior are unspecified. Migration: Use native PySpark until XML integer semantics are specified. |
| — | `xpath_long` | — | yes | yes | Status: `design-gated`. XPath long conversion, overflow, nullability, and parse failure behavior are unspecified. Migration: Use native PySpark until XML integer semantics are specified. |
| — | `xpath_number` | — | yes | yes | Status: `design-gated`. XPath number conversion and its return-type/null behavior need an explicit contract. Migration: Use native PySpark until XML numeric semantics are specified. |
| — | `xpath_short` | — | yes | yes | Status: `design-gated`. XPath short narrowing, overflow, nullability, and parse failure behavior are unspecified. Migration: Use native PySpark until XML integer semantics are specified. |
| — | `xpath_string` | — | yes | yes | Status: `design-gated`. XPath string extraction and XML parse/null behavior lack a typed Structure contract. Migration: Use native PySpark until XML string semantics are specified. |

## Other expression helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `==` | `Column.__eq__` | `o.total == 0` | yes | yes | `assert_true(...)` and `raise_error(...)` are typed Boolean guards. They lower to the matching native PySpark assertion followed by `.isNull()`, so a successful `assert_true(...)` can be used directly in `where(...)` without adding an output field. A false or null condition, or any evaluated `raise_error(...)`, raises Spark's error lazily. Messages are optional string literals for `assert_true(...)` and required string literals for `raise_error(...)`. Compose `~`, `==`, and other ordinary predicates instead of expecting `assert_false` or `assert_equal` aliases. |
| `!=` | `Column.__ne__` | `o.total != 0` | yes | yes | Compares compatible scalar values and returns nullable Boolean under SQL three-valued null semantics. |
| `<` | `Column.__lt__` | `o.total < 0` | yes | yes | Performs a typed ordering comparison on compatible orderable scalar values; null operands produce null. |
| `<=` | `Column.__le__` | `o.total <= 0` | yes | yes | Performs a typed inclusive ordering comparison on compatible orderable scalar values; null operands produce null. |
| `>` | `Column.__gt__` | `o.total > 0` | yes | yes | Performs a typed ordering comparison on compatible orderable scalar values; null operands produce null. |
| `>=` | `Column.__ge__` | `o.total >= 0` | yes | yes | Performs a typed inclusive ordering comparison on compatible orderable scalar values; null operands produce null. |
| `&` | `Column.__and__` | `(o.active & (o.total > 0), True & o.active)` | yes | yes | Field access is typed and alias-aware. Python `and`, `or`, and expression truthiness are rejected; bitwise AND, OR, and NOT require Boolean expressions. Reflected AND/OR are also supported for IDE and type-checker compatibility while preserving the authored operand order. |
| `~` | `Column.__invert__` | `~o.active` | yes | yes | Field access is typed and alias-aware. Python `and`, `or`, and expression truthiness are rejected; bitwise AND, OR, and NOT require Boolean expressions. Reflected AND/OR are also supported for IDE and type-checker compatibility while preserving the authored operand order. `assert_true(...)` and `raise_error(...)` are typed Boolean guards. They lower to the matching native PySpark assertion followed by `.isNull()`, so a successful `assert_true(...)` can be used directly in `where(...)` without adding an output field. A false or null condition, or any evaluated `raise_error(...)`, raises Spark's error lazily. Messages are optional string literals for `assert_true(...)` and required string literals for `raise_error(...)`. Compose `~`, `==`, and other ordinary predicates instead of expecting `assert_false` or `assert_equal` aliases. |
| `is_null(...)` | `isNull` | `is_null(order.value)` | yes | yes | Returns a non-null Boolean that is true only for SQL null; floating NaN remains a value. |
| `is_not_null(...)` | `isNotNull` | `is_not_null(order.value)` | yes | yes | Returns a non-null Boolean that is true for every non-null value, including floating NaN. |
| `null_safe_eq(...)` | `eqNullSafe` | `null_safe_eq(order.value)` | yes | yes | Comparisons and Boolean operators preserve SQL three-valued null semantics. `between(...)` is inclusive; `null_safe_eq(...)` and `equal_null(...)` consider two nulls equal and are never null. |
| `between(...)` | `between` | `between(order.value)` | yes | yes | Comparisons and Boolean operators preserve SQL three-valued null semantics. `between(...)` is inclusive; `null_safe_eq(...)` and `equal_null(...)` consider two nulls equal and are never null. |
| `+` | Column addition | `(o.total + 1, 1 + o.total)` | yes | yes | Adds numeric expressions with Spark type promotion; the result is nullable if either operand is nullable. |
| `+` | `functions.concat` | `(o.name + "!", "order:" + o.id)` | yes | yes | For String operands, PySpark `Column.__add__` concatenates text and preserves null propagation. |
| `+` | `functions.concat` | `(o.name + "!", "order:" + o.id)` | yes | yes | For String operands, PySpark `Column.__add__` concatenates text and preserves null propagation. |
| `-` | Column subtraction | `(o.total - 1, 1 - o.total)` | yes | yes | Subtracts numeric expressions with Spark type promotion and null propagation. |
| `*` | Column multiplication | `(o.total * 2, 2 * o.total)` | yes | yes | Multiplies numeric expressions with Spark type promotion and null propagation. |
| `/` | Column division | `(o.total / 2, 2 / o.total)` | yes | yes | Performs Spark numeric division; a zero divisor follows Spark's ANSI configuration. |
| `%` | Column remainder | `(o.total % 2, 2 % o.total)` | yes | yes | Computes Spark remainder on compatible numeric operands and propagates nulls. |
| `-` | Column negation | `-o.total` | yes | yes | Negates a numeric expression while preserving Spark's result-type and nullability rules. |
| `cast(...)` | `cast` | `cast(order.value)` | yes | yes | Casts to a declared Structure type and infers the corresponding Spark result type; use `try_cast(...)` for nullable conversion failure. |
| `astype(...)` | `astype` | `astype(order.value)` | yes | yes | PySpark spelling alias for typed `cast(...)`; the target type is declared explicitly. |
| `try_cast(...)` | `try_cast` | `try_cast(order.value)` | — | — | `try_cast(...)` is always nullable and needs target profile `>=4.0,<4.1`. |
| `asc(...)` | `asc` | `asc(order.value)` | yes | yes | Wraps a typed expression as ascending order for `order_by(...)`, window specifications, and range repartitioning. |
| `desc(...)` | `desc` | `desc(order.value)` | yes | yes | Wraps a typed expression as descending order for `order_by(...)`, window specifications, and range repartitioning. |
| `asc_nulls_first(...)` | `asc_nulls_first` | `asc_nulls_first(order.value)` | yes | yes | Produces ascending order with nulls first; the descriptor can be reused across supported ordering consumers. |
| `asc_nulls_last(...)` | `asc_nulls_last` | `asc_nulls_last(order.value)` | yes | yes | Produces ascending order with nulls last; the descriptor can be reused across supported ordering consumers. |
| `desc_nulls_first(...)` | `desc_nulls_first` | `desc_nulls_first(order.value)` | yes | yes | Produces descending order with nulls first; the descriptor can be reused across supported ordering consumers. |
| `desc_nulls_last(...)` | `desc_nulls_last` | `desc_nulls_last(order.value)` | yes | yes | Produces descending order with nulls last; the descriptor can be reused across supported ordering consumers. |
| `to_decimal(...)` | `Column.cast` | `to_decimal(o.raw_total, precision=12, scale=2)` | yes | yes | Casts a numeric or String expression to Decimal with declared precision and scale. |
| `hll_sketch_estimate(...)` | HLL typed equivalents | `hll_sketch_estimate(o.sketch)` | yes | yes | Decodes branded HLL sketch state to its nullable Long estimate; arbitrary Binary data is rejected. |

## Numeric expressions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `isin(...)` | `isin` | `isin(order.value)` | yes | yes | Comparisons and `isin(...)` require compatible typed values. Numeric values and Date/Timestamp pairs may be compared; Map values are not comparable. `isin(...)` accepts either variadic values or one list; list contents are captured when the expression is authored, and duplicates, nulls, and symbolic scalar expressions retain their authored order. An empty list, nested list, or list mixed with additional positional values is rejected. |
| `expr.get_field(...)` | `Column.getField` | `o.customer.address.get_field("zip")` | yes | yes | Reads a declared nested Struct field by name and preserves its inferred field type and nullability. |
| `expr.with_field(...)` | `Column.withField` | `o.details.with_field("label", "known", schema=Details)` | yes | yes | Replaces or adds one named Struct field and requires the exact declared result Schema. |
| `expr.drop_fields(...)` | `Column.dropFields` | `o.details.drop_fields("legacy", schema=CurrentDetails)` | yes | yes | Removes the named Struct fields and requires the exact declared result Schema. |
| `expr[index]` | `getItem` | `o.tags[0]` | — | — | Indexes a typed Array with zero-based integral access; an out-of-range index returns null. |
| `expr[key]` | `getItem` | `o.attributes["region"]` | — | — | Looks up a typed Map key; missing keys return null and the key expression must match the declared key type. |

## JSON, CSV, and Variant functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `variant_literal(...)` | Compile-time JSON Variant literal | `variant_literal('{"source":"migration"}')` | yes | yes | `variant_literal(...)` requires non-empty, standard JSON text and validates it during symbolic capture. It lowers to `parse_json(F.lit(...))` and does not expose PySpark's Python-specific `VariantVal` object. |
| `parse_json(...)` | Variant JSON parsing | `parse_json(o.payload_json)` | — | — | `variant_literal(...)` requires non-empty, standard JSON text and validates it during symbolic capture. It lowers to `parse_json(F.lit(...))` and does not expose PySpark's Python-specific `VariantVal` object. |
| `try_parse_json(...)` | Variant JSON parsing | `try_parse_json(order.value)` | — | — | Parses typed JSON text into Variant and returns null instead of raising for invalid JSON. |
| `variant_array_append(...)` | Variant array mutation | `variant_array_append(o.payload, "$.items", 1)` | — | — | Appends a typed value at a literal Variant array path and returns the updated Variant. |
| `try_variant_array_append(...)` | Variant array mutation | `try_variant_array_append(order.value)` | — | — | Nullable Variant array append; invalid paths or mutation failures produce null. |
| `variant_insert(...)` | Variant object/array insertion | `variant_insert(o.payload, "$.name", "spark")` | — | — | Inserts a typed value at a literal Variant path and returns the updated Variant. |
| `try_variant_insert(...)` | Variant object/array insertion | `try_variant_insert(order.value)` | — | — | Nullable Variant insertion; invalid paths or mutation failures produce null. |
| `variant_set(...)` | Variant upsert | `variant_set(o.payload, "$.name", "spark")` | — | — | Sets or replaces the value at a literal Variant path and returns Variant. |
| `try_variant_set(...)` | Variant upsert | `try_variant_set(order.value)` | — | — | Nullable Variant set; invalid paths or mutation failures produce null. |
| `variant_delete(...)` | Variant path deletion | `variant_delete(o.payload, "$.name")` | — | — | Deletes the value at a literal Variant path and returns the updated Variant. |
| `variant_explode(...)` | Variant TVF row expansion | `entry = variant_explode(o.payload, to=VariantEntry)` | — | — | Variant row expansion uses typed `variant_explode(...)`/`variant_explode_outer(...)` generators and the PySpark 4 TVF/lateral-join API. Dynamic paths, implicit extraction types, and ordering are not part of the current typed contract. |
| `variant_explode_outer(...)` | Variant TVF row expansion | `variant_explode_outer(order.value)` | — | — | Variant row expansion uses typed `variant_explode(...)`/`variant_explode_outer(...)` generators and the PySpark 4 TVF/lateral-join API. Dynamic paths, implicit extraction types, and ordering are not part of the current typed contract. |
| `JsonOptions(...)` | JSON read/write options | `JsonOptions(date_format="...")` | yes | yes | Typed JSON options make parsing/serialization policy explicit, including null value, date/timestamp formats, and malformed-record mode. |
| `CsvOptions(...)` | CSV read/write options | `CsvOptions(delimiter=";")` | yes | yes | Typed CSV options make delimiter, quote, escape, null value, date/timestamp formats, and malformed-record mode explicit. |
| `schema_of_variant(...)` | Variant schema inspection | `schema_of_variant(o.payload)` | — | — | Variant helpers require a resolved PySpark 4 profile. `is_valid_variant(...)` requires the `>=4.2,<4.3` profile. Paths are non-empty literal strings beginning with `$`; extraction requires an explicit Structure `as_type` and is nullable when the path is absent. The `try_` form is also nullable when casting fails. `schema_of_variant(...)` returns a nullable SQL-format schema string. `to_variant_object(...)` accepts declared Array, Map, or Struct values and rejects a Map with non-String keys anywhere in its nested type graph. |
| `variant_get(...)` | Variant extraction | `variant_get(order.value)` | — | — | Extracts the Variant value at a literal path; `as_type=` declares the typed result. |
| `try_variant_get(...)` | Variant extraction | `try_variant_get(o.payload, "$.name", as_type=types.string())` | — | — | Extracts and casts a Variant path to explicit `as_type=`; missing paths or failed casts produce null. |
| `to_variant_object(...)` | Variant object conversion | `to_variant_object(o.attributes)` | — | — | Variant helpers require a resolved PySpark 4 profile. `is_valid_variant(...)` requires the `>=4.2,<4.3` profile. Paths are non-empty literal strings beginning with `$`; extraction requires an explicit Structure `as_type` and is nullable when the path is absent. The `try_` form is also nullable when casting fails. `schema_of_variant(...)` returns a nullable SQL-format schema string. `to_variant_object(...)` accepts declared Array, Map, or Struct values and rejects a Map with non-String keys anywhere in its nested type graph. |
| `is_variant_null(...)` | Variant JSON-null test | `is_variant_null(o.payload)` | — | — | Tests Spark Variant JSON null specifically; SQL null is a separate condition. |
| `is_valid_variant(...)` | Variant structural validation | `is_valid_variant(o.payload)` | — | — | Variant helpers require a resolved PySpark 4 profile. `is_valid_variant(...)` requires the `>=4.2,<4.3` profile. Paths are non-empty literal strings beginning with `$`; extraction requires an explicit Structure `as_type` and is nullable when the path is absent. The `try_` form is also nullable when casting fails. `schema_of_variant(...)` returns a nullable SQL-format schema string. `to_variant_object(...)` accepts declared Array, Map, or Struct values and rejects a Map with non-String keys anywhere in its nested type graph. |

## Target-gated and boundary items

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| — | UTF-8 validation | — | no | yes | Status: `target-gated`. PySpark 4.0-only validation helpers are outside the default intersection baseline. Migration: Use native PySpark or add a versioned UTF-8 profile. |
| `aes_encrypt(...)` / `aes_decrypt(...)` | AES-GCM | `aes_encrypt(o.payload, key, mode="GCM", iv=nonce)` | yes | yes | Typed GCM calls support symbolic keys and include a nonce-reuse warning. Manual IVs remain available for deterministic use and external interoperability; callers own uniqueness and nonce safety. |
| — | String aggregation | — | no | yes | Status: `target-gated`. PySpark 4.0-only `string_agg`/`listagg` are outside the 3.5/4.0 intersection baseline. Migration: Use native PySpark on the 4.0 target or await a target-profile admission. |
| — | Profile evidence | — | — | — | Status: `target-gated`. Admission requires a released profile plus classic, Connect, generated/online, and streaming evidence. Migration: Use an admitted profile or native PySpark. |
| — | Geospatial providers | — | no | no | Status: `target-gated`. Native root `st_*` is 4.1+; external providers are namespaced and scope-matched. Migration: Use native PySpark or an explicit Binary boundary. |
| `transform(function)` | `Column.transform` | `o.name.transform(lambda value: upper(trim(value)))` | no | yes | Status: `planned`. Requires ordinary PySpark `>=4.1,<4.2`; a typed callback determines result type/nullability. Live online/generated evidence is pending, and Spark Connect remains gated. |

## Typed field and expression helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `&` | `Column.__and__` | `(o.active & (o.total > 0), True & o.active)` | yes | yes | Composes typed Boolean predicates with Spark SQL three-valued logic. |
| `\|` | `Column.__or__` | `(o.active \| o.is_priority, False \| o.active)` | yes | yes | Composes typed Boolean predicates with Spark SQL three-valued logic. |
| `~` | `Column.__invert__` | `~o.active` | yes | yes | Negates a typed Boolean expression and preserves SQL null semantics. |
| `otherwise(...)` | `when`, `otherwise` | `otherwise(order.value)` | — | — | Null and NaN predicates remain distinct. `when(...)` must finish with `.otherwise(...)` before use. |
| `hll_union(...)` | HLL typed equivalents | `hll_union(order.value)` | — | — | Merges branded HLL states; mixed precision requires explicit opt-in and may reduce precision. |

## Bitwise, hash, encoding, and crypto functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `try_url_decode(...)` | `try_url_decode` | `try_url_decode(o.url)` | no | yes | Status: `target-gated`. URL helpers accept String expressions and preserve input nullability. `url_decode(...)` is strict; `try_url_decode(...)` returns null for malformed input (live check: malformed `%G1` returns null). Migration: Use native PySpark 4.0+ when malformed escapes must become null. |
| `sha(...)` | `sha1` | `sha(o.name)` | — | — | PySpark-compatible alias for `sha1(...)`; hashes typed String/Binary input to a hexadecimal digest. |
| `bitmap_bit_position(...)` | Bitmap equivalents | `bitmap_bit_position(o.position)` | — | — | Maps a signed Integer/Long value to Spark Bitmap's non-negative bit position. |
| `bitmap_bucket_number(...)` | Bitmap equivalents | `bitmap_bucket_number(o.position)` | — | — | Maps an Integer/Long value to its Spark Bitmap bucket. |

## Temporal and query-clock functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `interval(...)` | Typed interval construction | `interval(type=Interval.YEAR_TO_MONTH, years=o.years, months=o.months)` | — | — | `interval(...)` requires exactly one of `type=` (compound qualifier) or `unit=` (single field), and exactly one named component per selected field. `types.interval(...)` declares matching Schema types. Spark's `make_ym_interval`, `make_dt_interval`, and mixed `make_interval` keep their public names. Calendar intervals are expression-only on PySpark 3.5; PySpark 4.0 may materialize them in Schema. DayTime values convert to Python `timedelta`, but YearMonth and Calendar values may fail Row conversion depending on the PySpark runtime or Connect Arrow path. Keep interval values inside expressions and collect non-interval results. Adding a Calendar interval to a Date produces a Date. Date/timestamp subtraction returns DayTimeInterval or CalendarInterval according to `spark.sql.legacy.interval.enabled`. |
## Unsupported

These PySpark functions or behaviors have no equivalent admitted Structure contract. Use the stated caller-owned or typed alternative.

| PySpark parity | Details |
| --- | --- |
| `call_function` | Status: `unsupported`. Dynamic SQL function lookup bypasses the typed function contract. Migration: Use a typed Structure function, or keep `call_function(...)` inside a native PySpark boundary. |
| `expr` | Status: `unsupported`. Arbitrary SQL text is outside compiler-visible expression ownership. Migration: Use typed Structure expressions, or isolate `expr(...)` in native PySpark. |
| `col` | Status: `caller-owned-guided`. Dynamic name resolution is not Structure's declared-field model. Migration: Refer to declared fields through the Structure transform input; use native `col(...)` at an explicit boundary when dynamic lookup is required. |
| `column` | Status: `caller-owned-guided`. Alias of dynamic column-name resolution, not a declared-field reference. Migration: Refer to declared fields through the Structure transform input; use native `column(...)` at an explicit boundary when dynamic lookup is required. |
| `current_catalog` | Status: `caller-owned-guided`. Session catalog identity is not a portable row-local value. Migration: Read session metadata in native PySpark outside the typed row expression. |
| `current_database` | Status: `caller-owned-guided`. Session database identity is not a portable row-local value. Migration: Read session metadata in native PySpark outside the typed row expression. |
| `current_schema` | Status: `caller-owned-guided`. Session schema identity is not a portable row-local value. Migration: Read session metadata in native PySpark outside the typed row expression. |
| `current_user` | Status: `caller-owned-guided`. Runtime identity is environment-dependent. Migration: Read runtime identity in native PySpark at the session boundary. |
| `user` | Status: `caller-owned-guided`. Runtime identity is environment-dependent. Migration: Read runtime identity in native PySpark at the session boundary. |
| `version` | Status: `caller-owned-guided`. Runtime version is deployment metadata rather than a portable row-local contract. Migration: Read runtime version in native PySpark outside the typed row expression. |
| `java_method` | Status: `caller-owned-guided`. JVM reflection has no stable cross-runtime type or failure contract. Migration: Use native PySpark reflection only within a runtime-specific boundary. |
| `reflect` | Status: `caller-owned-guided`. JVM reflection has no stable cross-runtime type or failure contract. Migration: Use native PySpark reflection only within a runtime-specific boundary. |
| `typeof` | Status: `caller-owned-guided`. Runtime type inspection is not a stable substitute for declared Structure types. Migration: Use declared Structure types; keep `typeof(...)` in native PySpark for diagnostics or runtime-specific logic. |
