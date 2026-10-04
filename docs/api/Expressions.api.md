# Expressions API

These supported helpers and expression methods compile to Spark Column expressions. Examples abbreviate the current
typed `order` row scope as `o`.

The default transformation baseline is ordinary PySpark `>=3.5,<4.1`. Helpers with a narrower target profile or an
explicit design gate are marked in the details below.

Column methods and SQL function helpers are separate APIs. Structure exposes a method when it is part of the supported
PySpark `Column` surface; functions such as `trim` and `lower` remain function-form helpers.

## Simple Field And Predicate Expressions

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| Field read | `col` / attribute access | `o.customer_id` |
| Nested field read | Nested Column access | `o.customer.address.zip` |
| `expr.get_field(...)` | `Column.getField` | `o.customer.address.get_field("zip")` |
| `expr.with_field(..., schema=...)` | `Column.withField` | `o.details.with_field("label", "known", schema=Details)` |
| `expr.drop_fields(..., schema=...)` | `Column.dropFields` | `o.details.drop_fields("legacy", schema=CurrentDetails)` |
| `==` | `Column.__eq__` | `o.total == 0` |
| `!=` | `Column.__ne__` | `o.total != 0` |
| `<` | `Column.__lt__` | `o.total < 0` |
| `<=` | `Column.__le__` | `o.total <= 0` |
| `>` | `Column.__gt__` | `o.total > 0` |
| `>=` | `Column.__ge__` | `o.total >= 0` |
| `&` and reverse `&` | `Column.__and__` | `(o.active & (o.total > 0), True & o.active)` |
| `\|` and reverse `\|` | `Column.__or__` | `(o.active \| o.is_priority, False \| o.active)` |
| `~` | `Column.__invert__` | `~o.active` |
| `is_null()` | `isNull` | `o.customer_id.is_null()` |
| `is_not_null()` | `isNotNull` | `o.customer_id.is_not_null()` |
| `isnan()` | `isNaN` | `o.score.isnan()` |
| `null_safe_eq(...)` | `eqNullSafe` | `o.code.null_safe_eq("A")` |
| `equal_null(...)` | `equal_null` | `equal_null(o.code, "A")` |
| `isin(...)` | `isin` | `o.state.isin("CA", "OR")` or `o.state.isin(["CA", "OR"])` |
| `between(...)` | `between` | `o.total.between(1, 100)` |

**Details And Differences**

- Field access is typed and alias-aware. Python `and`, `or`, and expression truthiness are rejected; `&`, `|`, and `~`
  require Boolean expressions. Reflected `&` and `|` are also supported for IDE and type-checker compatibility while
  preserving the authored operand order.
- Comparisons and Boolean operators preserve SQL three-valued null semantics. `between(...)` is inclusive;
  `null_safe_eq(...)` and `equal_null(...)` consider two nulls equal and are never null.
- `isnan()` is a non-null Boolean method on Float and Double expressions. Its Python spelling is lowercase to match
  Structure's existing `isnan(...)` function helper; it lowers to PySpark's `isNaN` capability through `F.isnan(...)`.
- Comparisons and `isin(...)` require compatible typed values. Numeric values and Date/Timestamp pairs may be compared;
  Map values are not comparable. `isin(...)` accepts either variadic values or one list; list contents are captured when
  the expression is authored, and duplicates, nulls, and symbolic scalar expressions retain their authored order.
  An empty list, nested list, or list mixed with additional positional values is rejected.
- Struct mutation requires an explicit declared result Schema. It is rejected unless that schema exactly preserves the
  source shape apart from the named replacement or removals.
- URL helpers accept String expressions and preserve input nullability. `url_decode(...)` is strict: malformed encoded
  input follows Spark's error behavior. `try_url_decode(...)` returns null for malformed input and requires PySpark 4.0+.

## General Column Transformations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `+` and reverse `+` | Column addition | `(o.total + 1, 1 + o.total)` |
| String `+` and reverse `+` | `functions.concat` | `(o.name + "!", "order:" + o.id)` |
| `-` and reverse `-` | Column subtraction | `(o.total - 1, 1 - o.total)` |
| `*` and reverse `*` | Column multiplication | `(o.total * 2, 2 * o.total)` |
| `/` and reverse `/` | Column division | `(o.total / 2, 2 / o.total)` |
| `%` and reverse `%` | Column remainder | `(o.total % 2, 2 % o.total)` |
| Unary `-` | Column negation | `-o.total` |
| `bitwise_and(...)` | `Column.bitwiseAND` | `o.flags.bitwise_and(3)` |
| `bitwise_or(...)` | `Column.bitwiseOR` | `o.flags.bitwise_or(o.mask)` |
| `bitwise_xor(...)` | `Column.bitwiseXOR` | `o.flags.bitwise_xor(o.mask)` |
| `bitwise_not()` | `Column.bitwiseNOT` | `o.flags.bitwise_not()` |
| `bitwise_not(value)` | `functions.bitwise_not` | `bitwise_not(o.flags)` |
| `expr[index]` | `getItem` | `o.tags[0]` |
| `expr[key]` | `getItem` | `o.attributes["region"]` |
| `substr(startPos, length)` | `Column.substr` | `o.name.substr(1, 10)` |
| `contains(...)` | `contains` | `o.name.contains("A")` or `o.name.contains(o.prefix)` |
| `startswith(...)` | `startswith` | `o.name.startswith("order-")` or `o.name.startswith(o.prefix)` |
| `endswith(...)` | `endswith` | `o.name.endswith("-hold")` or `o.name.endswith(o.suffix)` |
| `startswith(value, prefix)` | `functions.startswith` | `startswith(o.name, o.prefix)` |
| `endswith(value, suffix)` | `functions.endswith` | `endswith(o.name, o.suffix)` |
| `like(...)` | `like` | `o.name.like("A%")` |
| `ilike(...)` | `ilike` | `o.name.ilike("a%")` |
| `rlike(...)` | `rlike` | `o.name.rlike("^A")` |
| `cast(...)` | `cast` | `o.total.cast(types.decimal(12, 2))` |
| `astype(...)` | `astype` | `o.total.astype(types.decimal(12, 2))` |
| `try_cast(...)` | `try_cast` | `o.raw_total.try_cast(types.decimal(12, 2))` |
| `asc()` | `asc` | `o.at.asc()` |
| `desc()` | `desc` | `o.at.desc()` |
| `asc_nulls_first()` | `asc_nulls_first` | `o.at.asc_nulls_first()` |
| `asc_nulls_last()` | `asc_nulls_last` | `o.at.asc_nulls_last()` |
| `desc_nulls_first()` | `desc_nulls_first` | `o.at.desc_nulls_first()` |
| `desc_nulls_last()` | `desc_nulls_last` | `o.at.desc_nulls_last()` |

**Details And Differences**

- Ordering descriptors require a scalar expression whose type is orderable in Structure: Date, Decimal, Double, Float,
  Integer, Long, String, or Timestamp. All six descriptors preserve the expression type and nullability.
- Array and map lookup results are nullable. String predicates require String expressions; `contains(...)`,
  `startswith(...)`, and `endswith(...)` accept either a string literal or a String expression operand and become
  nullable when either operand is nullable. `rlike(...)` uses Java regex.
  Function-form `startswith(...)` and `endswith(...)` accept String or Binary expressions; Spark's mixed-type coercion
  is preserved when the two operands use different supported types.
  Function-form `like(...)`, `ilike(...)`, `regexp(...)`, `regexp_like(...)`, and `rlike(...)` accept typed String
  expressions for both the value and pattern.
- `substr(...)` requires a String expression and integral start/length literals or expressions. Its result is nullable
  when the receiver or either bound is nullable. Generated method calls use `o.name.substr(...)`; the equivalent
  function form is `substr(o.name, start=1, length=10)`.
- `try_cast(...)` is always nullable and needs target profile `>=4.0,<4.1`.
- `bit_length(...)` accepts String or Binary and returns nullable Integer, counting UTF-8 bytes for String values.
- Division, remainder, and negation require numeric expressions. Integral division returns Double; Decimal division uses
  Spark's bounded Decimal precision rules. Raw `Column.over(...)` remains unsupported.
- Bitwise methods accept only `integer` and `long` expressions. A mixed pair returns `long`; nullability propagates
  from either operand. The SQL-function helper `bitwise_not(value)` accepts an integral expression and preserves its
  type and nullability; it is distinct from the zero-argument Column method `value.bitwise_not()`.

String `+` accepts two String expressions or a String expression and a Python string literal, in either order.
Chains such as `o.first_name + " " + o.last_name` concatenate in authored order. Numeric addition is unchanged.
As in Python, strings do not implicitly convert numbers: write `"n=" + o.count.cast(types.string())` explicitly.
Arrays and binary values continue to use `concat(...)`.

This is Structure syntax: raw PySpark Column `+` is arithmetic; Structure translates string `+` to `F.concat(...)`.
Unlike Python's rejection of `str + None`, nullable String expressions are valid and follow Spark null semantics:
if either value is null, the result is null. Use `coalesce(o.name, "") + "!"` to replace missing strings explicitly.
Bare `None` is untyped and rejected; `literal(None).cast(types.string())` is a valid typed null string.

## SQL Function Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `lower(...)` | `lower` | `lower(o.name)` |
| `upper(...)` | `upper` | `upper(o.name)` |
| `url_encode(...)`, `url_decode(...)` | `url_encode`, `url_decode` | `url_encode(o.url)` |
| `try_url_decode(...)` | `try_url_decode` | `try_url_decode(o.url)`; PySpark 4.0+ |
| `ltrim(...)` | `ltrim` | `ltrim(o.name)` |
| `rtrim(...)` | `rtrim` | `rtrim(o.name)` |
| `trim(...)` | `trim` | `trim(o.name)` |
| `btrim(...)` | `btrim` | `btrim(o.name, trim=o.trim_chars)` |
| `char(...)` | `char` | `char(o.code_point)` |
| `substring(...)` | `substring` | `substring(o.code, start=1, length=3)` |
| `substr(...)` | `substr` | `substr(o.code, start=1, length=3)` |
| `elt(...)` | `elt` | `elt(2, o.primary, o.fallback)` |
| `format_string(...)`, `printf(...)` | `format_string`, `printf` | `format_string("id=%s", o.id)` |
| `split(...)` | `split` | `split(o.code, pattern="-")` |
| `regexp_replace(...)` | `regexp_replace` | `regexp_replace(o.code, pattern="-", replacement="")` |
| `regexp_extract(...)` | `regexp_extract` | `regexp_extract(o.code, pattern="(.*)", group=1)` |
| `regexp_count(...)` | `regexp_count` | `regexp_count(o.code, pattern="Ada")` |
| `regexp_extract_all(...)` | `regexp_extract_all` | `regexp_extract_all(o.code, pattern="(Ada)", group=1)` |
| `regexp_instr(...)` | `regexp_instr` | `regexp_instr(o.code, pattern="Ada", group=0)` |
| `regexp_substr(...)` | `regexp_substr` | `regexp_substr(o.code, pattern="Ada")` |
| `lpad(...)`, `rpad(...)` | `lpad`, `rpad` | `lpad(o.code, length=8, pad="0")` |
| `length(...)` | `length` | `length(o.name)` |
| `concat_ws(...)` | `concat_ws` | `concat_ws("-", o.region, o.code)`; `concat_ws("\u001f", o.path_ids)` for `array<string>` |
| `ascii(...)`, `char_length(...)`, `character_length(...)` | `ascii`, `char_length`, `character_length` | `character_length(o.name)` |
| `lower(...)`, `lcase(...)` | `lower`, `lcase` | `lcase(o.name)` |
| `upper(...)`, `ucase(...)` | `upper`, `ucase` | `ucase(o.name)` |
| `left(...)`, `right(...)` | `left`, `right` | `left(o.name, length=3)` |
| `locate(...)` | `locate` | `locate(o.name, substring="Ada", position=1)` |
| `contains(...)` | `contains` | `contains(o.name, "Ada")` |
| `like(...)`, `ilike(...)` | `like`, `ilike` | `like(o.name, "A%")` |
| `regexp(...)`, `regexp_like(...)`, `rlike(...)` | `regexp`, `regexp_like`, `rlike` | `regexp_like(o.name, "^A")` |
| `find_in_set(...)` | `find_in_set` | `find_in_set(o.name, o.candidates)` |
| `format_number(...)` | `format_number` | `format_number(o.amount, decimals=2)` |
| `octet_length(...)` | `octet_length` | `octet_length(o.name)` |
| `position(...)` | `position` | `position("Ada", o.name, start=1)` |
| `repeat(...)` | `repeat` | `repeat(o.code, count=2)` |
| `replace(...)` | `replace` | `replace(o.name, search="-", replacement="_")` |
| `substring_index(...)` | `substring_index` | `substring_index(o.path, delimiter="/", count=2)` |
| `split_part(...)` | `split_part` | `split_part(o.path, "/", 2)` |
| `initcap(...)` | `initcap` | `initcap(o.name)` |
| `reverse(...)` | `reverse` | `reverse(o.name)` |
| `soundex(...)` | `soundex` | `soundex(o.name)` |
| `translate(...)` | `translate` | `translate(o.name, matching="-", replacement="_")` |
| `instr(...)` | `instr` | `instr(o.name, substring="A")` |
| `levenshtein(...)` | `levenshtein` | `levenshtein(o.name, "Ada")` |
| `hash(...)` | `hash` | `hash(o.tenant, o.id)` |
| `xxhash64(...)` | `xxhash64` | `xxhash64(o.tenant, o.id)` |
| `crc32(...)` | `crc32` | `crc32(o.payload)` |
| `md5(...)` | `md5` | `md5(o.name)` |
| PySpark `sha(...)` | `sha1` | `sha1(o.name)` |
| `sha1(...)` | `sha1` | `sha1(o.name)` |
| `sha2(...)` | `sha2` | `sha2(o.name, bits=256)` |
| `date_add(...)` | `date_add` | `date_add(o.day, days=1)` |
| `dateadd(...)` | `dateadd` | `dateadd(o.day, days=1)` |
| `date_sub(...)` | `date_sub` | `date_sub(o.day, days=o.day_offset)` |
| `make_date(...)` | `make_date` | `make_date(o.year, o.month, o.day)` |
| `add_months(...)` | `add_months` | `add_months(o.day, months=1)` |
| `datediff(...)` | `datediff` | `datediff(o.end_day, o.start_day)` |
| `date_diff(...)` | `date_diff` | `date_diff(o.end_day, o.start_day)` |
| `months_between(...)` | `months_between` | `months_between(o.end_day, o.start_day, round_off=True)` |
| `date_trunc(...)` | `date_trunc` | `date_trunc(o.at, unit="month")` |
| `trunc(...)` | `trunc` | `trunc(o.day, unit="month")` |
| `unix_date(...)` | `unix_date` | `unix_date(o.day)` |
| `date_from_unix_date(...)` | `date_from_unix_date` | `date_from_unix_date(o.epoch_days)` |
| `weekday(...)` | `weekday` | `weekday(o.day)` |
| `year(...)`, `month(...)`, `dayofmonth(...)` | Calendar extraction | `year(o.day)` |
| `day(...)` | `day` | `day(o.day)` |
| `hour(...)`, `minute(...)`, `second(...)` | Time extraction | `hour(o.at)` |
| `next_day(...)` | `next_day` | `next_day(o.day, day_of_week="Mon")` |
| `to_date(...)` | `to_date` | `to_date(o.raw_day, format="yyyy-MM-dd")` |
| `to_timestamp(...)` | `to_timestamp` | `to_timestamp(o.raw_at, format="yyyy-MM-dd HH:mm:ss")` |
| `try_to_timestamp(...)` | `try_to_timestamp` | `try_to_timestamp(o.raw_at, format=o.pattern)` |
| `to_timestamp_ltz(...)` | `to_timestamp_ltz` | `to_timestamp_ltz(o.raw_at, format=o.pattern)` |
| `to_timestamp_ntz(...)` | `to_timestamp_ntz` | `to_timestamp_ntz(o.raw_at, format=o.pattern)` |
| `make_timestamp(...)`, `make_timestamp_ltz(...)`, `make_timestamp_ntz(...)` | Component construction | `make_timestamp_ltz(o.year, o.month, o.day, o.hour, o.minute, o.second, timezone=o.zone)` |
| `timestamp_seconds(...)`, `timestamp_millis(...)`, `timestamp_micros(...)` | Epoch to LTZ | `timestamp_micros(o.epoch_micros)` |
| `unix_seconds(...)`, `unix_millis(...)`, `unix_micros(...)` | LTZ to epoch | `unix_micros(o.at)` |
| `interval(...)`, `make_ym_interval(...)`, `make_dt_interval(...)`, `make_interval(...)` | Typed interval construction | `interval(type=Interval.YEAR_TO_MONTH, years=o.years, months=o.months)` |
| `abs(...)` | `abs` | `abs(o.total)` |
| `bit_count(...)` | `bit_count` | `bit_count(o.flags)` |
| `bit_get(...)`, `getbit(...)` | `bit_get`, `getbit` | `bit_get(o.flags, o.position)` |
| `shiftleft(...)`, `shiftright(...)`, `shiftrightunsigned(...)` | SQL bit shifts | `shiftleft(o.flags, bits=2)` |
| `acos(...)` | `acos` | `acos(o.total)` |
| `hypot(...)` | `hypot` | `hypot(o.x, o.y)` |
| `rand(...)`, `randn(...)` | `rand`, `randn` | `rand(seed=42)`; `randn(seed=42)` |
| `round(...)` | `round` | `round(o.total, scale=2)` |
| `bround(...)` | `bround` | `bround(o.total, scale=2)` |
| `ceil(...)` | `ceil` | `ceil(o.total)` |
| `floor(...)` | `floor` | `floor(o.total)` |
| `sqrt(...)` | `sqrt` | `sqrt(o.total)` |
| `pow(...)` | `pow` | `pow(o.total, 2)` |
| `log(...)` | `log` | `log(o.total, base=10)` |
| `exp(...)` | `exp` | `exp(o.total)` |
| `e()`, `pi()` | `e`, `pi` | `e()`; `pi()` |
| `factorial(...)` | `factorial` | `factorial(o.count)` |
| `greatest(...)`, `least(...)` | `greatest`, `least` | `greatest(o.left, o.right)` |
| `pmod(...)` | `pmod` | `pmod(o.value, 7)` |
| `bin(...)`, `hex(...)`, `unhex(...)` | `bin`, `hex`, `unhex` | `hex(o.id)`; `unhex(o.value)` |
| `conv(...)` | `conv` | `conv(o.digits, from_base=2, to_base=16)` |
| `width_bucket(...)` | `width_bucket` | `width_bucket(o.value, 0, 100, num_buckets=10)` |
| `signum(...)` | `signum` | `signum(o.total)` |
| `asin(...)`, `atan(...)`, `atan2(...)` | `asin`, `atan`, `atan2` | `atan2(o.y, o.x)` |
| `cos(...)`, `sin(...)`, `tan(...)` | `cos`, `sin`, `tan` | `sin(o.angle)` |
| `degrees(...)`, `radians(...)` | `degrees`, `radians` | `degrees(o.angle)` |
| `ln(...)`, `log10(...)` | `ln`, `log10` | `log10(o.amount)` |
| Hyperbolic helpers | `acosh`, `asinh`, `atanh`, `cosh`, `sinh`, `tanh` | `tanh(o.amount)` |
| Additional transcendental helpers | `cbrt`, `cot`, `csc`, `expm1`, `log1p`, `log2`, `sec` | `cbrt(o.amount)` |
| Rounding/sign helper | `rint`, `sign` | `rint(o.amount)` |
| `isnull(...)` | `isnull` | `isnull(o.score)` |
| `isnotnull(...)` | `isnotnull` | `isnotnull(o.score)` |
| `isnan(...)` | `isnan` | `isnan(o.score)` |
| `to_decimal(...)` | `Column.cast(DecimalType)` | `to_decimal(o.raw_total, precision=12, scale=2)` |
| `coalesce(...)` | `functions.coalesce` | `coalesce(o.discount, 0)` |
| `nvl(...)` | `functions.nvl` | `nvl(o.discount, 0)` |
| `ifnull(...)` | `functions.ifnull` | `ifnull(o.discount, 0)` |
| `nvl2(...)` | `functions.nvl2` | `nvl2(o.code, "known", "missing")` |
| `zeroifnull(...)` | typed `functions.coalesce` lowering | `zeroifnull(o.total)` |
| `nullif(...)` | `functions.nullif` | `nullif(o.status, "unknown")` |
| `nanvl(...)` | `functions.nanvl` | `nanvl(o.score, 0.0)` |
| `when(...).otherwise(...)` | `when`, `otherwise` | `when(o.total > 0, "paid").otherwise("free")` |
| `assert_true(...)` | `functions.assert_true` | `where(assert_true(o.total > 0, message="total must be positive"))` |
| `raise_error(...)` | `functions.raise_error` | `where(raise_error("unexpected row"))` |
| `base64(...)`, `unbase64(...)` | `base64`, `unbase64` | `base64(o.payload)` |
| `encode(...)`, `decode(...)` | `encode`, `decode` | `decode(encode(o.name, charset="UTF-8"), charset="UTF-8")` |
| `to_binary(...)`, `try_to_binary(...)` | `to_binary`, `try_to_binary` | `to_binary(o.token, format="utf-8")` |
| Query-clock helpers | `current_date`, `curdate`, `current_timestamp`, `now`, `localtimestamp`, `current_timezone` | `current_timestamp()` |
| AES-GCM helpers | `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt` | `aes_encrypt(o.payload, key=o.key)` |
| `hll_sketch_estimate(...)`, `hll_union(...)` | HLL typed equivalents | `hll_sketch_estimate(o.sketch)` |
| `bitmap_count(...)`, `bitmap_bit_position(...)`, `bitmap_bucket_number(...)` | Bitmap equivalents | `bitmap_count(o.bitmap)` |
| `from_unixtime(...)` | `from_unixtime` | `from_unixtime(o.epoch_seconds, format="yyyy-MM-dd")` |
| `unix_timestamp(...)` | `unix_timestamp` | `unix_timestamp(o.raw_at, format="yyyy-MM-dd HH:mm:ss")` |
| `to_unix_timestamp(...)` | `to_unix_timestamp` | `to_unix_timestamp(o.raw_at, format=o.pattern)` |
| `to_utc_timestamp(...)` | `to_utc_timestamp` | `to_utc_timestamp(o.raw_at, timezone="UTC")` |
| `from_utc_timestamp(...)` | `from_utc_timestamp` | `from_utc_timestamp(o.raw_at, timezone="America/Los_Angeles")` |
| `convert_timezone(...)` | `convert_timezone` | `convert_timezone(o.source_zone, o.target_zone, o.local_time)` |
| `date_part(...)` | `date_part` | `date_part("month", o.raw_at)` |
| `datepart(...)` | `datepart` | `datepart("year", o.raw_at)` |
| `extract(...)` | `extract` | `extract(Temporal.DAY_OF_YEAR, o.day)` |
| `from_json(...)`, `to_json(...)` | `from_json`, `to_json` | `from_json(o.payload_json, as_=Payload)` |
| `from_csv(...)`, `to_csv(...)` | `from_csv`, `to_csv` | `from_csv(o.payload_csv, as_=Payload)` |
| `get_json_object(...)` | `get_json_object` | `get_json_object(o.payload_json, "$.customer.id")` |
| `json_array_length(...)` | `json_array_length` | `json_array_length(o.payload_json)` |
| `json_object_keys(...)` | `json_object_keys` | `json_object_keys(o.payload_json)` |
| `json_tuple(...)` | `json_tuple` | `json_tuple(o.payload_json, as_=LegacyFields, fields={"customer_id": "customerId"})` |
| `schema_of_json(...)`, `schema_of_csv(...)` | `schema_of_json`, `schema_of_csv` | `schema_of_json('{"id": 1}')` |
| `parse_json(...)`, `try_parse_json(...)` | Variant JSON parsing | `parse_json(o.payload_json)` |
| `variant_literal(...)` | Compile-time JSON Variant literal | `variant_literal('{"source":"migration"}')` |
| **Design-gated:** `variant_array_append(...)`, `try_variant_array_append(...)` | Variant array mutation | `variant_array_append(o.payload, "$.items", 1)` |
| **Design-gated:** `variant_insert(...)`, `try_variant_insert(...)` | Variant object/array insertion | `variant_insert(o.payload, "$.name", "spark")` |
| **Design-gated:** `variant_set(...)`, `try_variant_set(...)` | Variant upsert | `variant_set(o.payload, "$.name", "spark")` |
| **Design-gated:** `variant_delete(...)` | Variant path deletion | `variant_delete(o.payload, "$.name")` |
| `variant_explode(...)`, `variant_explode_outer(...)` | Variant TVF row expansion | `entry = variant_explode(o.payload, as_=VariantEntry)` |
| `schema_of_variant(...)` | Variant schema inspection | `schema_of_variant(o.payload)` |
| `variant_get(...)`, `try_variant_get(...)` | Variant extraction | `try_variant_get(o.payload, "$.name", as_type=types.string())` |
| `to_variant_object(...)` | Variant object conversion | `to_variant_object(o.attributes)` |
| `is_variant_null(...)` | Variant JSON-null test | `is_variant_null(o.payload)` |
| `is_valid_variant(...)` | Variant structural validation | `is_valid_variant(o.payload)` |

**Details And Differences**

- Scalar `coalesce(value, fallback, *values)` requires at least two values. For a single nullable expression, use the
  expression directly; relation partition coalescing is a distinct keyword-only form, `coalesce(partitions=4)`.

- Pattern, replacement, separator, and search arguments are explicit compiler-visible values.
- `btrim(..., trim=...)` accepts a typed String expression or literal, matching PySpark's row-valued trim-string
  argument. The ordinary `trim`, `ltrim`, and `rtrim` helpers retain their one-argument PySpark contracts.
- `assert_true(...)` and `raise_error(...)` are typed Boolean guards. They lower to the matching native PySpark
  assertion followed by `.isNull()`, so a successful `assert_true(...)` can be used directly in `where(...)` without
  adding an output field. A false or null condition, or any evaluated `raise_error(...)`, raises Spark's error lazily.
  Messages are optional string literals for `assert_true(...)` and required string literals for `raise_error(...)`.
  Compose `~`, `==`, and other ordinary predicates instead of expecting `assert_false` or `assert_equal` aliases.
- Null and NaN predicates remain distinct. `when(...)` must finish with `.otherwise(...)` before use.
- `nullif(value, other)` returns `value`'s type and is always nullable because a matching value becomes null.
- `nanvl(value, fallback)` accepts Float/Double inputs, returns Double, and replaces only NaN—not null—values.
- `nvl(...)` and `ifnull(...)` select a typed fallback; `nvl2(...)` selects between typed present/null branches;
  `zeroifnull(...)` accepts numeric expressions and is never null. It lowers through a cast zero and `coalesce`, so it
  remains available on PySpark 3.5 even though the named `pyspark.sql.functions.zeroifnull` wrapper was added in 4.0.
- Decimal precision is an integer from 1 through 38; scale is an integer from 0 through that precision.
- Arithmetic requires numeric operands, widens mixed numeric expressions, and propagates operand nullability.
- `bround(...)` uses Spark's half-even rounding. `sqrt(...)`, `pow(...)`, `log(...)`, `exp(...)`, and `signum(...)`
  return Double values; `log(..., base=...)` accepts a finite positive literal base other than one.
- `trunc(...)` accepts Date values and `year`, `month`, `quarter`, or `week` units (including Spark aliases).
  Calendar extraction accepts Date or Timestamp values; time extraction requires Timestamp. String temporal parsing is
  nullable because invalid input becomes null, and its optional format is a compiler-visible literal.
- `date_add(...)`, `date_sub(...)`, and `add_months(...)` accept Date or Timestamp values and an integer literal or
  integral expression for their offset; each returns Date and combines value/offset nullability. `next_day(...)`
  accepts a Date or Timestamp and a weekday literal from
  Monday through Sunday (short names such as `Mon` are accepted) and returns a nullable Date.
- `months_between(...)` accepts Date or Timestamp values and returns a nullable Double. Its `round_off` argument must
  be a Boolean literal and renders to Spark's `roundOff` parameter.
- `dayofweek(...)`, `dayofyear(...)`, `quarter(...)`, and `weekofyear(...)` accept Date or Timestamp values and
  return nullable Integer calendar parts. `dayofweek(...)` numbers Sunday as 1 through Saturday as 7, while
  `weekofyear(...)` follows Spark's ISO week numbering.
- `last_day(...)` accepts a Date or Timestamp and returns the nullable month-end Date. `date_format(...)` accepts a
  Date or Timestamp plus a non-empty format literal and returns a nullable String.
- `unix_date(...)` accepts a Date expression and returns nullable Integer epoch days; `date_from_unix_date(...)`
  accepts an Integer or Long day count and returns a nullable Date.
- `weekday(...)` accepts a Date or Timestamp expression and returns a nullable zero-based Integer, with Monday as
  zero and Sunday as six.
- `shiftleft(...)`, `shiftright(...)`, and `shiftrightunsigned(...)` accept an integral expression plus an integer
  literal `bits` count and return a nullable Long. The count remains literal so generated PySpark uses the supported
  `numBits` argument form.
- `lpad(...)` and `rpad(...)` accept a String expression, a non-negative integer literal, and a non-empty padding
  literal. They return a String expression with the input nullability.
- `mask(...)` accepts a String expression and optional single-character literals for uppercase, lowercase, digit, and
  other characters. `overlay(...)` accepts same-family String or Binary values plus integral position and length
  expressions, returning the source type with combined nullability.
- `elt(...)` uses a one-based integral index and requires compatible scalar candidates; its result is nullable because
  the index may be null, out of range, or select a nullable candidate.
- `format_string(...)` and `printf(...)` require a literal format string and scalar arguments. Their String result is
  nullable when any candidate argument is nullable.
- `acos(...)` and `hypot(...)` accept numeric expressions and return nullable Double results.
- `ceiling(...)`, `power(...)`, `negate(...)`, `negative(...)`, and `positive(...)` preserve PySpark's exact function
  names. The unary helpers preserve their input type and nullability; `power(...)` returns Double and propagates either
  operand's nullability.
- `e()` and `pi()` return non-null Double constants. `factorial(...)` accepts Integer/Long expressions and returns a
  nullable Long. `greatest(...)` and `least(...)` require at least two compatible values, preserve their common type,
  and return null only when all arguments are null. `pmod(...)` accepts two numeric expressions, preserves their common
  numeric type, and propagates operand nullability.
- `bin(...)` accepts Integer/Long and returns nullable String; `hex(...)` accepts Integer/Long or Binary and returns
  nullable String; `unhex(...)` accepts String and returns nullable Binary because malformed input can decode to null.
- `conv(...)` accepts a String expression and base literals from -36 through -2 or 2 through 36, returning nullable
  String. `width_bucket(...)` accepts compatible numeric value/minimum/maximum expressions and a positive integer
  bucket-count literal, returning nullable Long because Spark exposes bucket numbers as `LongType`; invalid runtime
  ranges produce null.
- `rand(...)` returns a non-null Double in `[0.0, 1.0)`. It requires an integer `seed` by default; omitting the seed
  requires `reproducible=False`. The seed makes the use auditable but does not promise identical random values across
  repartitioning, retries, Spark versions, or query restarts. Streaming support follows the target-specific coverage
  ledger and is not implied by batch support.
- `randn(...)` uses the same explicit seed/reproducibility policy and returns a non-null standard-normal Double. It is
  nondeterministic and streaming evidence remains target-specific.
- `hash(...)` and `xxhash64(...)` accept scalar inputs. `crc32(...)` accepts String or Binary input and returns a
  nullable Long checksum. These are Spark hash/checksum functions, not cryptographic identifiers;
  do not use them for security, cross-engine interchange, or persistent identifiers. `md5(...)`, `sha1(...)`, and
  `sha2(...)` are deterministic digests of String values, not password-storage primitives.
- `base64(...)` and `decode(...)` return String values; `unbase64(...)` and `encode(...)` return Binary values.
  `encode(...)` and `decode(...)` accept compiler-visible charset names.
- `to_binary(...)` and `try_to_binary(...)` accept String expressions and optional literal formats `hex`, `utf-8`,
  `utf8`, or `base64`; omitted format defaults to `hex`. Both return Binary, while `try_to_binary(...)` is always
  nullable because conversion failures become null.
- `from_unixtime(...)` accepts a numeric epoch-seconds expression and a non-empty literal format, returning a
  nullable String formatted in Spark's session time zone.
- `unix_timestamp(...)` accepts String, Date, or LTZ Timestamp expressions and a literal or typed String format, returning
  nullable Long epoch seconds. Omitting the value uses Spark's query-time current timestamp and returns a non-nullable
  Long; its no-input format remains literal-only. `to_unix_timestamp(...)` requires an input. A format on Date/LTZ
  input is ignored by Spark and produces `PYSPARK-W2705`.
- `to_utc_timestamp(...)` and `from_utc_timestamp(...)` accept String or Timestamp expressions and a non-empty
  timezone literal, returning nullable Timestamp values.
- `convert_timezone(source_tz, target_tz, source_ts)` accepts String expressions for both time zones and a
  `timestamp_ntz()` source, returning nullable TimestampNTZ. Pass `None` for `source_tz` to use Spark's session time
  zone, matching PySpark. LTZ Timestamp and NTZ values remain distinct; use an explicit conversion when changing
  between instants and wall-clock values.
- Generic `to_timestamp(...)`, `try_to_timestamp(...)`, and `make_timestamp(...)` follow the resolved
  `spark.sql.timestampType` (`TIMESTAMP_LTZ` by default); explicit `_ltz` and `_ntz` helpers have fixed result types.
  Parsers accept typed String formats, including row-dependent patterns. `try_to_timestamp(...)` returns null on
  invalid text; the other parsers follow Spark's ANSI policy. A format on typed Date/LTZ input is ignored and emits
  `PYSPARK-W2705`. An NTZ `make_timestamp(...)` with `timezone=` emits `PYSPARK-W2706`.
- `timestamp_seconds/millis/micros` always return LTZ; the inverse `unix_seconds/millis/micros` accept LTZ and return
  Long. Millisecond/microsecond inputs must be integral; seconds may have a fractional part. Negative epochs use
  Spark's rounding and overflow rules.
- `date_part(...)`, `datepart(...)`, and `extract(...)` accept a compiler-visible String field (ordinary string,
  `Temporal` constant, or typed String literal) and a Date, Timestamp, or interval source. `"dayofyear"` is accepted
  as an alias of `Temporal.DAY_OF_YEAR` (`"doy"`). Integral fields return Integer; seconds return `Decimal(8,6)`.
- `interval(...)` requires exactly one of `type=` (compound qualifier) or `unit=` (single field), and exactly one
  named component per selected field. `types.interval(...)` declares matching Schema types. Spark's
  `make_ym_interval`, `make_dt_interval`, and mixed `make_interval` keep their public names. Calendar intervals are
  expression-only on PySpark 3.5; PySpark 4.0 may materialize them in Schema. DayTime values convert to Python
  `timedelta`, but YearMonth and Calendar values may fail Row conversion depending on the PySpark runtime or Connect
  Arrow path. Keep interval values inside expressions and collect non-interval results. Adding a Calendar interval
  to a Date produces a Date.
  Date/timestamp subtraction returns DayTimeInterval or CalendarInterval according to
  `spark.sql.legacy.interval.enabled`.
- `make_date(...)` accepts Integer/Long year, month, and day expressions and returns a nullable Date. Invalid
  components follow Spark's `spark.sql.ansi.enabled` behavior: NULL when ANSI mode is disabled, or a runtime error when
  it is enabled.
- Query-clock helpers preserve Spark start-of-query/session-timezone semantics. Clock values are non-null and
  query-stable but not stable across retries or streaming micro-batches.
- AES helpers are GCM-only typed equivalents. Keys are symbolic String/Binary expressions; explicit encryption IVs
  are accepted for interoperability and emit `CRYPTO-W0801`.
- HLL and Bitmap helpers use branded opaque Binary state. HLL precision mismatches reject by default; sketch
  aggregates follow the existing grouped-streaming contract. See [Sketches and Bitmaps](Aggregations.api.md#sketches-and-bitmaps)
  for their schema, aggregate, persistence, and profile rules.
- `from_json(...)` and `from_csv(...)` require an explicit result Schema; `to_json(...)` and `to_csv(...)` require a
  Struct expression. Parsing and rendering results are nullable.
- `get_json_object(...)` requires a non-empty literal JSON path and returns nullable String. `json_array_length(...)`
  returns nullable Integer, and `json_object_keys(...)` returns nullable `array<string>`. `json_tuple(...)` requires
  a declared schema of nullable String fields and returns a row-preserving generated scope; optional literal mappings
  translate Structure output names to top-level JSON member names.
- `schema_of_json(...)` and `schema_of_csv(...)` accept non-empty text literals plus immutable parser options and
  return non-nullable SQL-format schema Strings. Dynamic input is rejected because schema inference must be resolved
  before the typed output schema is compiled.
- Raw `expr(...)`, `call_function(...)`, direct UDF/UDTF expressions, and implicit Python-to-UDF conversion are
  unsupported. Scalar `@special(type="udf")` remains an ordinary-PySpark row-local feature with its warning policy;
  see the [Transforms API](Transforms.api.md).
- Variant helpers require a resolved PySpark 4 profile. `is_valid_variant(...)` requires the `>=4.2,<4.3` profile.
  Paths are non-empty literal strings beginning with `$`; extraction requires an explicit Structure `as_type` and is
  nullable when the path is absent. The `try_` form is also nullable when casting fails. `schema_of_variant(...)`
  returns a nullable SQL-format schema string. `to_variant_object(...)` accepts declared Array, Map, or Struct values
  and rejects a Map with non-String keys anywhere in its nested type graph.
- `schema_of_variant_agg(...)` is the grouped form of Variant schema inspection. It returns a nullable SQL-format schema
  string and requires a Variant expression in an aggregate step.
- Variant row expansion uses typed `variant_explode(...)`/`variant_explode_outer(...)` generators and the PySpark 4
  TVF/lateral-join API. Dynamic paths, implicit extraction types, and ordering are not part of the current typed
  contract.
- `variant_literal(...)` requires non-empty, standard JSON text and validates it during symbolic capture. It lowers to
  `parse_json(F.lit(...))` and does not expose PySpark's Python-specific `VariantVal` object.
- Mutation helpers use literal `$`-prefixed paths, but remain design-gated until a released PySpark 4.3+ profile is
  added to Structure's compatibility matrix. They are not admitted by the current PySpark 4.2 target and therefore do
  not lower in the current baseline.

## JSON And CSV Options

`from_json(...)`, `to_json(...)`, `from_csv(...)`, and `to_csv(...)` accept immutable, compiler-visible option records.
Options are literals rather than arbitrary dictionaries, so generated and online execution use the same Spark option
names and validation rules.

| Option record | Fields | Example |
| --- | --- | --- |
| `JsonOptions(...)` | `null_value`, `date_format`, `timestamp_format`, `mode` | `JsonOptions(date_format="...")` |
| `CsvOptions(...)` | `delimiter`, `quote`, `escape`, `null_value`, `date_format`, `timestamp_format`, `mode` | `CsvOptions(delimiter="|")` |

`mode` is currently limited to `"PERMISSIVE"`; writer calls omit it. Other options must be non-empty strings when
provided, except `null_value`, which may be an empty string. Parser Schemas must make every parsed field nullable,
including nested fields, because permissive Spark parsing can produce null values.

## Geospatial Expressions

Geospatial expressions are target-gated; the default PySpark `>=3.5,<4.1` profile has no stable spatial expression API.
The planned native PySpark 4.1+ root names are `st_geomfromwkb`, `st_geogfromwkb`, `st_asbinary`, `st_srid`, and
`st_setsrid`. External providers keep their own names in modules such as `sedona.st_geomfromwkt` and
`sedona.st_intersects`.

Spatial values do not become interchangeable because the function names are similar. A provider-scoped operation needs
the same `geo_provider` scope as its arguments; predicates require the same provider, Geometry/Geography kind, and
fixed SRID. See the [Geospatial reference](../reference/Geospatial.ref.md) for target status, scope, and Binary
handoffs.
