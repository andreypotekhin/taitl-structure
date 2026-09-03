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
| `null_safe_eq(...)` | `eqNullSafe` | `o.code.null_safe_eq("A")` |
| `equal_null(...)` | `equal_null` | `equal_null(o.code, "A")` |
| `isin(...)` | `isin` | `o.state.isin("CA", "OR")` |
| `between(...)` | `between` | `o.total.between(1, 100)` |

**Details And Differences**

- Field access is typed and alias-aware. Python `and`, `or`, and expression truthiness are rejected; `&`, `|`, and `~`
  require Boolean expressions. Reflected `&` and `|` are also supported for IDE and type-checker compatibility while
  preserving the authored operand order.
- Comparisons and Boolean operators preserve SQL three-valued null semantics. `between(...)` is inclusive;
  `null_safe_eq(...)` and `equal_null(...)` consider two nulls equal and are never null.
- Comparisons and `isin(...)` require compatible typed values. Numeric values and Date/Timestamp pairs may be compared;
  Map values are not comparable.
- Struct mutation requires an explicit declared result Schema. It is rejected unless that schema exactly preserves the
  source shape apart from the named replacement or removals.

## General Column Transformations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `+` and reverse `+` | Column addition | `(o.total + 1, 1 + o.total)` |
| `-` and reverse `-` | Column subtraction | `(o.total - 1, 1 - o.total)` |
| `*` and reverse `*` | Column multiplication | `(o.total * 2, 2 * o.total)` |
| `/` and reverse `/` | Column division | `(o.total / 2, 2 / o.total)` |
| `%` and reverse `%` | Column remainder | `(o.total % 2, 2 % o.total)` |
| Unary `-` | Column negation | `-o.total` |
| `bitwise_and(...)` | `Column.bitwiseAND` | `o.flags.bitwise_and(3)` |
| `bitwise_or(...)` | `Column.bitwiseOR` | `o.flags.bitwise_or(o.mask)` |
| `bitwise_xor(...)` | `Column.bitwiseXOR` | `o.flags.bitwise_xor(o.mask)` |
| `bitwise_not()` | `functions.bitwise_not` | `o.flags.bitwise_not()` |
| `expr[index]` | `getItem` | `o.tags[0]` |
| `expr[key]` | `getItem` | `o.attributes["region"]` |
| `substr(startPos, length)` | `Column.substr` | `o.name.substr(1, 10)` |
| `contains(...)` | `contains` | `o.name.contains("A")` |
| `startswith(...)` | `startswith` | `o.name.startswith("order-")` |
| `endswith(...)` | `endswith` | `o.name.endswith("-hold")` |
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

- Array and map lookup results are nullable. String predicates require String expressions; `rlike(...)` uses Java regex.
  Function-form `like(...)`, `ilike(...)`, `regexp(...)`, `regexp_like(...)`, and `rlike(...)` accept typed String
  expressions for both the value and pattern.
- `substr(...)` requires a String expression and integral start/length literals or expressions. Its result is nullable
  when the receiver or either bound is nullable. Generated method calls use `o.name.substr(...)`; the equivalent
  function form is `substr(o.name, start=1, length=10)`.
- `try_cast(...)` is always nullable and needs target profile `>=4.0,<4.1`.
- Division, remainder, and negation require numeric expressions. Integral division returns Double; Decimal division uses
  Spark's bounded Decimal precision rules. Raw `Column.over(...)` remains unsupported.
- Bitwise methods accept only `integer` and `long` expressions. A mixed pair returns `long`; nullability propagates
  from either operand.

## SQL Function Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `lower(...)` | `lower` | `lower(o.name)` |
| `upper(...)` | `upper` | `upper(o.name)` |
| `ltrim(...)` | `ltrim` | `ltrim(o.name)` |
| `rtrim(...)` | `rtrim` | `rtrim(o.name)` |
| `trim(...)` | `trim` | `trim(o.name)` |
| `btrim(...)` | `btrim` | `btrim(o.name, trim="0")` |
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
| `ascii(...)`, `char_length(...)` | `ascii`, `char_length` | `char_length(o.name)` |
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
| `sha1(...)` | `sha1` | `sha1(o.name)` |
| `sha2(...)` | `sha2` | `sha2(o.name, bits=256)` |
| `date_add(...)` | `date_add` | `date_add(o.day, days=1)` |
| `date_sub(...)` | `date_sub` | `date_sub(o.day, days=1)` |
| `add_months(...)` | `add_months` | `add_months(o.day, months=1)` |
| `datediff(...)` | `datediff` | `datediff(o.end_day, o.start_day)` |
| `date_trunc(...)` | `date_trunc` | `date_trunc(o.at, unit="month")` |
| `trunc(...)` | `trunc` | `trunc(o.day, unit="month")` |
| `year(...)`, `month(...)`, `dayofmonth(...)` | Calendar extraction | `year(o.day)` |
| `hour(...)`, `minute(...)`, `second(...)` | Time extraction | `hour(o.at)` |
| `next_day(...)` | `next_day` | `next_day(o.day, day_of_week="Mon")` |
| `to_date(...)` | `to_date` | `to_date(o.raw_day, format="yyyy-MM-dd")` |
| `to_timestamp(...)` | `to_timestamp` | `to_timestamp(o.raw_at, format="yyyy-MM-dd HH:mm:ss")` |
| `abs(...)` | `abs` | `abs(o.total)` |
| `bit_count(...)` | `bit_count` | `bit_count(o.flags)` |
| `bit_get(...)`, `getbit(...)` | `bit_get`, `getbit` | `bit_get(o.flags, o.position)` |
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
| `zeroifnull(...)` | `functions.zeroifnull` | `zeroifnull(o.total)` |
| `nullif(...)` | `functions.nullif` | `nullif(o.status, "unknown")` |
| `nanvl(...)` | `functions.nanvl` | `nanvl(o.score, 0.0)` |
| `when(...).otherwise(...)` | `when`, `otherwise` | `when(o.total > 0, "paid").otherwise("free")` |
| `base64(...)`, `unbase64(...)` | `base64`, `unbase64` | `base64(o.payload)` |
| `encode(...)`, `decode(...)` | `encode`, `decode` | `decode(encode(o.name, charset="UTF-8"), charset="UTF-8")` |
| `from_json(...)`, `to_json(...)` | `from_json`, `to_json` | `from_json(o.payload_json, as_=Payload)` |
| `from_csv(...)`, `to_csv(...)` | `from_csv`, `to_csv` | `from_csv(o.payload_csv, as_=Payload)` |
| `get_json_object(...)` | `get_json_object` | `get_json_object(o.payload_json, "$.customer.id")` |
| `json_array_length(...)` | `json_array_length` | `json_array_length(o.payload_json)` |
| `json_object_keys(...)` | `json_object_keys` | `json_object_keys(o.payload_json)` |
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

- Pattern, replacement, separator, and search arguments are explicit compiler-visible values.
- Null and NaN predicates remain distinct. `when(...)` must finish with `.otherwise(...)` before use.
- `nullif(value, other)` returns `value`'s type and is always nullable because a matching value becomes null.
- `nanvl(value, fallback)` accepts Float/Double inputs, returns Double, and replaces only NaN—not null—values.
- `nvl(...)` and `ifnull(...)` select a typed fallback; `nvl2(...)` selects between typed present/null branches;
  `zeroifnull(...)` accepts numeric expressions and is never null.
- Decimal precision is an integer from 1 through 38; scale is an integer from 0 through that precision.
- Arithmetic requires numeric operands, widens mixed numeric expressions, and propagates operand nullability.
- `bround(...)` uses Spark's half-even rounding. `sqrt(...)`, `pow(...)`, `log(...)`, `exp(...)`, and `signum(...)`
  return Double values; `log(..., base=...)` accepts a finite positive literal base other than one.
- `trunc(...)` accepts Date values and `year`, `month`, `quarter`, or `week` units (including Spark aliases).
  Calendar extraction accepts Date or Timestamp values; time extraction requires Timestamp. String temporal parsing is
  nullable because invalid input becomes null, and its optional format is a compiler-visible literal.
- `add_months(...)` accepts Date or Timestamp values and an integer literal or integral expression; the result is a
  nullable Date when either input is nullable. `next_day(...)` accepts a Date or Timestamp and a weekday literal from
  Monday through Sunday (short names such as `Mon` are accepted) and returns a nullable Date.
- `dayofweek(...)`, `dayofyear(...)`, `quarter(...)`, and `weekofyear(...)` accept Date or Timestamp values and
  return nullable Integer calendar parts. `dayofweek(...)` numbers Sunday as 1 through Saturday as 7, while
  `weekofyear(...)` follows Spark's ISO week numbering.
- `last_day(...)` accepts a Date or Timestamp and returns the nullable month-end Date. `date_format(...)` accepts a
  Date or Timestamp plus a non-empty format literal and returns a nullable String.
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
- `e()` and `pi()` return non-null Double constants. `factorial(...)` accepts Integer/Long expressions and returns a
  nullable Long. `greatest(...)` and `least(...)` require at least two compatible values, preserve their common type,
  and return null only when all arguments are null. `pmod(...)` accepts two numeric expressions, preserves their common
  numeric type, and propagates operand nullability.
- `bin(...)` accepts Integer/Long and returns nullable String; `hex(...)` accepts Integer/Long or Binary and returns
  nullable String; `unhex(...)` accepts String and returns nullable Binary because malformed input can decode to null.
- `conv(...)` accepts a String expression and base literals from -36 through -2 or 2 through 36, returning nullable
  String. `width_bucket(...)` accepts compatible numeric value/minimum/maximum expressions and a positive integer
  bucket-count literal, returning nullable Integer because invalid runtime ranges produce null.
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
- `from_json(...)` and `from_csv(...)` require an explicit result Schema; `to_json(...)` and `to_csv(...)` require a
  Struct expression. Parsing and rendering results are nullable.
- `get_json_object(...)` requires a non-empty literal JSON path and returns nullable String. `json_array_length(...)`
  returns nullable Integer, and `json_object_keys(...)` returns nullable `array<string>`. `json_tuple(...)` remains
  deferred because it produces multiple output columns rather than one typed expression.
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

## Geometry Expressions

These expressions form the provider-neutral planar Geometry slice. Geometry runtime support is optional and resolved
late through the active `GeoProvider`; the bundled PySpark plugin does not require a provider during ordinary
compilation or generated-source import.

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `geometry_from_wkt(...)` | `ST_GeomFromWKT` | `geometry_from_wkt(o.wkt, srid=4326)` |
| `geometry_as_wkt(...)` | `ST_AsText` | `geometry_as_wkt(o.shape)` |
| `intersects(...)` | `ST_Intersects` | `intersects(o.shape, other.shape)` |
| `contains(...)` | `ST_Contains` | `contains(o.shape, other.shape)` |
| `within(...)` | `ST_Within` | `within(o.shape, other.shape)` |

The WKT constructor requires a String expression and a positive literal SRID. Geometry predicates require matching
SRIDs and return nullable Boolean values; null inputs propagate null. WKT serialization returns nullable String. The
contract excludes `GEOGRAPHY`, runtime-selected SRIDs, CRS transformation, measurements, spatial joins/indexes, and
raw provider-specific `ST_*` calls. Missing providers fail with the Geometry runtime diagnostic when the type or
operation is materialized.
