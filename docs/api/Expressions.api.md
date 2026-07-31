# Expressions API

These supported helpers and expression methods compile to Spark Column expressions. Examples abbreviate the current
typed `order` row scope as `o`.

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
| `&` | `Column.__and__` | `o.active & (o.total > 0)` |
| `\|` | `Column.__or__` | `o.active \| o.is_priority` |
| `~` | `Column.__invert__` | `~o.active` |
| `is_null()` | `isNull` | `o.customer_id.is_null()` |
| `is_not_null()` | `isNotNull` | `o.customer_id.is_not_null()` |
| `null_safe_eq(...)` | `eqNullSafe` | `o.code.null_safe_eq("A")` |
| `isin(...)` | `isin` | `o.state.isin("CA", "OR")` |
| `between(...)` | `between` | `o.total.between(1, 100)` |

**Details And Differences**

- Field access is typed and alias-aware. Python `and`, `or`, and expression truthiness are rejected; `&`, `|`, and `~`
  require Boolean expressions.
- Comparisons and Boolean operators preserve SQL three-valued null semantics. `between(...)` is inclusive;
  `null_safe_eq(...)` considers two nulls equal and is never null.
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
| `substring(...)` | `substring` | `substring(o.code, start=1, length=3)` |
| `split(...)` | `split` | `split(o.code, pattern="-")` |
| `regexp_replace(...)` | `regexp_replace` | `regexp_replace(o.code, pattern="-", replacement="")` |
| `regexp_extract(...)` | `regexp_extract` | `regexp_extract(o.code, pattern="(.*)", group=1)` |
| `length(...)` | `length` | `length(o.name)` |
| `concat_ws(...)` | `concat_ws` | `concat_ws("-", o.region, o.code)`; `concat_ws("\u001f", o.path_ids)` for `array<string>` |
| `initcap(...)` | `initcap` | `initcap(o.name)` |
| `reverse(...)` | `reverse` | `reverse(o.name)` |
| `translate(...)` | `translate` | `translate(o.name, matching="-", replacement="_")` |
| `instr(...)` | `instr` | `instr(o.name, substring="A")` |
| `levenshtein(...)` | `levenshtein` | `levenshtein(o.name, "Ada")` |
| `hash(...)` | `hash` | `hash(o.tenant, o.id)` |
| `xxhash64(...)` | `xxhash64` | `xxhash64(o.tenant, o.id)` |
| `md5(...)` | `md5` | `md5(o.name)` |
| `sha1(...)` | `sha1` | `sha1(o.name)` |
| `sha2(...)` | `sha2` | `sha2(o.name, bits=256)` |
| `date_add(...)` | `date_add` | `date_add(o.day, days=1)` |
| `date_sub(...)` | `date_sub` | `date_sub(o.day, days=1)` |
| `datediff(...)` | `datediff` | `datediff(o.end_day, o.start_day)` |
| `date_trunc(...)` | `date_trunc` | `date_trunc(o.at, unit="month")` |
| `trunc(...)` | `trunc` | `trunc(o.day, unit="month")` |
| `year(...)`, `month(...)`, `dayofmonth(...)` | Calendar extraction | `year(o.day)` |
| `hour(...)`, `minute(...)`, `second(...)` | Time extraction | `hour(o.at)` |
| `to_date(...)` | `to_date` | `to_date(o.raw_day, format="yyyy-MM-dd")` |
| `to_timestamp(...)` | `to_timestamp` | `to_timestamp(o.raw_at, format="yyyy-MM-dd HH:mm:ss")` |
| `abs(...)` | `abs` | `abs(o.total)` |
| `round(...)` | `round` | `round(o.total, scale=2)` |
| `bround(...)` | `bround` | `bround(o.total, scale=2)` |
| `ceil(...)` | `ceil` | `ceil(o.total)` |
| `floor(...)` | `floor` | `floor(o.total)` |
| `sqrt(...)` | `sqrt` | `sqrt(o.total)` |
| `pow(...)` | `pow` | `pow(o.total, 2)` |
| `log(...)` | `log` | `log(o.total, base=10)` |
| `exp(...)` | `exp` | `exp(o.total)` |
| `signum(...)` | `signum` | `signum(o.total)` |
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
| `parse_json(...)`, `try_parse_json(...)` | Variant JSON parsing | `parse_json(o.payload_json)` |
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
- `hash(...)` and `xxhash64(...)` accept scalar inputs. They are Spark hash functions, not cryptographic identifiers;
  do not use them for security, cross-engine interchange, or persistent identifiers. `md5(...)`, `sha1(...)`, and
  `sha2(...)` are deterministic digests of String values, not password-storage primitives.
- Raw `expr(...)`, `call_function(...)`, and UDF/UDTF expressions are unsupported. See the
  [Schemas reference](../reference/Schema.ref.md).
- Variant helpers require a resolved PySpark 4 profile. `is_valid_variant(...)` requires the `>=4.2,<4.3` profile.
  Paths are non-empty literal strings beginning with `$`; extraction requires an explicit Structure `as_type` and is
  nullable when the path is absent. The `try_` form is also nullable when casting fails. `schema_of_variant(...)`
  returns a nullable SQL-format schema string. `to_variant_object(...)` accepts declared Array, Map, or Struct values
  and rejects a Map with non-String keys anywhere in its nested type graph.
