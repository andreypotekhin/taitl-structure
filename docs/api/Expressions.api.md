# Expressions API

These supported helpers and expression methods compile to Spark Column expressions. Examples abbreviate the current
typed `order` row scope as `o`.

## Simple Field And Predicate Expressions

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| Field read | `col` / attribute access | `o.customer_id` |
| Nested field read | Nested Column access | `o.customer.address.zip` |
| `expr.get_field(...)` | `Column.getField` | `o.customer.address.get_field("zip")` |
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

## General Column Transformations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `+` and reverse `+` | Column addition | `(o.total + 1, 1 + o.total)` |
| `-` and reverse `-` | Column subtraction | `(o.total - 1, 1 - o.total)` |
| `*` and reverse `*` | Column multiplication | `(o.total * 2, 2 * o.total)` |
| `expr[index]` | `getItem` | `o.tags[0]` |
| `expr[key]` | `getItem` | `o.attributes["region"]` |
| `contains(...)` | `contains` | `o.name.contains("A")` |
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
- The current arithmetic surface excludes division and modulo; raw `Column.over(...)` remains unsupported.

## SQL Function Helpers

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `lower(...)` | `lower` | `lower(o.name)` |
| `upper(...)` | `upper` | `upper(o.name)` |
| `trim(...)` | `trim` | `trim(o.name)` |
| `substring(...)` | `substring` | `substring(o.code, start=1, length=3)` |
| `split(...)` | `split` | `split(o.code, pattern="-")` |
| `regexp_replace(...)` | `regexp_replace` | `regexp_replace(o.code, pattern="-", replacement="")` |
| `regexp_extract(...)` | `regexp_extract` | `regexp_extract(o.code, pattern="(.*)", group=1)` |
| `length(...)` | `length` | `length(o.name)` |
| `concat_ws(...)` | `concat_ws` | `concat_ws("-", o.region, o.code)` |
| `initcap(...)` | `initcap` | `initcap(o.name)` |
| `reverse(...)` | `reverse` | `reverse(o.name)` |
| `translate(...)` | `translate` | `translate(o.name, matching="-", replacement="_")` |
| `instr(...)` | `instr` | `instr(o.name, substring="A")` |
| `levenshtein(...)` | `levenshtein` | `levenshtein(o.name, "Ada")` |
| `date_add(...)` | `date_add` | `date_add(o.day, days=1)` |
| `datediff(...)` | `datediff` | `datediff(o.end_day, o.start_day)` |
| `date_trunc(...)` | `date_trunc` | `date_trunc(o.at, unit="month")` |
| `abs(...)` | `abs` | `abs(o.total)` |
| `round(...)` | `round` | `round(o.total, scale=2)` |
| `ceil(...)` | `ceil` | `ceil(o.total)` |
| `floor(...)` | `floor` | `floor(o.total)` |
| `isnull(...)` | `isnull` | `isnull(o.score)` |
| `isnotnull(...)` | `isnotnull` | `isnotnull(o.score)` |
| `isnan(...)` | `isnan` | `isnan(o.score)` |
| `to_decimal(...)` | `Column.cast(DecimalType)` | `to_decimal(o.raw_total, precision=12, scale=2)` |
| `coalesce(...)` | `functions.coalesce` | `coalesce(o.discount, 0)` |
| `when(...).otherwise(...)` | `when`, `otherwise` | `when(o.total > 0, "paid").otherwise("free")` |

**Details And Differences**

- Pattern, replacement, separator, and search arguments are explicit compiler-visible values.
- Null and NaN predicates remain distinct. `when(...)` must finish with `.otherwise(...)` before use.
- Decimal precision is an integer from 1 through 38; scale is an integer from 0 through that precision.
- Arithmetic requires numeric operands, widens mixed numeric expressions, and propagates operand nullability.
- Raw `expr(...)`, `call_function(...)`, and UDF/UDTF expressions are unsupported. See the
  [Schemas reference](../reference/Schema.ref.md).
