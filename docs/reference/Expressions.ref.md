# Expression Reference

Expressions describe typed Spark work inside a transform step. Read schema fields, combine them with helpers from
`structure.plugin.pyspark`, and assign the results to an output Schema. The [Expressions API](../api/Expressions.api.md)
lists the full helper inventory; the [Transform reference](Transform.ref.md) explains step declarations and reusable
expression helpers.

## PySpark 4.1 scalar helpers

Select `plugin.pyspark.profile = ">=4.1,<4.2"` to use these helpers on ordinary PySpark or Spark Connect.
The default `>=3.5,<4.1` profile rejects them during compilation.

| Helper | Input | Result | Null behavior |
| --- | --- | --- | --- |
| `chr(value)` | Integer/Long expression or integer literal | String, matching `char` with the native `chr` spelling | Preserves input nullability |
| `quote(value)` | String expression or string literal | Text enclosed in single quotes; embedded single quotes gain a backslash | Always nullable in Spark; null input stays null |
| `try_to_date(value, format=None)` | String, Date, or LTZ Timestamp; optional non-empty datetime pattern literal | Date | Nullable; malformed text becomes null in either ANSI mode |
| `random(seed=None, reproducible=True)` | Integer seed literal or explicit `reproducible=False` | Non-null Double in `[0, 1)` | Never null |
| `uuid(seed=None, reproducible=True)` | Integer seed literal or explicit `reproducible=False` | Non-null canonical 36-character UUID String | Never null |

String arguments are values. For example, `quote("name")` quotes the literal text `name`; use `quote(row.name)` to read a
field. `chr` follows Spark's byte-range character rules: positive values are reduced modulo 256, negative values
produce an empty string, and zero produces a NUL character. `quote` preserves existing backslashes.

`try_to_date` uses Spark datetime patterns such as `"dd/MM/yyyy"`. Its format is a Python string literal;
row-dependent patterns are rejected. A format on Date or LTZ Timestamp input is ignored and emits `PYSPARK-W2705`.
Invalid pattern definitions can still raise Spark errors. Timestamp-to-Date conversion follows the session time zone.
Declare the result nullable even when the source text is required.

```python
from structure import *
from structure.plugin.pyspark import *


class Raw(Schema):
    code = long(nullable=True)
    note = string(nullable=True)
    date_text = string(nullable=False)


class Clean(Schema):
    character = string(nullable=True)
    quoted_note = string(nullable=True)
    parsed_date = date(nullable=True)


@transform
class CleanValues(Transform):
    rows = input(Raw)
    clean = output(Clean)

    def clean_values(self, row: Raw) -> Clean:
        return Clean(
            character=chr(row.code),
            quoted_note=quote(row.note),
            parsed_date=try_to_date(row.date_text, format="dd/MM/yyyy"),
        )
```

These three conversions are stateless and preserve rows. They are compatible with streaming by design; the current
runtime evidence covers batch execution.

### Random values and UUIDs

The random helpers use the existing `rand` seed policy. Pass a literal seed, or explicitly accept nondeterminism by
setting `reproducible=False`:

```python
sample = random(seed=42)
identifier = uuid(seed=42)
unseeded_identifier = uuid(reproducible=False)
```

A seed documents the random choice. It does not promise identical values across partitions, retries, Spark versions,
or query restarts. Seeded UUIDs are not persistent identifiers for reruns. Both helpers are batch-only until their
streaming seed and replay policy is defined; compilation reports `STREAM-E0801` on streaming input, including use in
filters or reusable expression helpers.

The [compatibility table](../compatibility/Expressions.compat.md#pyspark-41-scalar-helpers) records the admitted profiles.

## Spark 4.1 TIME values

TIME requires the exact `>=4.1,<4.2` profile on ordinary PySpark and Spark Connect. Its Spark type is disabled by
default. Enable it in the Spark session before running the transform, for example with
`SparkSession.builder.config("spark.sql.timeType.enabled", "true")`. Structure leaves this setting to the Spark
installation; when TIME is disabled, Spark's native error is returned.

`time(precision=6)` declares a TIME field with precision from 0 through 6. TIME values have no timezone. A Python
`datetime.time` literal is accepted; if it has timezone information, Spark keeps its clock fields and ignores the
timezone. Declared precision is metadata for the schema and Spark handles casts that reduce precision.

| Helper | Result | Null behavior |
| --- | --- | --- |
| `current_time(precision=6)` | TIME at the requested precision | Non-null and stable within one query |
| `make_time(hour, minute, second)` | TIME(6) | Nullable; Spark validates runtime component ranges |
| `to_time(value, format=None)` | TIME(6) | Nullable; malformed values raise Spark's parse error in either ANSI mode |
| `try_to_time(value, format=None)` | TIME(6) | Nullable; malformed values become null |
| `time_diff(unit, start, end)` | Long | Nullable; unit may be a String literal or typed String expression |
| `time_trunc(unit, value)` | TIME at the input precision | Nullable; unit may be a String literal or typed String expression |

`to_time` accepts a literal or typed String format expression. Formats use Spark datetime patterns. `time_diff` supports
`HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, and `MICROSECOND` (case-insensitive). TIME comparisons require matching
precisions; ordering accepts all TIME precisions. Casts are supported only between TIME precisions and between TIME
and String. Date and timestamp conversions and
TIME arithmetic are not part of this contract. Set output nullability to match the table above even when an input is
required.

```python
from structure import *
from structure.plugin.pyspark import *


class Raw(Schema):
    start = time(nullable=False)
    text = string(nullable=True)
    pattern = string(nullable=False)
    unit = string(nullable=False)


class Parsed(Schema):
    current = time(precision=3, nullable=False)
    parsed = time(nullable=True)
    safe = time(nullable=True)
    elapsed_us = long(nullable=True)
    hour = time(nullable=True)


@transform
class ParseTimes(Transform):
    rows = input(Raw)
    parsed = output(Parsed)

    def parse(self, row: Raw) -> Parsed:
        return Parsed(
            current=current_time(3),
            parsed=to_time(row.text, format=row.pattern),
            safe=try_to_time(row.text),
            elapsed_us=time_diff("microsecond", row.start, row.start),
            hour=time_trunc(row.unit, row.start),
        )
```

See the [TIME compatibility entry](../compatibility/Expressions.compat.md#spark-41-time) for runtime evidence and
profile details.
