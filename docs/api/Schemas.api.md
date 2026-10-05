# Schemas API

Schema declarations define Structure's typed row contract and materialize to Spark SQL schemas.

## Simple Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Schema` | `StructType` | `class Order(Schema): pass` |
| `string(...)` | `StructField(StringType)` | `id = string(nullable=False)` |
| `boolean()` | Spark Boolean type | `boolean()` |
| `integer()` | Spark integer type | `integer()` |
| `long()` | Spark long type | `long()` |
| `float()` | Spark float type | `float()` |
| `double()` | Spark double type | `double()` |
| `date()` | Spark date type | `date()` |
| `timestamp()` | Spark timestamp type | `timestamp()` |
| `timestamp_ntz()` | Spark timestamp without time zone | `timestamp_ntz()` |
| `field.interval(type=...)`, `field.interval(unit=...)` | Qualified Spark interval | `field.interval(type=Interval.YEAR_TO_MONTH)` |
| `binary()` | Spark binary type | `payload = binary(nullable=True)` |
| `decimal(...)` | `DecimalType` | `total = decimal(12, 2)` |
| `variant(...)` | `VariantType` | `payload = variant(nullable=True)` |
| `geometry(srid=...)` | Target-gated provider-bound Geometry | `location = geometry(srid=4326)` |
| `geography(srid=...)` | Target-gated provider-bound Geography | `location = geography(srid=4326)` |
| `hll_sketch(lg_config_k=...)` | Branded opaque Binary HLL state | `customers = hll_sketch(lg_config_k=12)` |
| `bitmap()` | Branded opaque Binary Bitmap state | `features = bitmap()` |

For a caller-bound Delta table, `Schema.constraints = (check(predicate, name=...),)` declares expected native CHECK
metadata. See the [Delta tables API](DeltaTables.api.md). This is separate from ordinary DataFrame validation modes.

**Details And Differences**

- Field factories are the declaration boundary: raw PySpark `StructField` objects and implicit source-type inference are
  outside the DSL.
- `nullable=`, `alias=`, `metadata=`, and `description=` belong on field factories.
- `decimal(...)` requires precision and scale in the type contract.
- `timestamp()` represents Spark's instant-based TimestampType; `timestamp_ntz()` represents wall-clock
  TimestampNTZType. The two types are not interchangeable. Python `datetime` annotations continue to map to
  `timestamp()`; declare `timestamp_ntz()` explicitly for NTZ fields.
- `field.interval(...)` and `types.interval(...)` require exactly one of `type=` (a compound qualifier such as
  `Interval.YEAR_TO_MONTH`) or `unit=` (one field such as `Interval.DAY`). The qualifier is preserved in generated
  `YearMonthIntervalType` or `DayTimeIntervalType`. `Interval.CALENDAR` is expression-only on PySpark 3.5 and may be
  declared in PySpark 4.0 schemas. DayTime values convert to Python `timedelta`; YearMonth and Calendar Row
  conversion is not portable (and fails in tested PySpark 4.0/Connect paths). Use interval fields in expressions and
  select non-interval outputs for collection.
- `variant(...)` declares Spark's opaque semi-structured `VariantType`. It preserves schema and field nullability only.
  A transform using it must resolve to a PySpark 4 profile, including when that profile comes from
  `[tool.structure.plugin.pyspark]`.
- `geometry(srid=...)` and `geography(srid=...)` are planned target-gated declarations. A resolved provider owns their
  physical Spark type. A literal SRID is fixed; `srid=None` is mixed. The default baseline has no stable spatial field
  claim. See the [Geospatial reference](../reference/Geospatial.ref.md).
- `hll_sketch(...)` and `bitmap()` preserve an opaque algorithm brand even though Spark materializes Binary. HLL
  precision is part of the declared type. See [Sketches and Bitmaps](Aggregations.api.md#sketches-and-bitmaps).

## Nested Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `array(...)` | `ArrayType` | `tags = array(string())` |
| `map(...)` | `MapType` | `labels = map(string(), string())` |
| `struct(...)` | nested `StructType` | `address = struct(Address)` |
| `types.decimal(...)` | `DecimalType` | `decimal = types.decimal(12, 2)` |

**Details And Differences**

- `array(...)` and `map(...)` make nested element/value nullability compiler-visible.
- `struct(...)` enables typed nested attribute reads and `get_field(...)` expression access.
- `types.decimal(...)` is the standalone decimal type factory; use `decimal(...)` in schema declarations.

See the [Schemas reference](../reference/Schema.ref.md) for construction and nullability rules.

## Compatibility

See the exhaustive [Schemas compatibility ledger](../compatibility/Schemas.compat.md).
