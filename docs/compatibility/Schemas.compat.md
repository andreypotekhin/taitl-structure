# Schemas Compatibility

This is the compatibility companion to the [API reference](../api/Schemas.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Schema declarations and type helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `Schema` | `StructType` | `Schema` | yes | yes | Declares a StructType from named fields; constructors preserve field order, types, and nullability. |
| `string(...)` | `StructField` | `id = string(nullable=False)` | yes | yes | Declares a StringType field; `nullable=False` makes it non-nullable in the generated StructType. |
| `boolean(...)` | Spark Boolean type | `boolean()` | yes | yes | Declares a BooleanType field with explicit field nullability. |
| `integer(...)` | Spark integer type | `integer()` | yes | yes | Declares a 32-bit IntegerType field with explicit field nullability. |
| `long(...)` | Spark long type | `long()` | yes | yes | Declares a 64-bit LongType field with explicit field nullability. |
| `float(...)` | Spark float type | `float()` | yes | yes | Declares a 32-bit FloatType field with explicit field nullability. |
| `double(...)` | Spark double type | `double()` | yes | yes | Declares a 64-bit DoubleType field with explicit field nullability. |
| `date(...)` | Spark date type | `date()` | yes | yes | Declares Spark DateType, a calendar date without a time-of-day. |
| `timestamp(...)` | Spark timestamp type | `timestamp()` | yes | yes | `timestamp()` represents Spark's instant-based TimestampType; `timestamp_ntz()` represents wall-clock TimestampNTZType. The two types are not interchangeable. Python `datetime` annotations continue to map to `timestamp()`; declare `timestamp_ntz()` explicitly for NTZ fields. |
| `timestamp_ntz(...)` | Spark timestamp without time zone | `timestamp_ntz()` | yes | yes | `timestamp()` represents Spark's instant-based TimestampType; `timestamp_ntz()` represents wall-clock TimestampNTZType. The two types are not interchangeable. Python `datetime` annotations continue to map to `timestamp()`; declare `timestamp_ntz()` explicitly for NTZ fields. |
| `field.interval(...)` | Qualified Spark interval | `field.interval(type=Interval.YEAR_TO_MONTH)` | yes | yes | `field.interval(...)` and `types.interval(...)` require exactly one of `type=` (a compound qualifier such as `Interval.YEAR_TO_MONTH`) or `unit=` (one field such as `Interval.DAY`). The qualifier is preserved in generated `YearMonthIntervalType` or `DayTimeIntervalType`. `Interval.CALENDAR` is expression-only on PySpark 3.5 and may be declared in PySpark 4.0 schemas. DayTime values convert to Python `timedelta`; YearMonth and Calendar Row conversion is not portable (and fails in tested PySpark 4.0/Connect paths). Use interval fields in expressions and select non-interval outputs for collection. |
| `binary(...)` | Spark binary type | `payload = binary(nullable=True)` | yes | yes | Declares BinaryType for opaque bytes; it does not infer an algorithm-specific sketch brand. |
| `decimal(...)` | `DecimalType` | `total = decimal(12, 2)` | yes | yes | `decimal(...)` requires precision and scale in the type contract. `types.decimal(...)` is the standalone decimal type factory; use `decimal(...)` in schema declarations. |
| `variant(...)` | `VariantType` | `payload = variant(nullable=True)` | yes | yes | `variant(...)` declares Spark's opaque semi-structured `VariantType`. It preserves schema and field nullability only. A transform using it must resolve to a PySpark 4 profile, including when that profile comes from `[tool.structure.plugin.pyspark]`. |
| `geometry(...)` | Target-gated provider-bound Geometry | `location = geometry(srid=4326)` | — | — | `geometry(srid=...)` and `geography(srid=...)` are planned target-gated declarations. A resolved provider owns their physical Spark type. A literal SRID is fixed; `srid=None` is mixed. The default baseline has no stable spatial field claim. See the [Geospatial reference](../reference/Geospatial.ref.md). |
| `geography(...)` | Target-gated provider-bound Geography | `location = geography(srid=4326)` | — | — | `geometry(srid=...)` and `geography(srid=...)` are planned target-gated declarations. A resolved provider owns their physical Spark type. A literal SRID is fixed; `srid=None` is mixed. The default baseline has no stable spatial field claim. See the [Geospatial reference](../reference/Geospatial.ref.md). |
| `hll_sketch(...)` | Branded opaque Binary HLL state | `customers = hll_sketch(lg_config_k=12)` | yes | yes | `hll_sketch(...)` and `bitmap()` preserve an opaque algorithm brand even though Spark materializes Binary. HLL precision is part of the declared type. See [Sketches and Bitmaps](../api/Aggregations.api.md#sketches-and-bitmaps). |
| `bitmap(...)` | Branded opaque Binary Bitmap state | `features = bitmap()` | yes | yes | `hll_sketch(...)` and `bitmap()` preserve an opaque algorithm brand even though Spark materializes Binary. HLL precision is part of the declared type. See [Sketches and Bitmaps](../api/Aggregations.api.md#sketches-and-bitmaps). |
| `array(...)` | `ArrayType` | `tags = array(string())` | yes | yes | `array(...)` and `map(...)` make nested element/value nullability compiler-visible. |
| `map(...)` | `MapType` | `labels = map(string(), string())` | yes | yes | `array(...)` and `map(...)` make nested element/value nullability compiler-visible. |
| `struct(...)` | `StructType` | `address = struct(Address)` | yes | yes | `struct(...)` enables typed nested attribute reads and `get_field(...)` expression access. |
| `types.decimal(...)` | `DecimalType` | `decimal = types.decimal(12, 2)` | yes | yes | `types.decimal(...)` is the standalone decimal type factory; use `decimal(...)` in schema declarations. |
## Unsupported

These PySpark functions or behaviors have no equivalent admitted Structure contract. Use the stated caller-owned or typed alternative.

| PySpark parity | Details |
| --- | --- |
| `struct` | Status: `caller-owned-guided`. PySpark infers a result schema from runtime columns; Structure requires a declared `Schema`. Migration: Declare the output `Schema` and construct typed fields; retain native `struct(...)` where inference is intentional. |
| `unwrap_udt` | Status: `caller-owned-guided`. Removing a user-defined type wrapper depends on opaque UDT metadata. Migration: Keep UDT unwrapping in native PySpark and declare the resulting Structure schema at the boundary. |
