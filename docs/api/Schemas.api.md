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
| `decimal(...)` | `DecimalType` | `total = decimal(12, 2)` |

**Details And Differences**

- Field factories are the declaration boundary: raw PySpark `StructField` objects and implicit source-type inference are
  outside the DSL.
- `nullable=`, `alias=`, `metadata=`, and `description=` belong on field factories.
- `decimal(...)` requires precision and scale in the type contract.

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
