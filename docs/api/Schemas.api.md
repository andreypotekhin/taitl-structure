# Schemas API

Schema declarations define Structure's typed row contract and materialize to Spark SQL schemas.

## Simple Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Schema` | `StructType` | `class Order(Schema): pass` |
| `field(...)` | `StructField` | `id = field(String(), nullable=False)` |
| `Boolean()` | Spark Boolean type | `field(Boolean())` |
| `Integer()` | Spark integer type | `field(Integer())` |
| `Long()` | Spark long type | `field(Long())` |
| `Float()` | Spark float type | `field(Float())` |
| `Double()` | Spark double type | `field(Double())` |
| `String()` | Spark string type | `field(String())` |
| `Date()` | Spark date type | `field(Date())` |
| `Timestamp()` | Spark timestamp type | `field(Timestamp())` |
| `Decimal(...)` | `DecimalType` | `total = field(Decimal(12, 2))` |

**Details And Differences**

- `field(...)` is the declaration boundary: raw PySpark `StructField` objects and implicit source-type inference are
  outside the DSL.
- `nullable=`, `primary_key=`, `alias=`, `metadata=`, and `description=` belong on `field(...)`.
- `Decimal(...)` requires precision and scale in the type contract.

## Nested Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `Array(...)` | `ArrayType` | `tags = field(Array(String()))` |
| `Map(...)` | `MapType` | `labels = field(Map(String(), String()))` |
| `Struct(...)` | nested `StructType` | `address = field(Struct(Address))` |
| `DecimalType` | `DecimalType` | `decimal = DecimalType(12, 2)` |

**Details And Differences**

- `Array(...)` and `Map(...)` make nested element/value nullability compiler-visible.
- `Struct(...)` enables typed nested attribute reads and `get_field(...)` expression access.
- `DecimalType` is the underlying decimal type class; use `Decimal(...)` in ordinary schema declarations.

See the [Schemas reference](../reference/Schema.ref.md) for construction and nullability rules.
