from dataclasses import dataclass


@dataclass(frozen=True)
class StructType:
    fields: tuple


@dataclass(frozen=True)
class StructField:
    name: str
    dataType: object
    nullable: bool


class StringType:
    pass


class IntegerType:
    pass


class LongType:
    pass


class FloatType:
    pass


class DoubleType:
    pass


class BooleanType:
    pass


class DateType:
    pass


class TimestampType:
    pass


@dataclass(frozen=True)
class DecimalType:
    precision: int
    scale: int


@dataclass(frozen=True)
class ArrayType:
    elementType: object
    containsNull: bool = True


@dataclass(frozen=True)
class MapType:
    keyType: object
    valueType: object
    valueContainsNull: bool = True
