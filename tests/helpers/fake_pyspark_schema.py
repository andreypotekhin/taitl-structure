from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StructType:
    fields: tuple


@dataclass(frozen=True)
class StructField:
    name: str
    dataType: object
    nullable: bool
    metadata: dict[str, Any] | None = None

    def __post_init__(self):
        if self.metadata is None:
            object.__setattr__(self, "metadata", {})


class StringType:
    pass


class BinaryType:
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
