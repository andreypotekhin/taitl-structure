from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DateType import DateType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.TimestampType import TimestampType


def literal(value: object) -> Expression:
    if isinstance(value, Expression):
        return value

    if isinstance(value, WhenBuilder):
        raise TypeError("when(...) must end with .otherwise(...) before it can be used as an expression")

    if isinstance(value, bool):
        return Expression(kind="literal", type=BooleanType(), nullable=False, data={"value": value})

    if isinstance(value, str):
        return Expression(kind="literal", type=StringType(), nullable=False, data={"value": value})

    if isinstance(value, int):
        type = IntegerType() if -(2**31) <= value <= 2**31 - 1 else LongType()
        return Expression(kind="literal", type=type, nullable=False, data={"value": value})

    if isinstance(value, float):
        return Expression(kind="literal", type=DoubleType(), nullable=False, data={"value": value})

    if isinstance(value, datetime):
        return Expression(kind="literal", type=TimestampType(), nullable=False, data={"value": value})

    if isinstance(value, date):
        return Expression(kind="literal", type=DateType(), nullable=False, data={"value": value})

    if value is None:
        return Expression(kind="literal", type=None, nullable=True, data={"value": None})

    return Expression(kind="literal", type=None, nullable=False, data={"value": value})


def lower(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "lower"}, args=(argument,)
    )


def trim(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "trim"}, args=(argument,)
    )


def upper(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "upper"}, args=(argument,)
    )


def to_decimal(value: object, *, precision: int, scale: int) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call",
        type=DecimalType(precision=precision, scale=scale),
        nullable=argument.nullable,
        data={"function": "to_decimal", "precision": precision, "scale": scale},
        args=(argument,),
    )


def coalesce(*values: object) -> Expression:
    arguments = tuple(literal(value) for value in values)
    result_type = next((argument.type for argument in arguments if argument.type is not None), None)
    return Expression(kind="call", type=result_type, nullable=False, data={"function": "coalesce"}, args=arguments)


def when(condition: object, value: object) -> "WhenBuilder":
    predicate = literal(condition)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("when(...) requires a boolean Structure expression as its condition")
    return WhenBuilder(condition=predicate, value=literal(value))


@dataclass(frozen=True)
class WhenBuilder:
    condition: Expression
    value: Expression

    def otherwise(self, fallback: object) -> Expression:
        alternative = literal(fallback)
        return Expression(
            kind="when",
            type=self.value.type or alternative.type,
            nullable=self.value.nullable or alternative.nullable,
            args=(self.condition, self.value, alternative),
        )
