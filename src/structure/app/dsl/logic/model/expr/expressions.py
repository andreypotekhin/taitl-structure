from __future__ import annotations

from datetime import date, datetime

from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.types.BooleanType import BooleanType
from structure.app.dsl.logic.model.types.DateType import DateType
from structure.app.dsl.logic.model.types.DecimalType import DecimalType
from structure.app.dsl.logic.model.types.DoubleType import DoubleType
from structure.app.dsl.logic.model.types.IntegerType import IntegerType
from structure.app.dsl.logic.model.types.LongType import LongType
from structure.app.dsl.logic.model.types.StringType import StringType
from structure.app.dsl.logic.model.types.TimestampType import TimestampType


def literal(value: object) -> Expression:
    if isinstance(value, Expression):
        return value

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
