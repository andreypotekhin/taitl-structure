"""Provider-neutral Geometry expressions backed by common Spark SQL functions."""

from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import BooleanType, GeometryType, StringType


def geometry_from_wkt(value: object, *, srid: int) -> Expression:
    text = literal(value)
    if not isinstance(text.type, StringType):
        raise TypeError("geometry_from_wkt(...) requires a String expression")
    geometry = GeometryType(srid)
    return Expression(kind="call", type=geometry, nullable=text.nullable, data={"function": "geo_from_wkt", "srid": srid}, args=(text,))


def geometry_as_wkt(value: object) -> Expression:
    geometry = _geometry(value, "geometry_as_wkt")
    return Expression(kind="call", type=StringType(), nullable=geometry.nullable, data={"function": "geo_as_wkt"}, args=(geometry,))


def intersects(left: object, right: object) -> Expression:
    return _predicate("geo_intersects", left, right)


def contains(left: object, right: object) -> Expression:
    return _predicate("geo_contains", left, right)


def within(left: object, right: object) -> Expression:
    return _predicate("geo_within", left, right)


def _predicate(function: str, left: object, right: object) -> Expression:
    first, second = _geometry(left, function), _geometry(right, function)
    first_type, second_type = first.type, second.type
    assert isinstance(first_type, GeometryType)
    assert isinstance(second_type, GeometryType)
    if first_type.srid != second_type.srid:
        raise TypeError(f"{function.removeprefix('geo_')}(...) requires Geometry values with the same SRID")
    return Expression(kind="call", type=BooleanType(), nullable=first.nullable or second.nullable, data={"function": function}, args=(first, second))


def _geometry(value: object, call: str) -> Expression:
    expression = literal(value)
    if not isinstance(expression.type, GeometryType):
        raise TypeError(f"{call}(...) requires a Geometry expression")
    return expression
