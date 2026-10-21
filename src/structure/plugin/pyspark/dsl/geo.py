"""Shared expression construction for explicitly namespaced spatial helpers."""

from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import (
    BinaryType,
    BooleanType,
    GeographyType,
    GeometryType,
    IntegerType,
    LongType,
    SedonaGeometryType,
    StringType,
)


def _provider_types(provider: str):
    return (SedonaGeometryType,) if provider == "sedona" else (GeometryType, GeographyType)


def spatial_call(
    provider: str, name: str, *values: object, default_srid: int | None = None
) -> Expression:
    args = tuple(literal(value) for value in values)
    provider_types = _provider_types(provider)
    input_spatial = next((arg.type for arg in args if isinstance(arg.type, provider_types)), None)
    output = input_spatial
    if name == "st_geomfromwkb":
        declared_srid = _literal_srid(args[1]) if len(args) > 1 else default_srid
        output = SedonaGeometryType(declared_srid) if provider == "sedona" else GeometryType(declared_srid)
    elif name == "st_geogfromwkb":
        output = GeographyType(4326 if provider in {"apache", "databricks"} else None)
    elif name == "st_geomfromwkt":
        output = SedonaGeometryType(_literal_srid(args[1]) if len(args) > 1 else None)
    elif name == "st_geomfromewkb":
        output = SedonaGeometryType(None)
    elif name == "st_astext":
        output = StringType()
    elif name in {"st_asbinary", "st_asewkb"}:
        output = BinaryType()
    elif name == "st_srid":
        output = IntegerType()
    elif name in {"st_intersects", "st_contains", "st_within"}:
        output = BooleanType()
        _validate_predicate(name, args, provider)
    elif name == "st_setsrid":
        output = _setsrid_type(args[0].type, args[1], provider)
    if output is None:
        raise TypeError(f"{provider}.{name}(...) requires a spatial input")
    if name in {"st_geomfromwkb", "st_geogfromwkb", "st_geomfromwkt", "st_geomfromewkb"}:
        if not args or not isinstance(args[0].type, BinaryType if name != "st_geomfromwkt" else StringType):
            expected = "String" if name == "st_geomfromwkt" else "Binary"
            raise TypeError(f"{provider}.{name}(...) requires a {expected} expression")
    if name in {"st_asbinary", "st_asewkb", "st_astext", "st_srid"} and not isinstance(args[0].type, provider_types):
        raise TypeError(f"{provider}.{name}(...) requires a spatial expression")
    nullable = any(arg.nullable for arg in args)
    return Expression(
        kind="call",
        type=output,
        nullable=nullable,
        data={"function": "geo_helper", "provider": provider, "operation": name},
        args=args,
    )


def _setsrid_type(input_type, srid: Expression, provider: str):
    if not isinstance(input_type, _provider_types(provider)):
        raise TypeError(f"{provider}.st_setsrid(...) requires a spatial expression")
    if not isinstance(srid.type, (IntegerType, LongType)):
        raise TypeError(f"{provider}.st_setsrid(...) requires an integral SRID")
    value = (srid.data or {}).get("value") if srid.kind == "literal" else None
    if isinstance(value, bool) or (value is not None and not isinstance(value, int)):
        raise TypeError("Spatial SRID must be an integer")
    return type(input_type)(value)


def _literal_srid(expression: Expression) -> int | None:
    if expression.kind != "literal":
        return None
    value = (expression.data or {}).get("value")
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("Spatial SRID must be an integer")
    return value


def _validate_predicate(name: str, args: tuple[Expression, ...], provider: str) -> None:
    if len(args) != 2 or not all(isinstance(arg.type, _provider_types(provider)) for arg in args):
        raise TypeError(f"{provider}.{name}(...) requires two spatial expressions")
    left, right = args
    if type(left.type) is not type(right.type):
        raise TypeError(f"{provider}.{name}(...) requires matching spatial representations")
    if isinstance(left.type, GeographyType) != isinstance(right.type, GeographyType):
        raise TypeError(f"{provider}.{name}(...) requires matching spatial kinds")
    left_srid = getattr(left.type, "srid", None)
    right_srid = getattr(right.type, "srid", None)
    if left_srid is None or left_srid != right_srid:
        raise TypeError(f"{provider}.{name}(...) requires the same fixed SRID on both inputs")


def contains(left: object, right: object) -> Expression:
    first, second = literal(left), literal(right)
    if not isinstance(first.type, StringType) or not isinstance(second.type, StringType):
        raise TypeError("contains(...) requires two String expressions")
    return Expression(
        kind="call", type=BooleanType(), nullable=first.nullable or second.nullable,
        data={"function": "contains"}, args=(first, second),
    )
