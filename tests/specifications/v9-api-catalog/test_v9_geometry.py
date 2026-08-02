import pytest

from structure.plugin.pyspark import contains, geometry, geometry_as_wkt, geometry_from_wkt, intersects, string, within
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.types import GeometryType, StringType
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression


def test_geometry_requires_positive_srid() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        geometry(srid=0)


def test_geometry_from_wkt_and_serialization_are_typed() -> None:
    value = geometry_from_wkt("POINT (0 0)", srid=4326)

    assert isinstance(value.type, GeometryType)
    assert value.type.srid == 4326
    assert isinstance(geometry_as_wkt(value).type, StringType)


def test_geometry_predicates_require_matching_srids() -> None:
    left = geometry_from_wkt("POINT (0 0)", srid=4326)
    right = geometry_from_wkt("POINT (0 0)", srid=3857)

    with pytest.raises(TypeError, match="same SRID"):
        intersects(left, right)
    with pytest.raises(TypeError, match="same SRID"):
        contains(left, right)
    with pytest.raises(TypeError, match="same SRID"):
        within(left, right)


def test_geometry_calls_render_through_common_spark_functions() -> None:
    recipe = MapPySparkExpression().map(
        geometry_from_wkt("POINT (0 0)", srid=4326),
        capabilities=PySparkCapabilities(),
    )

    assert RenderPySparkExpression()(recipe) == "F.call_function('ST_GeomFromWKT', F.lit('POINT (0 0)'), F.lit(4326))"
