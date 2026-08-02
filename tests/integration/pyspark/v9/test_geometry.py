from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import session

from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import geometry, geometry_as_wkt, string

pytestmark = pytest.mark.integration


class GeometryInput(Schema):
    shape = geometry(srid=4326, nullable=True)


class GeometryOutput(Schema):
    wkt = string(nullable=True)


@transform
class ReadGeometry(Transform):
    rows = input(GeometryInput)
    result = output(GeometryOutput)

    def convert(self, row: GeometryInput) -> GeometryOutput:
        return GeometryOutput(wkt=geometry_as_wkt(row.shape))


def test_sedona_geometry_wkt_round_trip(spark) -> None:
    source = spark.sql("SELECT ST_GeomFromWKT('POINT (1 2)', 4326) AS shape")
    result = ReadGeometry(rows=source).run(session(spark, execution_mode="online"))
    assert result.result.first().wkt == "POINT (1 2)"
