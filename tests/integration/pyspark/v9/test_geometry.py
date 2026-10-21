from __future__ import annotations

import os

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)

from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import binary, boolean, integer, sedona, string

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name().endswith("41") and os.environ.get("STRUCTURE_SEDONA_TESTS") != "1",
        reason=(
            "The Spark 4.1 Compose lane does not install Sedona JVM packages or its Spark extension; "
            "see Geospatial.ref.md."
        ),
    ),
]


class GeometryInput(Schema):
    shape = sedona.geometry(srid=4326, nullable=False)
    polygon_wkt = string(nullable=False)
    point_wkt = string(nullable=False)
    polygon_wkb = binary(nullable=False)
    polygon_ewkb = binary(nullable=False)


class GeometryOutput(Schema):
    wkt = string(nullable=False)
    wkb = binary(nullable=False)
    ewkb = binary(nullable=False)
    srid = integer(nullable=False)
    reset_srid = integer(nullable=False)
    intersects = boolean(nullable=False)
    contains = boolean(nullable=False)
    within = boolean(nullable=False)


@transform
class ReadGeometry(Transform):
    rows = input(GeometryInput)
    result = output(GeometryOutput)

    def convert(self, row: GeometryInput) -> GeometryOutput:
        polygon = sedona.st_geomfromwkt(row.polygon_wkt, 4326)
        point = sedona.st_geomfromwkt(row.point_wkt, 4326)
        source_shape = row.shape
        from_wkb = sedona.st_geomfromwkb(row.polygon_wkb)
        from_ewkb = sedona.st_geomfromewkb(row.polygon_ewkb)
        return GeometryOutput(
            wkt=sedona.st_astext(polygon),
            wkb=sedona.st_asbinary(from_wkb),
            ewkb=sedona.st_asewkb(from_ewkb),
            srid=sedona.st_srid(source_shape),
            reset_srid=sedona.st_srid(sedona.st_setsrid(source_shape, 4326)),
            intersects=sedona.st_intersects(source_shape, point),
            contains=sedona.st_contains(source_shape, point),
            within=sedona.st_within(point, source_shape),
        )


def test_sedona_geometry_helpers_match_online_generated_and_spark(spark, tmp_path) -> None:
    from sedona.spark import SedonaContext  # type: ignore[import-not-found]

    # Registers Sedona's JVM wrappers for the explicit `sedona.*` Python API.
    spark = SedonaContext.create(spark)
    source = spark.sql(
        """SELECT
          'POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))' AS polygon_wkt,
          'POINT (1 2)' AS point_wkt,
          ST_GeomFromWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))', 4326) AS shape,
          ST_AsBinary(ST_GeomFromWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))', 4326)) AS polygon_wkb,
          ST_AsEWKB(ST_GeomFromWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))', 4326)) AS polygon_ewkb"""
    )
    package = "integration_v9_geometry_generated"
    files = render_generated_project(
        ReadGeometry,
        source_transform=f"{ReadGeometry.__module__}.ReadGeometry",
        generated_package=package,
        source_schema_modules={ReadGeometry.__module__: [GeometryInput, GeometryOutput]},
    )
    direct = source.selectExpr(
        "ST_AsText(ST_GeomFromWKT(polygon_wkt, 4326)) AS wkt",
        "ST_AsBinary(ST_GeomFromWKB(polygon_wkb)) AS wkb",
        "ST_AsEWKB(ST_GeomFromEWKB(polygon_ewkb)) AS ewkb",
        "ST_SRID(ST_GeomFromWKT(polygon_wkt, 4326)) AS srid",
        "ST_SRID(ST_SetSRID(ST_GeomFromWKT(polygon_wkt, 4326), 4326)) AS reset_srid",
        "ST_Intersects(ST_GeomFromWKT(polygon_wkt, 4326), ST_GeomFromWKT(point_wkt, 4326)) AS intersects",
        "ST_Contains(ST_GeomFromWKT(polygon_wkt, 4326), ST_GeomFromWKT(point_wkt, 4326)) AS contains",
        "ST_Within(ST_GeomFromWKT(point_wkt, 4326), ST_GeomFromWKT(polygon_wkt, 4326)) AS within",
    )
    with generated_project(tmp_path, package, files):
        online = ReadGeometry(rows=source).run(session(spark, execution_mode="online")).result
        generated = ReadGeometry(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        ).result
        assert online.schema == generated.schema == direct.schema
        assert online.first() == generated.first() == direct.first()
