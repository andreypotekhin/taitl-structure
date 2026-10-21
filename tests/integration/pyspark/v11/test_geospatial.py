from __future__ import annotations

import importlib
import os
import re

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    binary,
    geography,
    geometry,
    integer,
    st_asbinary,
    st_geogfromwkb,
    st_geomfromwkb,
    st_setsrid,
    st_srid,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="Apache native spatial helpers require exact PySpark 4.1 ordinary or Connect profiles.",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_geospatial"
PACKAGE = "integration_v11_geospatial_generated"


class SpatialInput(Schema):
    id = integer(nullable=False)
    payload = binary(nullable=False)


class SpatialOutput(Schema):
    id = integer(nullable=False)
    shape = geometry(srid=0, nullable=False)
    fixed_shape = geometry(srid=4326, nullable=False)
    geog = geography(srid=4326, nullable=False)
    payload = binary(nullable=False)
    srid = integer(nullable=False)
    geog_srid = integer(nullable=False)


@transform
class DecodeNativeGeometry(Transform):
    rows = input(SpatialInput)
    decoded = output(SpatialOutput)

    def decode(self, row: SpatialInput) -> SpatialOutput:
        shape = st_geomfromwkb(row.payload)
        geog = st_geogfromwkb(row.payload)
        return SpatialOutput(
            id=row.id,
            shape=shape,
            fixed_shape=st_setsrid(shape, 4326),
            geog=geog,
            payload=st_asbinary(shape),
            srid=st_srid(shape),
            geog_srid=st_srid(geog),
        )


def _files():
    files = render_generated_project(
        DecodeNativeGeometry,
        source_transform=f"{SOURCE_MODULE}.DecodeNativeGeometry",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [SpatialInput, SpatialOutput]},
    )
    generated = "\n".join(files.values())
    compact = re.sub(r"\s+", "", generated)
    assert "call_geospatial_function('apache','st_geomfromwkb',F.col(\"spatial_input.payload\"))" in compact
    assert "call_geospatial_function('apache','st_asbinary',call_geospatial_function(" in compact
    assert not any(token in generated for token in ("SparkContext", "_jdf", "_jvm", ".rdd"))
    return files


def test_native_geometry_matches_online_generated_and_spark(spark, tmp_path) -> None:
    if os.environ.get("STRUCTURE_SEDONA_TESTS") == "1":
        pytest.skip("Sedona replaces Spark's unqualified ST function registrations; native parity runs separately.")
    from pyspark.sql import functions as F

    # Spark 4.1 guards native spatial execution behind this session setting.
    spark.conf.set("spark.sql.geospatial.enabled", "true")
    files = _files()
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_geospatial")
        native_shape = F.st_geomfromwkb(F.lit(bytes.fromhex("0101000000000000000000f03f0000000000000040")))
        payload = spark.range(0, 1).select(F.st_asbinary(native_shape).alias("payload")).first()[0]
        source = spark.createDataFrame([(1, payload)], schemas.SPATIAL_INPUT_SCHEMA)
        online = DecodeNativeGeometry(rows=source).run(session(spark, execution_mode="online")).decoded
        generated = DecodeNativeGeometry(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).decoded
        native = source.select(
            "id",
            F.st_geomfromwkb("payload").alias("shape"),
            F.st_setsrid(F.st_geomfromwkb("payload"), F.lit(4326)).alias("fixed_shape"),
            F.st_geogfromwkb("payload").alias("geog"),
            F.st_asbinary(F.st_geomfromwkb("payload")).alias("payload"),
            F.st_srid(F.st_geomfromwkb("payload")).alias("srid"),
            F.st_srid(F.st_geogfromwkb("payload")).alias("geog_srid"),
        )

        assert_online_generated_parity(lambda: online.drop("shape"), lambda: generated.drop("shape"))
        assert online.schema == generated.schema == native.schema

        def values(frame):
            return {row["id"]: (row["payload"], row["srid"]) for row in rows(frame, "id")}

        assert values(online) == values(generated) == values(native) == {1: (payload, 0)}


@pytest.mark.skipif(
    backend_name() not in {"pyspark41", "spark-connect41"}
    or os.environ.get("STRUCTURE_SEDONA_TESTS") != "1",
    reason="Requires the isolated Spark 4.1 Sedona registration collision phase.",
)
def test_sedona_registration_conflicts_with_native_st_function_names(spark) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import GeometryType
    from sedona.spark import SedonaContext  # type: ignore[import-not-found]

    spark.conf.set("spark.sql.geospatial.enabled", "true")
    SedonaContext.create(spark)
    native_shape = F.st_geomfromwkb(
        F.lit(bytes.fromhex("0101000000000000000000f03f0000000000000040"))
    )
    actual = spark.range(0, 1).select(native_shape.alias("shape")).schema["shape"].dataType

    assert actual != GeometryType(0)
    assert str(actual) == "GeometryType()"
