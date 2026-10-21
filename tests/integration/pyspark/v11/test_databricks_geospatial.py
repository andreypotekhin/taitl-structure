from __future__ import annotations

import importlib
import os

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import binary, databricks, geography, geometry, integer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("STRUCTURE_DATABRICKS_TESTS") != "1",
        reason="Requires an explicitly selected, caller-owned Databricks Spark or Databricks Connect session.",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_databricks_geospatial"
PACKAGE = "integration_v11_databricks_geospatial_generated"


class DatabricksInput(Schema):
    id = integer(nullable=False)
    payload = binary(nullable=False)


class DatabricksOutput(Schema):
    id = integer(nullable=False)
    shape = geometry(srid=4326, nullable=False)
    geography = geography(srid=4326, nullable=False)
    payload = binary(nullable=False)
    srid = integer(nullable=False)
    reassigned = geometry(srid=3857, nullable=False)
    geography_srid = integer(nullable=False)
    reassigned_srid = integer(nullable=False)


@transform
class DatabricksSpatialCalls(Transform):
    rows = input(DatabricksInput)
    result = output(DatabricksOutput)

    def convert(self, row: DatabricksInput) -> DatabricksOutput:
        shape = databricks.st_geomfromwkb(row.payload, 4326)
        geog = databricks.st_geogfromwkb(row.payload)
        reassigned = databricks.st_setsrid(shape, 3857)
        return DatabricksOutput(
            id=row.id,
            shape=shape,
            geography=geog,
            payload=databricks.st_asbinary(shape),
            srid=databricks.st_srid(shape),
            reassigned=reassigned,
            geography_srid=databricks.st_srid(geog),
            reassigned_srid=databricks.st_srid(reassigned),
        )


def _caller_session():
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    remote = os.environ.get("SPARK_REMOTE")
    if remote:
        return SparkSession.builder.remote(remote).getOrCreate()
    pytest.fail(
        "STRUCTURE_DATABRICKS_TESTS=1 requires an active Databricks SparkSession or caller-configured SPARK_REMOTE; "
        "the test will not start a local Spark service."
    )


def test_databricks_helpers_match_online_generated_and_native_api(tmp_path) -> None:
    from pyspark.databricks.sql import functions as dbf
    spark = _caller_session()
    files = render_generated_project(
        DatabricksSpatialCalls,
        source_transform=f"{SOURCE_MODULE}.DatabricksSpatialCalls",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [DatabricksInput, DatabricksOutput]},
    )
    source_code = "\n".join(files.values())
    assert "call_geospatial_function('databricks', 'st_geomfromwkb'" in source_code
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_databricks_geospatial")
        payload = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        source = spark.createDataFrame([(1, payload)], schemas.DATABRICKS_INPUT_SCHEMA)
        online = DatabricksSpatialCalls(rows=source).run(session(spark, execution_mode="online")).result
        generated = DatabricksSpatialCalls(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).result
        shape = dbf.st_geomfromwkb("payload", 4326)
        native = source.select(
            "id",
            shape.alias("shape"),
            dbf.st_geogfromwkb("payload").alias("geography"),
            dbf.st_asbinary(shape).alias("payload"),
            dbf.st_srid(shape).alias("srid"),
            dbf.st_setsrid(shape, 3857).alias("reassigned"),
            dbf.st_srid(dbf.st_geogfromwkb("payload")).alias("geography_srid"),
            dbf.st_srid(dbf.st_setsrid(shape, 3857)).alias("reassigned_srid"),
        )
        assert_online_generated_parity(
            lambda: online.drop("shape", "geography", "reassigned"),
            lambda: generated.drop("shape", "geography", "reassigned"),
        )
        assert online.schema == generated.schema == native.schema

        def values(frame):
            return {
                row["id"]: (row["payload"], row["srid"], row["geography_srid"], row["reassigned_srid"])
                for row in rows(frame, "id")
            }

        expected = {1: (payload, 4326, 4326, 3857)}
        assert values(online) == values(generated) == values(native) == expected
