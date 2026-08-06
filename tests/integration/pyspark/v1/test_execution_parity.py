from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows
from integration.pyspark.v1.fixtures.parity import schemas, transforms

from structure.lib.testing import assert_online_generated_parity

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v1.fixtures.parity.transforms"
SCHEMA_MODULE = "integration.pyspark.v1.fixtures.parity.schemas"


def test_parent_hook_owner_has_online_generated_runtime_parity(spark, tmp_path) -> None:
    package = "integration_parent_hook_generated"
    files = render_generated_project(
        transforms.ParentHookPublished,
        source_transform=f"{SOURCE_MODULE}.ParentHookPublished",
        generated_package=package,
        source_schema_modules={SCHEMA_MODULE: [schemas.RawRow, schemas.NormalizedRow, schemas.PublishedRow]},
    )

    with generated_project(tmp_path, package, files):
        generated_schemas = importlib.import_module(f"{package}.pyspark.schemas.schemas")
        frame = spark.createDataFrame([("one",)], generated_schemas.RAW_ROW_SCHEMA)
        online = transforms.ParentHookPublished(rows=frame).run(session(spark, execution_mode="online"))
        generated = transforms.ParentHookPublished(rows=frame).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)

        assert rows(online.published) == [{"id": "one", "hook_owner": "parent"}]


def test_embedded_parent_hook_has_online_generated_runtime_parity(spark, tmp_path) -> None:
    package = "integration_embedded_parent_hook_generated"
    files = render_generated_project(
        transforms.ParentHookPublished,
        source_transform=f"{SOURCE_MODULE}.ParentHookPublished",
        generated_package=package,
        source_schema_modules={SCHEMA_MODULE: [schemas.RawRow, schemas.NormalizedRow, schemas.PublishedRow]},
        generated_code_options=("embed_hooks",),
    )
    transform_source = files[
        f"{package}/pyspark/transforms/integration/pyspark/v1/fixtures/parity/transforms.py"
    ]

    assert SOURCE_MODULE not in transform_source
    assert "self._impl" not in transform_source

    with generated_project(tmp_path, package, files):
        generated_schemas = importlib.import_module(f"{package}.pyspark.schemas.schemas")
        generated_transforms = importlib.import_module(
            f"{package}.pyspark.transforms.integration.pyspark.v1.fixtures.parity.transforms"
        )
        frame = spark.createDataFrame([("one",)], generated_schemas.RAW_ROW_SCHEMA)
        online = transforms.ParentHookPublished(rows=frame).run(session(spark, execution_mode="online"))
        generated = generated_transforms.ParentHookPublishedGenerated(spark=spark).run(rows=frame)

        assert_online_generated_parity(lambda: online, lambda: generated)


def test_watermarked_stream_static_lookup_has_online_generated_plan_parity(spark, tmp_path, capsys) -> None:
    package = "integration_watermark_generated"
    files = render_generated_project(
        transforms.WatermarkedLookup,
        source_transform=f"{SOURCE_MODULE}.WatermarkedLookup",
        generated_package=package,
        source_schema_modules={SCHEMA_MODULE: [schemas.StreamEvent, schemas.StreamCustomer, schemas.StreamEnriched]},
    )

    with generated_project(tmp_path, package, files):
        generated_schemas = importlib.import_module(f"{package}.pyspark.schemas.schemas")
        from pyspark.sql import functions as F

        events = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", 1)
            .load()
            .select(
                F.col("value").cast("string").alias("id"),
                F.col("timestamp").alias("event_time"),
            )
        )
        customers = spark.createDataFrame([("0", "known")], generated_schemas.STREAM_CUSTOMER_SCHEMA)
        online = (
            transforms.WatermarkedLookup(events=events, customers=customers)
            .run(session(spark, execution_mode="online"))
            .enriched
        )
        generated = (
            transforms.WatermarkedLookup(events=events, customers=customers)
            .run(session(spark, execution_mode="generated", generated_package=package))
            .enriched
        )

        assert online.isStreaming and generated.isStreaming
        online.explain(extended=True)
        generated.explain(extended=True)
        assert capsys.readouterr().out.count("EventTimeWatermark") >= 2
