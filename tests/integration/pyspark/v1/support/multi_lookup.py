from __future__ import annotations

import importlib

from integration.pyspark.fixtures.multi_lookup import AddLookupProduct, LookupEnriched, LookupOrder, LookupProduct

MODULE = "integration.pyspark.fixtures.multi_lookup"


def source_schema_modules():
    return {
        MODULE: [LookupOrder, LookupProduct, LookupEnriched],
    }


def generated_schemas(package: str):
    return importlib.import_module(f"{package}.pyspark.schemas.multi_lookup")


def input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(
            [("o-1", "p-1"), ("o-2", "missing")],
            schema=schemas.LOOKUP_ORDER_SCHEMA,
        ),
        "products": spark.createDataFrame(
            [("p-1", "Engine")],
            schema=schemas.LOOKUP_PRODUCT_SCHEMA,
        ),
    }
