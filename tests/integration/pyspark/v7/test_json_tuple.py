from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import json_tuple, string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_json_tuple"
PACKAGE = "integration_v7_json_tuple_generated"


class Document(Schema):
    key = string(nullable=False)
    payload = string(nullable=True)


class Extracted(Schema):
    customer_id = string(nullable=True)
    tier = string(nullable=True)


class Published(Schema):
    key = string(nullable=False)
    customer_id = string(nullable=True)
    tier = string(nullable=True)


@transform
class ExtractJsonTuple(Transform):
    documents = input(Document)
    published = output(Published)

    def publish(self, document: Document) -> Published:
        extracted = json_tuple(
            document.payload,
            as_=Extracted,
            fields={"customer_id": "customerId"},
            scope="payload_fields",
        )
        return Published(key=document.key, customer_id=extracted.customer_id, tier=extracted.tier)


def test_json_tuple_matches_online_and_generated_execution(spark, tmp_path) -> None:
    files = render_generated_project(
        ExtractJsonTuple,
        source_transform=f"{SOURCE_MODULE}.ExtractJsonTuple",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Document, Extracted, Published]},
    )
    transform_source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_json_tuple.py"]
    assert "F.json_tuple" in transform_source

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_json_tuple")
        source = spark.createDataFrame(
            [
                ("present", '{"customerId":"c-1","tier":"gold"}'),
                ("missing", '{"customerId":"c-2"}'),
                ("null", None),
                ("malformed", "not-json"),
                ("typed-values", '{"customerId":7,"tier":true}'),
                ("nested", '{"customerId":"c-3","nested":{"tier":"silver"}}'),
            ],
            generated_schemas.DOCUMENT_SCHEMA,
        )
        online = ExtractJsonTuple(documents=source).run(session(spark, execution_mode="online"))
        generated = ExtractJsonTuple(documents=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.published, "key")

    assert actual == [
        {"key": "malformed", "customer_id": None, "tier": None},
        {"key": "missing", "customer_id": "c-2", "tier": None},
        {"key": "nested", "customer_id": "c-3", "tier": None},
        {"key": "null", "customer_id": None, "tier": None},
        {"key": "present", "customer_id": "c-1", "tier": "gold"},
        {"key": "typed-values", "customer_id": "7", "tier": "true"},
    ]
