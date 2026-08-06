from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import CsvOptions, from_csv, from_json, integer, string, struct, to_csv, to_json

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_schema_carrying_parsing"
PACKAGE = "integration_v7_schema_carrying_parsing_generated"


class Payload(Schema):
    code = string(nullable=True)
    amount = integer(nullable=True)


class RawPayload(Schema):
    id = string(nullable=False)
    payload_json = string(nullable=True)
    payload_csv = string(nullable=True)
    payload = struct(Payload, nullable=True)


class ParsedPayload(Schema):
    id = string(nullable=False)
    from_json_payload = struct(Payload, nullable=True)
    from_csv_payload = struct(Payload, nullable=True)
    payload_json = string(nullable=True)
    payload_csv = string(nullable=True)


@transform
class ParsePayloads(Transform):
    rows = input(RawPayload)
    parsed = output(ParsedPayload)

    def publish(self, row: RawPayload) -> ParsedPayload:
        return ParsedPayload(
            id=row.id,
            from_json_payload=from_json(row.payload_json, as_=Payload),
            from_csv_payload=from_csv(row.payload_csv, as_=Payload, options=CsvOptions(delimiter="|")),
            payload_json=to_json(row.payload),
            payload_csv=to_csv(row.payload, options=CsvOptions(delimiter="|")),
        )


def test_v7_schema_carrying_parsing_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        ParsePayloads,
        source_transform=f"{SOURCE_MODULE}.ParsePayloads",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Payload, RawPayload, ParsedPayload]},
    )
    transform_source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_schema_carrying_parsing.py"]
    assert "from_json" in transform_source
    assert "to_json" in transform_source
    assert "from_csv" in transform_source
    assert "to_csv" in transform_source

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_schema_carrying_parsing")
        source = spark.createDataFrame(
            [
                ("row-1", '{"code":"paid","amount":2}', "paid|2", {"code": "ship", "amount": 4}),
                ("row-2", None, None, None),
            ],
            generated_schemas.RAW_PAYLOAD_SCHEMA,
        )

        online = ParsePayloads(rows=source).run(session(spark, execution_mode="online"))
        generated = ParsePayloads(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)

        actual = rows(generated.parsed, "id")

    assert actual == [
        {
            "id": "row-1",
            "from_json_payload": {"code": "paid", "amount": 2},
            "from_csv_payload": {"code": "paid", "amount": 2},
            "payload_json": '{"code":"ship","amount":4}',
            "payload_csv": "ship|4",
        },
        {
            "id": "row-2",
            "from_json_payload": None,
            "from_csv_payload": None,
            "payload_json": None,
            "payload_csv": None,
        },
    ]
