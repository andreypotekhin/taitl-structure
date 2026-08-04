from __future__ import annotations

import importlib
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import base64, binary, decode, encode, string, unbase64

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_binary_encoding"
PACKAGE = "integration_v7_binary_encoding_generated"


class EncodedInput(Schema):
    id = string(nullable=False)
    payload = binary(nullable=True)
    text = string(nullable=True)
    base64_text = string(nullable=True)


class DecodedOutput(Schema):
    id = string(nullable=False)
    payload_base64 = string(nullable=True)
    payload_text = string(nullable=True)
    text_payload = binary(nullable=True)
    decoded_payload = binary(nullable=True)


@transform
class DecodePayloads(Transform):
    rows = input(EncodedInput)
    decoded = output(DecodedOutput)

    def publish(self, row: EncodedInput) -> DecodedOutput:
        return DecodedOutput(
            id=row.id,
            payload_base64=base64(row.payload),
            payload_text=decode(row.payload, charset="UTF-8"),
            text_payload=encode(row.text, charset="UTF-8"),
            decoded_payload=unbase64(row.base64_text),
        )


def test_v7_binary_encoding_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        DecodePayloads,
        source_transform=f"{SOURCE_MODULE}.DecodePayloads",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [EncodedInput, DecodedOutput]},
    )
    assert "T.BinaryType()" in files[f"{PACKAGE}/pyspark/schemas/test_binary_encoding.py"]
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_binary_encoding.py"
    assert "F.base64(" in files[transform_path]
    assert "F.unbase64(" in files[transform_path]
    assert "F.encode(" in files[transform_path]
    assert "F.decode(" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_binary_encoding")
        source = spark.createDataFrame(
            [
                ("row-1", bytearray(b"paid"), "ship", "cGFpZA=="),
                ("row-2", None, None, None),
            ],
            generated_schemas.ENCODED_INPUT_SCHEMA,
        )

        assert_online_generated_parity(
            lambda: DecodePayloads(rows=source).run(session(spark, execution_mode="online")),
            lambda: DecodePayloads(rows=source).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            ),
        )

        actual = rows(
            DecodePayloads(rows=source).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).decoded,
            "id",
        )

    assert actual[0]["id"] == "row-1"
    assert actual[0]["payload_base64"] == "cGFpZA=="
    assert actual[0]["payload_text"] == "paid"
    assert bytes(cast(bytearray, actual[0]["text_payload"])) == b"ship"
    assert bytes(cast(bytearray, actual[0]["decoded_payload"])) == b"paid"
    assert actual[1] == {
        "id": "row-2",
        "payload_base64": None,
        "payload_text": None,
        "text_payload": None,
        "decoded_payload": None,
    }
