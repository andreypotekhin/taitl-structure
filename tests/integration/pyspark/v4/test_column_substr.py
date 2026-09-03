from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import integer, string

pytestmark = pytest.mark.integration

PACKAGE = "integration_v4_column_substr_generated"


class SubstrInput(Schema):
    id = string(nullable=False)
    name = string(nullable=True)
    required_name = string(nullable=False)
    start = integer(nullable=False)
    length = integer(nullable=False)


class SubstrOutput(Schema):
    id = string(nullable=False)
    literal = string(nullable=True)
    dynamic = string(nullable=False)


@transform
class ColumnSubstr(Transform):
    rows = input(SubstrInput)
    output_rows = output(SubstrOutput)

    def publish(self, row: SubstrInput) -> SubstrOutput:
        return SubstrOutput(
            id=row.id,
            literal=row.name.substr(1, 3),
            dynamic=row.required_name.substr(row.start, row.length),
        )


def test_column_substr_matches_online_and_generated_execution(spark, tmp_path) -> None:
    source_module = ColumnSubstr.__module__
    files = render_generated_project(
        ColumnSubstr,
        source_transform=f"{source_module}.ColumnSubstr",
        generated_package=PACKAGE,
        source_schema_modules={source_module: [SubstrInput, SubstrOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/{source_module.replace('.', '/')}.py"
    assert ".substr(" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_column_substr")
        source = spark.createDataFrame(
            [
                ("row-1", "Ada", "Ada Lovelace", 1, 3),
                ("row-2", None, "Grace Hopper", 1, 5),
            ],
            generated_schemas.SUBSTR_INPUT_SCHEMA,
        )

        online = ColumnSubstr(rows=source).run(session(spark, execution_mode="online"))
        generated = ColumnSubstr(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.output_rows, "id")

    assert actual == [
        {"id": "row-1", "literal": "Ada", "dynamic": "Ada"},
        {"id": "row-2", "literal": None, "dynamic": "Grace"},
    ]
