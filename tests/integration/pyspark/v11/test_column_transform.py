from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, special, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import string, trim, upper

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v11.test_column_transform"
PACKAGE = "integration_v11_column_transform_generated"


class TransformInput(Schema):
    value = string(nullable=False)


class TransformOutput(Schema):
    value = string(nullable=False)


@transform
class NormalizeValue(Transform):
    rows = input(TransformInput)
    normalized = output(TransformOutput)

    @special(type="expr")
    def normalize_text(value):
        return upper(trim(value))

    def normalize(self, row: TransformInput) -> TransformOutput:
        return TransformOutput(value=row.value.transform(self.normalize_text))


@pytest.mark.skipif(backend_name() != "pyspark41", reason="Column.transform requires ordinary PySpark 4.1")
def test_column_transform_matches_online_and_generated_execution(spark, tmp_path) -> None:
    files = render_generated_project(
        NormalizeValue,
        source_transform=f"{SOURCE_MODULE}.NormalizeValue",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [TransformInput, TransformOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_column_transform.py"
    generated = "".join(files[transform_path].split())
    assert '.transform(lambda_column:F.upper(F.trim(F.col("transform_input.value"))))' in generated

    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_column_transform")
        source = spark.createDataFrame([("  hello  ",)], schemas.TRANSFORM_INPUT_SCHEMA)
        online = NormalizeValue(rows=source).run(session(spark, execution_mode="online"))
        generated = NormalizeValue(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )

        assert_online_generated_parity(lambda: online, lambda: generated)
        assert rows(generated.normalized) == [{"value": "HELLO"}]
