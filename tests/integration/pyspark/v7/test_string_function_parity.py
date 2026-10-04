import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import binary, bit_length, boolean, btrim, endswith, integer, startswith, string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_string_function_parity"
PACKAGE = "integration_v7_string_function_generated"


class StringFunctionInput(Schema):
    label = string(nullable=False)
    trim_source = string(nullable=False)
    trim_chars = string(nullable=False)
    prefix = string(nullable=False)
    payload = binary(nullable=False)
    byte_prefix = binary(nullable=False)
    byte_suffix = binary(nullable=False)
    text_suffix = string(nullable=False)


class StringFunctionOutput(Schema):
    label = string(nullable=False)
    custom_trim = string(nullable=False)
    text_bits = integer(nullable=False)
    payload_bits = integer(nullable=False)
    has_prefix = boolean(nullable=False)
    has_byte_prefix = boolean(nullable=False)
    has_byte_suffix = boolean(nullable=False)
    has_mixed_prefix = boolean(nullable=False)
    has_mixed_suffix = boolean(nullable=False)


@transform
class StringFunctions(Transform):
    rows = input(StringFunctionInput)
    result = output(StringFunctionOutput)

    def publish(self, row: StringFunctionInput) -> StringFunctionOutput:
        return StringFunctionOutput(
            label=row.label,
            custom_trim=btrim(row.trim_source, trim=row.trim_chars),
            text_bits=bit_length(row.label),
            payload_bits=bit_length(row.payload),
            has_prefix=startswith(row.label, row.prefix),
            has_byte_prefix=startswith(row.payload, row.byte_prefix),
            has_byte_suffix=endswith(row.payload, row.byte_suffix),
            has_mixed_prefix=startswith(row.label, row.byte_prefix),
            has_mixed_suffix=endswith(row.payload, row.text_suffix),
        )


def test_string_functions_match_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        StringFunctions,
        source_transform=f"{SOURCE_MODULE}.StringFunctions",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StringFunctionInput, StringFunctionOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_string_function_parity.py"
    assert "F.bit_length(" in files[transform_path]
    assert ".btrim(" in files[transform_path]
    assert "startswith(" in files[transform_path]
    assert "endswith(" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = __import__(f"{PACKAGE}.pyspark.schemas.test_string_function_parity", fromlist=["*"])
        source = spark.createDataFrame(
            [("🐈", "SSparkSQLS", "SL", "🐈", "prefix🐈".encode("utf-8"), bytes([240]), "🐈".encode("utf-8"), "🐈")],
            generated_schemas.STRING_FUNCTION_INPUT_SCHEMA,
        )
        online = StringFunctions(rows=source).run(session(spark, execution_mode="online"))
        generated = StringFunctions(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.result, "label")
        from pyspark.sql import functions as F

        native = source.select(F.btrim("trim_source", "trim_chars").alias("custom_trim"))
        generated_trim = generated.result.select("custom_trim")
        assert rows(native) == rows(generated_trim)

    assert actual == [
        {
            "label": "🐈",
            "custom_trim": "parkSQ",
            "text_bits": 32,
            "payload_bits": 80,
            "has_prefix": True,
            "has_byte_prefix": False,
            "has_byte_suffix": True,
            "has_mixed_prefix": True,
            "has_mixed_suffix": True,
        }
    ]
