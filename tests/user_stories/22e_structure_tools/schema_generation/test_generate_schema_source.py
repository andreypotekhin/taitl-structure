from helpers.fake_pyspark_schema import (  # type: ignore[import-not-found]
    ArrayType,
    DecimalType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from structure import *


def test_generate_structure_schema_from_pyspark_struct_type() -> None:
    schema = StructType(
        (
            StructField("id", StringType(), False),
            StructField("total", DecimalType(12, 2), True),
            StructField("tags", ArrayType(StringType(), containsNull=False), True),
            StructField("attributes", MapType(StringType(), StringType(), valueContainsNull=True), True),
            StructField(
                "shipping",
                StructType(
                    (
                        StructField("street", StringType(), True),
                        StructField("postal_code", StringType(), True),
                    )
                ),
                True,
            ),
        )
    )

    text = StructureTools.schemas.generate(schema=schema, to="OrderRaw")

    assert "from structure import Schema" in text
    assert "from structure.platform.pyspark.dsl.field import *" in text
    assert "class OrderRawShipping(Schema):" in text
    assert "    street = string()" in text
    assert "class OrderRaw(Schema):" in text
    assert "    id = string(nullable=False)" in text
    assert "    total = decimal(12, 2)" in text
    assert "    tags = array(string(), contains_null=False)" in text
    assert "    attributes = map(string(), string(), value_contains_null=True)" in text
    assert "    shipping = struct(OrderRawShipping)" in text
    namespace: dict[str, object] = {}
    exec(text, namespace)
    assert "OrderRaw" in namespace


def test_generate_structure_schema_from_dataframe_like_schema() -> None:
    class DataFrame:
        schema = StructType((StructField("id", StringType(), False),))

    text = StructureTools.schemas.generate(schema=DataFrame(), to="OrderRaw")

    assert "class OrderRaw(Schema):" in text
    assert "    id = string(nullable=False)" in text


def test_generate_structure_schema_uses_aliases_for_non_identifier_spark_fields() -> None:
    schema = StructType(
        (
            StructField("promo-code", StringType(), True),
            StructField("customer id", StringType(), True),
            StructField("class", StringType(), True),
            StructField("1st code", StringType(), True),
        )
    )

    text = StructureTools.schemas.generate(schema=schema, to="OrderRaw")

    assert '    promo_code = string(alias="promo-code")' in text
    assert '    customer_id = string(alias="customer id")' in text
    assert '    class_ = string(alias="class")' in text
    assert '    field_1st_code = string(alias="1st code")' in text
