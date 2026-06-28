from helpers.fake_pyspark_schema import (  # type: ignore[import-not-found]
    ArrayType,
    DecimalType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from structure import StructureTools


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

    assert "class OrderRawShipping(Structure):" in text
    assert "    street = field(String(), nullable=True)" in text
    assert "class OrderRaw(Structure):" in text
    assert "    id = field(String(), nullable=False)" in text
    assert "    total = field(Decimal(12, 2), nullable=True)" in text
    assert "    tags = field(Array(String(), contains_null=False), nullable=True)" in text
    assert "    attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)" in text
    assert "    shipping = field(Struct(OrderRawShipping), nullable=True)" in text


def test_generate_structure_schema_from_dataframe_like_schema() -> None:
    class DataFrame:
        schema = StructType((StructField("id", StringType(), False),))

    text = StructureTools.schemas.generate(schema=DataFrame(), to="OrderRaw")

    assert "class OrderRaw(Structure):" in text
    assert "    id = field(String(), nullable=False)" in text


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

    assert '    promo_code = field(String(), nullable=True, alias="promo-code")' in text
    assert '    customer_id = field(String(), nullable=True, alias="customer id")' in text
    assert '    class_ = field(String(), nullable=True, alias="class")' in text
    assert '    field_1st_code = field(String(), nullable=True, alias="1st code")' in text
