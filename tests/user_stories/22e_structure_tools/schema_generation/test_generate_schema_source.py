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

    assert "import structure" in text
    assert "class OrderRawShipping(structure.Structure):" in text
    assert "    street = structure.field(structure.String(), nullable=True)" in text
    assert "class OrderRaw(structure.Structure):" in text
    assert "    id = structure.field(structure.String(), nullable=False)" in text
    assert "    total = structure.field(structure.Decimal(12, 2), nullable=True)" in text
    assert "    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)" in text
    assert (
        "    attributes = structure.field("
        "structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True)"
        in text
    )
    assert "    shipping = structure.field(structure.Struct(OrderRawShipping), nullable=True)" in text


def test_generate_structure_schema_from_dataframe_like_schema() -> None:
    class DataFrame:
        schema = StructType((StructField("id", StringType(), False),))

    text = StructureTools.schemas.generate(schema=DataFrame(), to="OrderRaw")

    assert "class OrderRaw(structure.Structure):" in text
    assert "    id = structure.field(structure.String(), nullable=False)" in text


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

    assert '    promo_code = structure.field(structure.String(), nullable=True, alias="promo-code")' in text
    assert '    customer_id = structure.field(structure.String(), nullable=True, alias="customer id")' in text
    assert '    class_ = structure.field(structure.String(), nullable=True, alias="class")' in text
    assert '    field_1st_code = structure.field(structure.String(), nullable=True, alias="1st code")' in text
