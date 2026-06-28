import pytest
from helpers.fake_pyspark_schema import StringType, StructField, StructType  # type: ignore[import-not-found]

from structure import StructureTools
from structure.app.runtime.session.model.StructureSession import StructureSession
from structure.app.tools.model import StructureToolError


def test_generate_requires_exactly_one_source() -> None:
    with pytest.raises(StructureToolError, match="exactly one schema source"):
        StructureTools.schemas.generate(to="OrderRaw")

    with pytest.raises(StructureToolError, match="exactly one schema source"):
        StructureTools.schemas.generate(schema=StructType(()), from_table="orders", to="OrderRaw")


def test_live_sources_accept_spark_or_structure_session() -> None:
    spark = FakeSpark(StructType((StructField("id", StringType(), False),)))
    session = StructureSession(spark=spark)

    by_spark = StructureTools.schemas.generate(from_table="orders", spark=spark, to="OrderRaw")
    by_session = StructureTools.schemas.generate(from_table="orders", session=session, to="OrderRaw")

    assert "id = field(String(), nullable=False)" in by_spark
    assert by_session == by_spark


def test_live_sources_reject_ambiguous_or_missing_spark_source() -> None:
    spark = FakeSpark(StructType(()))

    with pytest.raises(StructureToolError, match="spark=.*session"):
        StructureTools.schemas.generate(
            from_table="orders", spark=spark, session=StructureSession(spark=spark), to="OrderRaw"
        )

    with pytest.raises(StructureToolError, match="Spark metadata access"):
        StructureTools.schemas.generate(from_table="orders", to="OrderRaw")


def test_path_sources_require_format_and_support_reader_options() -> None:
    schema = StructType((StructField("id", StringType(), False),))
    spark = FakeSpark(schema)

    text = StructureTools.schemas.generate(
        from_path="orders.parquet",
        format="parquet",
        spark=spark,
        options={"mergeSchema": "true"},
        to="OrderRaw",
    )

    assert "id = field(String(), nullable=False)" in text
    assert spark.read.options_value == {"mergeSchema": "true"}
    assert spark.read.format_value == "parquet"
    assert spark.read.path == "orders.parquet"


def test_invalid_class_names_fail_and_non_identifier_fields_generate_aliases() -> None:
    with pytest.raises(StructureToolError, match="Invalid Structure class name"):
        StructureTools.schemas.generate(schema=StructType(()), to="order_raw")

    text = StructureTools.schemas.generate(
        schema=StructType((StructField("order-id", StringType(), False),)),
        to="OrderRaw",
    )

    assert 'order_id = field(String(), nullable=False, alias="order-id")' in text


class FakeSpark:

    def __init__(self, schema) -> None:
        self.schema = schema
        self.read = FakeReader(schema)

    def table(self, table: str):
        self.table_value = table
        return FakeDataFrame(self.schema)


class FakeReader:

    def __init__(self, schema) -> None:
        self.schema = schema
        self.options_value: dict[str, str] = {}
        self.format_value: str | None = None
        self.path: str | None = None

    def options(self, **kwargs):
        self.options_value = kwargs
        return self

    def format(self, format: str):
        self.format_value = format
        return self

    def load(self, path: str):
        self.path = path
        return FakeDataFrame(self.schema)


class FakeDataFrame:

    def __init__(self, schema) -> None:
        self.schema = schema
