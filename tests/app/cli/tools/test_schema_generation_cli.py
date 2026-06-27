import sys

from click.testing import CliRunner
from helpers.fake_pyspark_schema import StringType, StructField, StructType  # type: ignore[import-not-found]

from structure.app.cli.api import cli


def test_schema_generation_cli_prints_generated_source(mocker) -> None:
    spark = FakeSpark(StructType((StructField("id", StringType(), False),)))
    module = "structure.app.cli.api.cli"
    mocker.patch(f"{module}._spark_session", return_value=spark)

    result = CliRunner().invoke(
        cli,
        ["tools", "schemas", "generate", "--from-path", "orders.parquet", "--format", "parquet", "--to", "OrderRaw"],
    )

    assert result.exit_code == 0, result.output
    assert "class OrderRaw(Structure):" in result.output
    assert "    id = field(String(), nullable=False)" in result.output
    assert spark.read.format_value == "parquet"
    assert spark.read.path == "orders.parquet"


def test_schema_generation_cli_validates_without_importing_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    result = CliRunner().invoke(cli, ["tools", "schemas", "generate", "--to", "OrderRaw"])

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert result.exit_code == 1
    assert "exactly one schema source" in result.output
    assert after == before


def test_schema_generation_cli_parses_reader_options(mocker) -> None:
    spark = FakeSpark(StructType((StructField("id", StringType(), False),)))
    mocker.patch("structure.app.cli.api.cli._spark_session", return_value=spark)

    result = CliRunner().invoke(
        cli,
        [
            "tools",
            "schemas",
            "generate",
            "--from-path",
            "orders.parquet",
            "--format",
            "parquet",
            "--option",
            "mergeSchema=true",
            "--to",
            "OrderRaw",
        ],
    )

    assert result.exit_code == 0, result.output
    assert spark.read.options_value == {"mergeSchema": "true"}


def test_schema_generation_cli_rejects_invalid_reader_option() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "tools",
            "schemas",
            "generate",
            "--from-path",
            "orders.parquet",
            "--format",
            "parquet",
            "--option",
            "broken",
            "--to",
            "OrderRaw",
        ],
    )

    assert result.exit_code == 1
    assert "Use KEY=VALUE" in result.output


def test_schema_generation_cli_validates_format_before_starting_spark(mocker) -> None:
    spark_session = mocker.patch("structure.app.cli.api.cli._spark_session")

    result = CliRunner().invoke(
        cli,
        [
            "tools",
            "schemas",
            "generate",
            "--from-path",
            "orders.parquet",
            "--to",
            "OrderRaw",
        ],
    )

    assert result.exit_code == 1
    assert "requires format" in result.output
    spark_session.assert_not_called()


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
