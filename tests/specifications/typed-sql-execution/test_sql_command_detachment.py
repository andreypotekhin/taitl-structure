"""Command-result materialization prevents repeated effectful SQL evaluation."""

from structure.plugin.pyspark.execution.logic.running.ExecutePySparkSql import execute_pyspark_sql


def test_command_result_executes_once_and_returns_a_detached_relation() -> None:
    class Schema:
        fields = ("value",)

    rows = [object()]
    schema = Schema()

    class EffectResult:
        def __init__(self):
            self.collect_calls = 0

        @property
        def schema(self):
            return schema

        def collect(self):
            self.collect_calls += 1
            return rows

    class Spark:
        def __init__(self):
            self.result = EffectResult()
            self.create_calls = []

        def sql(self, statement, **kwargs):
            return self.result

        def createDataFrame(self, data, *, schema):
            self.create_calls.append((data, schema))
            return "detached"

    spark = Spark()
    result = execute_pyspark_sql(
        spark, "INSERT INTO target VALUES (1)", args=None, relations={},
        step="append", label=None, command_result=True,
    )

    assert result == "detached"
    assert spark.result.collect_calls == 1
    assert spark.create_calls == [(rows, schema)]


def test_empty_native_command_result_becomes_a_zero_row_detached_relation() -> None:
    class EmptyResult:
        schema = type("Schema", (), {"fields": ()})()

        def collect(self):
            return []

    class Range:
        def selectExpr(self, expression):
            return ("empty", expression)

    class Spark:
        def sql(self, statement, **kwargs):
            return EmptyResult()

        def range(self, size):
            assert size == 0
            return Range()

    assert execute_pyspark_sql(
        Spark(), "ALTER TABLE target ADD COLUMN value INT", args=None, relations={},
        step="alter", label=None, command_result=True,
    ) == ("empty", "CAST(NULL AS INT) AS __structure_empty_command_result")
