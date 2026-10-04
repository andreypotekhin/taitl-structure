import pytest

from structure import Schema, Transform, input, output, step, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import PySpark, SqlCommandResult, SqlResult, sql, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


def _recipe(transform_type: type[Transform]) -> PySparkExecutionPlan:
    return Compiler.frontend.compile()(transform_type, materialize_schemas=False).lowered  # type: ignore[return-value]


def test_sql_captures_typed_query_and_relation_binding() -> None:
    class Order(Schema):
        id = string(nullable=False)

    class Result(Schema):
        id = string(nullable=False)

    @transform
    class SelectOrders(Transform):
        orders = input(Order)
        selected = output(Result)

        def select(self, order: Order) -> Result:
            result = sql("SELECT id FROM {orders}", relations={"orders": order}, to=Result)
            return Result(id=result.id)

    recipe = _recipe(SelectOrders)
    step = recipe.steps[0]
    operation = step.operations[0]

    assert operation.kind == "sql"
    assert operation.sql is not None
    assert operation.sql.statement == "SELECT id FROM {orders}"
    assert operation.sql.relations == (("orders", "orders"),)
    assert operation.sql.schema is Result
    assert step.projection[0].expression.data["scope"] == operation.sql.scope
    assert "execute_pyspark_sql(" in PySpark.render.step()(
        step,
        current="current",
        sources={"orders": "orders"},
    )


def test_sql_captures_labeled_command_result_as_a_schema() -> None:
    class Order(Schema):
        id = string(nullable=False)

    @transform
    class DeleteOrders(Transform):
        orders = input(Order)
        commands = output(SqlCommandResult)

        def delete(self, order: Order) -> SqlCommandResult:
            return sql(
                "DELETE FROM {target} WHERE id = :id",
                relations={"target": "catalog.sales.orders"},
                args={"id": "A-1"},
                label="delete-order",
                to=SqlCommandResult,
            )

    assert issubclass(SqlCommandResult, SqlResult)
    assert tuple(SqlCommandResult._structure_fields) == (
        "label",
        "num_affected_rows",
        "num_updated_rows",
        "num_inserted_rows",
        "num_deleted_rows",
    )
    recipe = _recipe(DeleteOrders)
    sql_operation = recipe.steps[0].operations[0].sql
    assert sql_operation is not None
    assert sql_operation.statement == "DELETE FROM catalog.sales.orders WHERE id = :id"
    assert sql_operation.label == "delete-order"
    assert sql_operation.args is not None
    rendered = PySpark.render.step()(
        recipe.steps[0],
        current="current",
        sources={},
        frame_mapping="frames",
    )
    assert "normalize_sql_command_result" in rendered
    assert "num_affected_rows" in rendered


def test_sql_backend_errors_keep_their_type_and_gain_transform_context() -> None:
    from structure.plugin.pyspark.execution.logic.running.ExecutePySparkSql import execute_pyspark_sql

    class AnalysisFailure(Exception):
        pass

    class Spark:
        def sql(self, statement, **kwargs):
            raise AnalysisFailure(statement)

    with pytest.raises(AnalysisFailure, match="DELETE FROM target") as error:
        execute_pyspark_sql(
            Spark(),
            "DELETE FROM target",
            args=None,
            relations={},
            step="delete_orders",
            label="delete-orders",
        )
    assert any("delete_orders" in note and "delete-orders" in note for note in error.value.__notes__)


def test_repeated_command_outputs_accumulate_in_generated_steps() -> None:
    class Source(Schema):
        id = string(nullable=False)

    @transform
    class DeleteTwo(Transform):
        source = input(Source)
        commands = output(SqlCommandResult)

        @step(input=source, output=commands)
        def first(self, row: Source) -> SqlCommandResult:
            return sql("DELETE FROM target", label="first", to=SqlCommandResult)

        @step(input=source, output=commands)
        def second(self, row: Source) -> SqlCommandResult:
            return sql("DELETE FROM target", label="second", to=SqlCommandResult)

    recipe = _recipe(DeleteTwo)
    assert [step.results[0].frame for step in recipe.steps] == ["commands", "commands"]
    rendered = PySpark.render.step()(
        recipe.steps[0],
        current="current",
        sources={},
        frame_mapping="frames",
    )
    assert "__structure_sql_command_result_frames__" in rendered
    assert "unionByName(commands, allowMissingColumns=True)" in rendered
    module = PySpark.render.transform()(
        recipe,
        source_transform="__main__.DeleteTwo",
        schema_modules={},
        runtime_module="tests.typed_sql.runtime",
    )
    compile(module, "<generated typed SQL transform>", "exec")
    assert "unionByName(commands, allowMissingColumns=True)" in module


def test_command_result_subclass_can_flow_to_base_output_schema() -> None:
    class Source(Schema):
        id = string(nullable=False)

    class ProviderCommandResult(SqlCommandResult):
        provider_code = string(nullable=True)

    @transform
    class RunProviderCommand(Transform):
        source = input(Source)
        commands = output(SqlCommandResult)

        @step(input=source, output=commands)
        def run_command(self, row: Source) -> ProviderCommandResult:
            return sql("DELETE FROM target", label="provider-delete", to=ProviderCommandResult)

    recipe = _recipe(RunProviderCommand)
    assert recipe.outputs[0].input_schema is ProviderCommandResult
    assert recipe.outputs[0].output_schema is SqlCommandResult
    output_code = PySpark.render.step()(
        recipe.outputs[0],
        current="commands",
        sources={"commands": "commands"},
    )
    assert "provider_code" not in output_code
    assert "num_affected_rows" in output_code


def test_sql_requires_concrete_typed_results_and_allows_omitted_command_labels() -> None:
    with pytest.raises(TypeError, match="SqlResult.*abstract"):
        sql("SELECT 1", to=SqlResult)

    class Source(Schema):
        id = string(nullable=False)

    @transform
    class DeleteOrders(Transform):
        source = input(Source)
        commands = output(SqlCommandResult)

        def delete(self, row: Source) -> SqlCommandResult:
            return sql("DELETE FROM orders", to=SqlCommandResult)

    assert SqlCommandResult._structure_fields["label"].nullable is True
    recipe = _recipe(DeleteOrders)
    operation = recipe.steps[0].operations[0]
    assert operation.sql is not None
    assert operation.sql.label is None
    rendered = PySpark.render.step()(
        recipe.steps[0],
        current="current",
        sources={"source": "source"},
    )
    assert "normalize_sql_command_result" in rendered
    assert "label=None" in rendered
