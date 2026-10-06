from decimal import Decimal
from typing import cast

import pytest

from structure import Schema, Transform, input, output, step, variable
from structure.core.compiler.api import Compiler
from structure.core.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform
from structure.core.compiler.artifacts.model import CompilerOptions
from structure.plugin.pyspark import long, string, where
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.RuntimeVariables import (
    bind_runtime_variables,
    invocation_variables,
    reset_runtime_variables,
)
from structure.plugin.pyspark.dsl.types import LongType
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class Row(Schema):
    id = long(nullable=False)
    name = string()


class VariableFilter(Transform):
    rows = input(Row)
    result = output(Row)
    threshold = variable(int)
    prefix = variable(str, default="A")

    @step(input=rows, output=result)
    def select(self, row: Row) -> Row:
        where((row.id > self.threshold) & row.name.startswith(self.prefix))
        return row


class VariableChild(Transform):
    rows = input(Row)
    threshold = variable(int)
    result = output(Row)

    @step(input=rows, output=result)
    def select(self, row: Row) -> Row:
        where(row.id > self.threshold)
        return row


class VariableWrapper(Transform):
    rows = input(Row)
    threshold = variable(int)
    selected = VariableChild(rows=rows, threshold=threshold)
    result = output(Row, selected.result)


def _plan(subject) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(subject, materialize_schemas=False).lowered)


def test_variable_is_required_typed_and_immutable() -> None:
    with pytest.raises(TypeError, match="requires int"):
        VariableFilter(threshold=True)
    _plan(VariableFilter)
    with pytest.raises(TypeError, match="is required"):
        invocation_variables(VariableFilter())
    invocation = VariableFilter(threshold=2)
    with pytest.raises(AttributeError, match="immutable"):
        invocation.threshold = 3


def test_runtime_bindings_do_not_specialize_the_compiled_artifact() -> None:
    builder = BuildCompiledTransform()
    options = CompilerOptions.resolve()
    first = builder(VariableFilter(threshold=3), options=options, materialize_schemas=False)
    second = builder(VariableFilter(threshold=9), options=options, materialize_schemas=False)
    assert first.key == second.key
    assert first.semantic_fingerprint == second.semantic_fingerprint


def test_variable_expression_is_symbolic_and_rendered_as_runtime_lookup() -> None:
    plan = _plan(VariableFilter(threshold=3))
    predicate = plan.steps[0].filters[0]
    rendered = render_pyspark_expression(predicate, scope_aliases={"row": "r"})
    assert "self._structure_variables['threshold']" in rendered
    assert "self._structure_variables['prefix']" in rendered
    assert "3" not in rendered
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.VariableFilter",
        schema_modules={Row: __name__},
        runtime_module="structure.plugin.pyspark.runtime",
    )
    compile(source, "<generated-variable-transform>", "exec")
    assert "self._structure_variables['threshold']" in source
    assert "self._structure_variables['prefix']" in source
    assert "3" not in source


def test_optional_and_decimal_variable_validation() -> None:
    optional = variable(int | None, default=None)
    assert optional.validate(None) is None
    with pytest.raises(TypeError, match="requires int"):
        optional.validate(False)
    decimal = variable(Decimal, precision=5, scale=2)
    assert decimal.validate(Decimal("12.34")) == Decimal("12.34")
    with pytest.raises(TypeError, match="does not fit"):
        decimal.validate(Decimal("1234.56"))


def test_online_expression_evaluation_reads_per_invocation_value(monkeypatch) -> None:
    import sys
    from types import ModuleType

    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    spark_types = ModuleType("pyspark.sql.types")
    setattr(spark_types, "LongType", LongType)
    setattr(sql, "types", spark_types)
    setattr(pyspark, "sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", spark_types)

    class Column:
        def __init__(self, value):
            self.value = value

        def cast(self, target):
            return (self.value, type(target).__name__)

    class Functions:
        @staticmethod
        def lit(value):
            return Column(value)

    expression = PySparkExpressionRecipe(
        kind="variable",
        type=LongType(),
        nullable=False,
        data={"name": "threshold"},
    )
    token = bind_runtime_variables({"threshold": 17})
    try:
        assert EvaluatePySparkExpression().evaluate(expression, functions=Functions(), aliases={}) == (17, "LongType")
    finally:
        reset_runtime_variables(token)


def test_runtime_variable_is_forwarded_through_composed_stage() -> None:
    plan = _plan(VariableWrapper)
    predicate = plan.steps[0].filters[0]
    assert "self._structure_variables['threshold']" in render_pyspark_expression(
        predicate, scope_aliases={"row": "r"}
    )
    assert invocation_variables(VariableWrapper(threshold=11)) == {"threshold": 11}
