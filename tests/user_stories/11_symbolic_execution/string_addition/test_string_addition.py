from decimal import Decimal
from operator import add, mul, sub, truediv
from typing import cast

import pytest
from helpers.string_addition import StringAddition

from structure import Schema, StructureCompileError, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import array, integer, literal, string, types
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.render.commands.RenderPySparkExpression import render_pyspark_expression
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class TestStringAddition:
    """I can concatenate typed strings with + while retaining numeric addition and Spark null semantics."""

    @pytest.mark.parametrize("reverse", [False, True])
    def test_literals(self, reverse) -> None:
        value = literal("12")
        expression = "3" + value if reverse else value + "3"
        expected = "F.concat(F.lit('3'), F.lit('12'))" if reverse else "F.concat(F.lit('12'), F.lit('3'))"
        assert render_pyspark_expression(cast(PySparkExpressionRecipe, expression)) == expected
        assert expression.type is not None
        assert expression.type.name == "string"
        assert expression.nullable is False

    def test_transform(self) -> None:
        compilation = Compiler.frontend.compile()(StringAddition, materialize_schemas=False)
        body = cast(PySparkStepBody, cast(TransformPlan, compilation.analysis).steps[0].plugin_body)
        projection = {item.field.name: item.expression for item in body.projection}
        for name in ("joined", "prefixed", "suffixed", "chained", "null_string"):
            assert projection[name].type.name == "string"
            assert projection[name].nullable is True
        for name in ("fallback", "converted"):
            assert projection[name].type.name == "string"
            assert projection[name].nullable is False
        assert projection["incremented"].kind == "add"
        assert render_pyspark_expression(projection["prefixed"]) == 'F.concat(F.lit(\'order:\'), F.col("rows.first"))'
        assert render_pyspark_expression(projection["chained"]) == (
            'F.concat(F.concat(F.col("rows.first"), F.lit(\' / \')), F.col("rows.last"))'
        )

    @pytest.mark.parametrize("other", [1, 1.5, Decimal("1.2"), True, None, b"a", array("a")])
    @pytest.mark.parametrize("reverse", [False, True])
    def test_mixed_types(self, other, reverse) -> None:
        value = literal("a")
        with pytest.raises(TypeError, match="String addition requires two String operands"):
            add(other, value) if reverse else add(value, other)

    @pytest.mark.parametrize("operation", [sub, mul, truediv])
    def test_other_operators(self, operation) -> None:
        with pytest.raises(TypeError, match="Arithmetic requires numeric"):
            operation(literal("a"), literal("b"))

    @pytest.mark.parametrize("value", [array("a"), literal(b"a")])
    def test_other_sequences(self, value) -> None:
        with pytest.raises(TypeError, match="Arithmetic requires numeric"):
            value + value

    def test_numeric_widening(self) -> None:
        expression = 1 + literal(2.5)
        assert expression.type is not None
        assert expression.type.name == "double"
        assert expression.nullable is False
        assert render_pyspark_expression(cast(PySparkExpressionRecipe, expression)) == "(F.lit(1) + F.lit(2.5))"

    def test_diagnostic(self) -> None:
        class Source(Schema):
            count = integer(nullable=False)

        class Target(Schema):
            label = string(nullable=False)

        class Invalid(Transform):
            rows = input(Source)
            result = output(Target)

            def project(self, row: Source) -> Target:
                return Target(label="n=" + row.count)

        with pytest.raises(StructureCompileError) as raised:
            Invalid.compile()
        diagnostic = raised.value.diagnostic
        assert diagnostic.code == "DSL-E0401"
        assert diagnostic.primary_span is not None
        assert "String addition requires two String operands" in diagnostic.problem_text()
        assert ".cast(types.string())" in diagnostic.problem_text()
        assert "Troubleshooting.md#string-addition-rejects-mixed-types" in diagnostic.problem_text()

    def test_nullable_assignment(self) -> None:
        class Source(Schema):
            text = string(nullable=True)

        class Target(Schema):
            text = string(nullable=False)

        class Invalid(Transform):
            rows = input(Source)
            result = output(Target)

            def project(self, row: Source) -> Target:
                return Target(text=row.text + "!")

        with pytest.raises(StructureCompileError) as raised:
            Invalid.compile()
        assert raised.value.diagnostic.code == "SCHEMA-E0301"
        assert "nullable" in raised.value.diagnostic.problem_text()

    def test_typed_null(self) -> None:
        expression = literal(None).cast(types.string()) + "suffix"
        assert expression.type is not None
        assert expression.type.name == "string"
        assert expression.nullable is True
