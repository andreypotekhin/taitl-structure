from dataclasses import dataclass

from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.expressions import assert_true, raise_error
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression


def test_online_expression_evaluator_uses_native_scalar_assertions_as_boolean_guards() -> None:
    functions = FakeFunctions()
    evaluator = EvaluatePySparkExpression()
    mapper = MapPySparkExpression()
    capabilities = PySparkCapabilities()

    asserted = evaluator.evaluate(
        mapper.map(assert_true(True, message="expected true"), capabilities=capabilities),
        functions=functions,
        aliases={},
    )
    defaulted = evaluator.evaluate(
        mapper.map(assert_true(True), capabilities=capabilities),
        functions=functions,
        aliases={},
    )
    failed = evaluator.evaluate(
        mapper.map(raise_error("stop"), capabilities=capabilities),
        functions=functions,
        aliases={},
    )

    assert asserted.expression == "assert_true(lit(True),'expected true').isNull()"
    assert defaulted.expression == "assert_true(lit(True)).isNull()"
    assert failed.expression == "raise_error('stop').isNull()"


@dataclass(frozen=True)
class FakeColumn:
    expression: str

    def isNull(self) -> "FakeColumn":
        return FakeColumn(f"{self.expression}.isNull()")


class FakeFunctions:
    def lit(self, value: object) -> FakeColumn:
        return FakeColumn(f"lit({value!r})")

    def assert_true(self, condition: FakeColumn, message: str | None = None) -> FakeColumn:
        arguments = condition.expression if message is None else f"{condition.expression},{message!r}"
        return FakeColumn(f"assert_true({arguments})")

    def raise_error(self, message: str) -> FakeColumn:
        return FakeColumn(f"raise_error({message!r})")
