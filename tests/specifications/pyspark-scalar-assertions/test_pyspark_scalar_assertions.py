from dataclasses import dataclass

from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.expressions import assert_true, raise_error, try_url_decode, url_decode, url_encode
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


def test_online_expression_evaluator_dispatches_url_helpers() -> None:
    functions = FakeFunctions()
    evaluator = EvaluatePySparkExpression()
    mapper = MapPySparkExpression()
    capabilities = PySparkCapabilities()

    encoded = evaluator.evaluate(
        mapper.map(url_encode("a b"), capabilities=capabilities), functions=functions, aliases={}
    )
    decoded = evaluator.evaluate(
        mapper.map(url_decode("a+b"), capabilities=capabilities), functions=functions, aliases={}
    )
    safe_decoded = evaluator.evaluate(
        mapper.map(
            try_url_decode("a+b"), capabilities=PySparkCapabilities(target_profile=">=4.0,<4.1")
        ),
        functions=functions,
        aliases={},
    )

    assert encoded.expression == "url_encode(lit('a b'))"
    assert decoded.expression == "url_decode(lit('a+b'))"
    assert safe_decoded.expression == "try_url_decode(lit('a+b'))"


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

    def url_encode(self, value: FakeColumn) -> FakeColumn:
        return FakeColumn(f"url_encode({value.expression})")

    def url_decode(self, value: FakeColumn) -> FakeColumn:
        return FakeColumn(f"url_decode({value.expression})")

    def try_url_decode(self, value: FakeColumn) -> FakeColumn:
        return FakeColumn(f"try_url_decode({value.expression})")
