from datetime import date as date_value

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import BackendCapabilityError
from structure.plugin.pyspark import *
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.OptimizePySparkProjectionUnions import OptimizePySparkProjectionUnions
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import DateType, DoubleType, IntegerType, LongType, StringType, TimestampType
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression


def _field(type, nullable: bool) -> Expression:
    return Expression(kind="field", type=type, nullable=nullable, data={"field": "value", "scope": "rows"})


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("type", [IntegerType(), LongType()])
def test_chr_reuses_character_type_rules(type, nullable: bool) -> None:
    value = _field(type, nullable)
    result = chr(value)
    assert result.type == StringType()
    assert result.nullable == nullable
    assert result.args == char(value).args


@pytest.mark.parametrize("nullable", [False, True])
def test_quote_keeps_native_nullable_schema_even_for_required_input(nullable: bool) -> None:
    result = quote(_field(StringType(), nullable))
    assert result.type == StringType()
    assert result.nullable is True


@pytest.mark.parametrize("type", [StringType(), DateType(), TimestampType()])
def test_try_to_date_is_nullable_even_for_required_input(type) -> None:
    result = try_to_date(_field(type, False), format="yyyy-MM-dd")
    assert result.type == DateType()
    assert result.nullable is True
    assert (result.data or {}).get("warnings", ()) == (() if type == StringType() else ("PYSPARK-W2705",))


@pytest.mark.parametrize("helper,value", [(chr, True), (chr, 1.5), (chr, "65"), (quote, 1), (try_to_date, 1)])
def test_scalar_helpers_reject_implicit_coercion(helper, value) -> None:
    with pytest.raises(TypeError, match="requires"):
        helper(value)


@pytest.mark.parametrize("format", ["", 7, _field(StringType(), False)])
def test_try_to_date_requires_a_literal_pattern(format) -> None:
    with pytest.raises(TypeError, match="non-empty string literal"):
        try_to_date("2024-02-29", format=format)


@pytest.mark.parametrize("helper,type", [(random, DoubleType()), (uuid, StringType())])
def test_random_helpers_share_the_explicit_seed_policy(helper, type) -> None:
    with pytest.raises(TypeError, match="seed is required"):
        helper()
    for seed in (True, "42", _field(LongType(), False)):
        with pytest.raises(TypeError, match="integer literal"):
            helper(seed=seed)
    with pytest.raises(TypeError, match="Boolean"):
        helper(seed=42, reproducible=1)
    for result in (helper(seed=-42), helper(reproducible=False)):
        assert result.type == type
        assert result.nullable is False
        assert (result.data or {})["nondeterministic"] is True


HELPERS = [
    (chr(65), "chr"),
    (quote("Don't"), "quote"),
    (try_to_date("2024-02-29"), "try_to_date"),
    (random(seed=42), "random"),
    (uuid(seed=42), "uuid"),
]


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize("expression,name", HELPERS)
def test_helpers_render_native_spelling_and_keep_nondeterminism(expression, name: str, variant: str) -> None:
    recipe = MapPySparkExpression().map(
        expression, capabilities=PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant=variant)
    )
    source = RenderPySparkExpression()(recipe)
    assert source.startswith(f"F.{name}(")
    assert not any(token in source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))
    if name in {"random", "uuid"}:
        assert source == f"F.{name}(seed=42)"
        assert not OptimizePySparkProjectionUnions()._deterministic_expression(recipe)


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize("profile", [">=3.5,<4.1", ">=3.5,<4.0", ">=4.0,<4.1", ">=4.2,<4.3"])
@pytest.mark.parametrize("expression,name", HELPERS)
def test_helpers_reject_unproven_profiles(expression, name: str, profile: str, variant: str) -> None:
    with pytest.raises(BackendCapabilityError, match=f"expression.{name}") as error:
        MapPySparkExpression().map(
            expression, capabilities=PySparkCapabilities(target_profile=profile, target_variant=variant)
        )
    assert f"{name}(...) requires PySpark >=4.1,<4.2" in error.value.decision.why
    assert "Expressions.compat.md" in error.value.decision.use


def test_try_to_date_renders_literal_format_as_a_python_string() -> None:
    recipe = MapPySparkExpression().map(
        try_to_date("29/02/2024", format="dd/MM/yyyy"), capabilities=PySparkCapabilities(target_profile=">=4.1,<4.2")
    )
    assert RenderPySparkExpression()(recipe) == "F.try_to_date(F.lit('29/02/2024'), 'dd/MM/yyyy')"
    assert try_to_date(date_value(2024, 2, 29)).type == DateType()


class Raw(Schema):
    value = string(nullable=False)


@pytest.mark.parametrize("helper,type", [(try_to_date, date), (quote, string)])
def test_nullable_helper_cannot_be_narrowed_by_a_non_null_source(helper, type) -> None:
    class Parsed(Schema):
        value = type(nullable=False)

    @transform
    class Parse(Transform):
        rows = input(Raw)
        parsed = output(Parsed)

        def parse(self, row: Raw) -> Parsed:
            where(row.value.is_not_null())
            return Parsed(value=helper(row.value))

    with pytest.raises(StructureCompileError, match="nullable"):
        Compiler.frontend.compile()(Parse, materialize_schemas=False, plugin={"pyspark": {"profile": ">=4.1,<4.2"}})


@pytest.mark.parametrize("helper,type", [(random, double), (uuid, string)])
@pytest.mark.parametrize("placement", ["projection", "filter", "special", "callback"])
def test_random_helpers_are_batch_only_in_nested_recipes(helper, type, placement: str) -> None:
    class Output(Schema):
        value = type(nullable=False)

    @transform(streaming=True)
    class Generate(Transform):
        rows = input(Raw, streaming=True)
        generated = output(Output)

        @special(type="expr")
        def draw(value):
            return helper(seed=42)

        def publish(self, row: Raw) -> Output:
            if placement == "filter":
                where(helper(seed=42).is_not_null())
                value = 0.0 if helper is random else "id"
            elif placement == "special":
                value = self.draw(row.value)
            elif placement == "callback":
                value = row.value.transform(lambda _: helper(seed=42))
            else:
                value = helper(seed=42)
            return Output(value=value)

    compilation = Compiler.frontend.compile()(
        Generate, materialize_schemas=False, plugin={"pyspark": {"profile": ">=4.1,<4.2"}}
    )
    report = Compiler.compileability.streaming()(compilation.lowered, required=True)
    assert any(finding.operation == helper.__name__ and finding.code == "STREAM-E0801" for finding in report.findings)
