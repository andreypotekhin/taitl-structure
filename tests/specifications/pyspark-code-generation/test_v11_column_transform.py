from typing import cast

from structure import Schema, Transform, input, output, special, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import string, trim, upper
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import string as string_type
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression


class RawName(Schema):
    name = string(nullable=False)


class NormalizedName(Schema):
    name = string(nullable=False)


@transform
class NormalizeName(Transform):
    rows = input(RawName)
    normalized = output(NormalizedName)

    @special(type="expr")
    def normalize_text(value):
        return upper(trim(value))

    def normalize(self, row: RawName) -> NormalizedName:
        return NormalizedName(name=row.name.transform(self.normalize_text))


def test_column_transform_renders_as_a_typed_pyspark_column_callback() -> None:
    value = Expression(kind="field", type=string_type(), nullable=False, data={"field": "name", "scope": "orders"})
    expression = value.transform(lambda column: upper(trim(column)))
    recipe = MapPySparkExpression().map(
        expression,
        capabilities=PySparkCapabilities(target_profile=">=4.1,<4.2"),
    )

    assert RenderPySparkExpression()(recipe, scope_aliases={"orders": "orders"}) == (
        'F.col("orders.name").transform(lambda _column: F.upper(F.trim(F.col("orders.name"))))'
    )


def test_special_expression_helper_can_be_passed_directly_to_column_transform() -> None:
    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(
            NormalizeName,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
        ).lowered,
    )

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert '.transform(lambda _column: F.upper(F.trim(F.col("raw_name.name"))))' in text
