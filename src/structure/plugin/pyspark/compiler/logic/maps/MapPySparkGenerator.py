from __future__ import annotations

from structure.plugin.api.v1.model import BackendCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.dsl.operations.PosexplodeStructPlan import PosexplodeStructPlan


class MapPySparkGenerator:
    """Map symbolic typed generators to PySpark compiler recipes."""

    def __init__(self, expressions: MapPySparkExpression) -> None:
        self._expressions = expressions

    def posexplode_struct(
        self,
        generator: PosexplodeStructPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkOperationRecipe:
        recipe = PySparkPosexplodeStructRecipe(
            expression=self._expressions.map(generator.expression, capabilities=capabilities),
            scope=generator.scope,
            schema=generator.schema,
            ordinal=generator.ordinal,
            function=generator.function,
        )
        if generator.function == "explode":
            return PySparkOperationRecipe.explode_struct_operation(recipe)
        return PySparkOperationRecipe.posexplode_struct_operation(recipe)
