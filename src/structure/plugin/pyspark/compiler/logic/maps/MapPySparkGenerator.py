from __future__ import annotations

from structure.plugin.api.v1.model import BackendCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkScalarGeneratorRecipe import PySparkScalarGeneratorRecipe
from structure.plugin.pyspark.dsl.operations.PosexplodeStructPlan import PosexplodeStructPlan
from structure.plugin.pyspark.dsl.operations.ScalarGeneratorPlan import ScalarGeneratorPlan


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
            outer=generator.outer,
            tvf=generator.tvf,
        )
        if generator.function == "explode_outer":
            return PySparkOperationRecipe.explode_outer_struct_operation(recipe)
        if generator.function == "explode":
            return PySparkOperationRecipe.explode_struct_operation(recipe)
        if generator.function == "posexplode_outer":
            return PySparkOperationRecipe.posexplode_outer_struct_operation(recipe)
        if generator.function == "inline_outer":
            return PySparkOperationRecipe.inline_outer_struct_operation(recipe)
        if generator.function == "inline":
            return PySparkOperationRecipe.inline_struct_operation(recipe)
        if generator.function == "variant_explode_outer":
            return PySparkOperationRecipe.variant_explode_outer_operation(recipe)
        if generator.function == "variant_explode":
            return PySparkOperationRecipe.variant_explode_operation(recipe)
        return PySparkOperationRecipe.posexplode_struct_operation(recipe)

    def scalar_array(
        self,
        generator: ScalarGeneratorPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkOperationRecipe:
        recipe = PySparkScalarGeneratorRecipe(
            expression=self._expressions.map(generator.expression, capabilities=capabilities),
            scope=generator.scope,
            schema=generator.schema,
            value_field=generator.value_field,
            ordinal=generator.ordinal,
            function=generator.function,
            outer=generator.outer,
        )
        return getattr(PySparkOperationRecipe, f"{generator.function}_array_operation")(recipe)
