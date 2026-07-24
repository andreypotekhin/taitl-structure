from structure.dsl import SchemaMode
from structure.plugin.api.v1.model import BackendCapabilities, OutputPlan
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkName import MapPySparkName
from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


class MapPySparkOutput:
    """Map Core-owned final routing; PySpark bodies belong to step recipes."""

    def __init__(self) -> None:
        self._names = MapPySparkName()

    def map(
        self,
        output: OutputPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkOutputRecipe:
        input_alias = self._names.alias(output.source_schema.__name__)
        output_alias = self._names.alias(output.schema.__name__)
        return PySparkOutputRecipe(
            name=output.name,
            ordinal=output.ordinal,
            source=output.source,
            source_scope=output.source_scope,
            input_schema=output.source_schema,
            output_schema=output.schema,
            input_alias=input_alias,
            output_alias=output_alias,
            filters=(),
            joins=(),
            projection=(),
            validation=PySparkValidationRecipe(
                target=output.name,
                schema=output.schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="final",
            ),
            operations=(),
            aliases=output.aliases,
        )
