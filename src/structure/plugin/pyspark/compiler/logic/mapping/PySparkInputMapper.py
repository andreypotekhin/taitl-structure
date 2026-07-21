from structure.dsl import Schema, SchemaMode, StreamingMode
from structure.plugin.pyspark.compiler.model.PySparkInputRecipe import PySparkInputRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


class PySparkInputMapper:

    def map(
        self,
        name: str,
        schema: type[Schema],
        ordinal: int,
        streaming: StreamingMode,
        aliases: tuple[str, ...] = (),
    ) -> PySparkInputRecipe:
        return PySparkInputRecipe(
            name=name,
            schema=schema,
            ordinal=ordinal,
            streaming=streaming,
            aliases=aliases,
            validation=PySparkValidationRecipe(
                target=name,
                schema=schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="input",
            ),
        )
