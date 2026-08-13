from structure.dsl import Schema, SchemaMode
from structure.plugin.pyspark.compiler.model.PySparkInputRecipe import PySparkInputRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


class MapPySparkInput:

    def map(
        self,
        name: str,
        schema: type[Schema],
        ordinal: int,
        streaming: bool,
        aliases: tuple[str, ...] = (),
        optional: bool = False,
        internal: bool = False,
    ) -> PySparkInputRecipe:
        return PySparkInputRecipe(
            name=name,
            schema=schema,
            ordinal=ordinal,
            streaming=streaming,
            optional=optional,
            aliases=aliases,
            internal=internal,
            validation=PySparkValidationRecipe(
                target=name,
                schema=schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="input",
            ),
        )
