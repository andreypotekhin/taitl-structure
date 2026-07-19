from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.SchemaMode import SchemaMode
from structure.core.dsl.model.transforms.StreamingMode import StreamingMode
from structure.platform.pyspark.model.PySparkInputRecipe import PySparkInputRecipe
from structure.platform.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


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
