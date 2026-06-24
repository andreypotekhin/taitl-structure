from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.target.pyspark.model.PySparkInputRecipe import PySparkInputRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class PySparkInputMapper:

    def map(self, name: str, schema: type[Structure], ordinal: int) -> PySparkInputRecipe:
        return PySparkInputRecipe(
            name=name,
            schema=schema,
            ordinal=ordinal,
            validation=PySparkValidationRecipe(
                target=name,
                schema=schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="input",
            ),
        )
