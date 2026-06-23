from structure.app.target.pyspark.logic.actions.MaterializePySparkSchema import MaterializePySparkSchema
from structure.app.target.pyspark.logic.actions.RenderPySparkSchema import RenderPySparkSchema
from structure.app.target.pyspark.logic.actions.RenderPySparkSchemaModule import RenderPySparkSchemaModule


class SchemaEndpoint:

    def materialize(self) -> MaterializePySparkSchema:
        return MaterializePySparkSchema()

    def render(self) -> RenderPySparkSchema:
        return RenderPySparkSchema()

    def module(self) -> RenderPySparkSchemaModule:
        return RenderPySparkSchemaModule()
