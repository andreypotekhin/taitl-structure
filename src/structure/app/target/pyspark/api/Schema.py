from structure.app.target.pyspark.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.app.target.pyspark.commands.RenderPySparkSchema import RenderPySparkSchema
from structure.app.target.pyspark.commands.RenderPySparkSchemaModule import RenderPySparkSchemaModule


class Schema:

    @staticmethod
    def materialize() -> MaterializePySparkSchema:
        return MaterializePySparkSchema()

    @staticmethod
    def render() -> RenderPySparkSchema:
        return RenderPySparkSchema()

    @staticmethod
    def module() -> RenderPySparkSchemaModule:
        return RenderPySparkSchemaModule()
