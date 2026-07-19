from structure.platform.pyspark.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.platform.pyspark.commands.RenderPySparkSchema import RenderPySparkSchema
from structure.platform.pyspark.commands.RenderPySparkSchemaModule import RenderPySparkSchemaModule


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
