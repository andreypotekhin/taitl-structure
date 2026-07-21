from structure.plugin.pyspark.schema.commands.BuildTransformSchemas import BuildTransformSchemas
from structure.plugin.pyspark.schema.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.plugin.pyspark.schema.commands.ReadPySparkSchema import ReadPySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchemaModule import RenderPySparkSchemaModule
from structure.plugin.pyspark.schema.logic.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource


class Schema:

    def materialize(self) -> MaterializePySparkSchema:
        return MaterializePySparkSchema()

    def render(self) -> RenderPySparkSchema:
        return RenderPySparkSchema()

    def module(self) -> RenderPySparkSchemaModule:
        return RenderPySparkSchemaModule()

    def build(self) -> BuildTransformSchemas:
        return BuildTransformSchemas()

    def read(self) -> ReadPySparkSchema:
        return ReadPySparkSchema()

    def source(self) -> MapPySparkSchemaToStructureSource:
        return MapPySparkSchemaToStructureSource()
