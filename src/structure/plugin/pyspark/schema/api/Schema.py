from collections.abc import Mapping

from structure.dsl import Schema as StructureSchema
from structure.plugin.pyspark.schema.commands.BuildTransformSchemas import BuildTransformSchemas
from structure.plugin.pyspark.schema.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.plugin.pyspark.schema.commands.ReadPySparkSchema import ReadPySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchemaModule import RenderPySparkSchemaModule
from structure.plugin.pyspark.schema.commands.RenderPySparkStructureSource import RenderPySparkStructureSource
from structure.plugin.pyspark.schema.logic.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource


class Schema:

    def materialize(self) -> MaterializePySparkSchema:
        return MaterializePySparkSchema()

    def render(self, schema_names: Mapping[type[StructureSchema], str] | None = None) -> RenderPySparkSchema:
        return RenderPySparkSchema(schema_names)

    def module(self, schema_names: Mapping[type[StructureSchema], str] | None = None) -> RenderPySparkSchemaModule:
        return RenderPySparkSchemaModule(schema_names)

    def build(self) -> BuildTransformSchemas:
        return BuildTransformSchemas()

    def read(self) -> ReadPySparkSchema:
        return ReadPySparkSchema()

    def source(self) -> MapPySparkSchemaToStructureSource:
        return MapPySparkSchemaToStructureSource()

    def structure_source(self) -> RenderPySparkStructureSource:
        return RenderPySparkStructureSource()
