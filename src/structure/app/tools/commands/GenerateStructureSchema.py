from __future__ import annotations

from typing import Mapping

from structure.app.tools.logic.maps.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource
from structure.app.tools.logic.render.RenderStructureSchemaSource import RenderStructureSchemaSource
from structure.app.tools.logic.rules.ValidateSchemaToolRequest import ValidateSchemaToolRequest


class GenerateStructureSchema:

    def __call__(
        self,
        *,
        schema=None,
        from_path: str | None = None,
        from_table: str | None = None,
        format: str | None = None,
        spark=None,
        session=None,
        options: Mapping[str, str] | None = None,
        to: str,
    ) -> str:
        ValidateSchemaToolRequest()(
            schema=schema,
            from_path=from_path,
            from_table=from_table,
            format=format,
            spark=spark,
            session=session,
            options=options,
            to=to,
        )
        schema = self._schema(
            schema=schema,
            from_path=from_path,
            from_table=from_table,
            format=format,
            spark=spark,
            session=session,
            options=options,
        )
        source = MapPySparkSchemaToStructureSource()(schema, to=to)
        return RenderStructureSchemaSource()(source)

    def _schema(
        self,
        *,
        schema=None,
        from_path: str | None,
        from_table: str | None,
        format: str | None,
        spark=None,
        session=None,
        options: Mapping[str, str] | None,
    ):
        if schema is not None:
            return getattr(schema, "schema", schema)

        spark = spark if spark is not None else session.spark
        if from_table is not None:
            return spark.table(from_table).schema

        reader = spark.read
        if options:
            reader = reader.options(**dict(options))
        return reader.format(format).load(from_path).schema
