from __future__ import annotations

from typing import Mapping

from structure.app.target.pyspark.logic.SparkConnectCompatibility import is_spark_connect_session
from structure.app.tools.logic.maps.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource
from structure.app.tools.logic.render.RenderStructureSchemaSource import RenderStructureSchemaSource
from structure.app.tools.logic.rules.ValidateSchemaToolRequest import ValidateSchemaToolRequest
from structure.app.tools.model import StructureToolError


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
            try:
                return spark.table(from_table).schema
            except Exception as error:
                if is_spark_connect_session(session=session, spark=spark):
                    raise self._spark_connect_error("table", from_table, error) from error
                raise

        reader = spark.read
        if options:
            reader = reader.options(**dict(options))
        try:
            return reader.format(format).load(from_path).schema
        except Exception as error:
            if is_spark_connect_session(session=session, spark=spark):
                raise self._spark_connect_error("path", from_path, error) from error
            raise

    def _spark_connect_error(self, source: str, value: str | None, error: Exception) -> StructureToolError:
        return StructureToolError(
            f"StructureTools could not read schema from {source} {value!r} through Spark Connect. "
            "Spark Connect metadata access must stay within APIs exposed by the remote session. "
            "Pass schema=... with an explicit StructType, or run the tool with target_variant = \"ordinary\" "
            "when metadata access requires classic PySpark internals. "
            f"Cause: {type(error).__name__}: {error}. "
            "See docs/reference/SparkConnect.md."
        )
