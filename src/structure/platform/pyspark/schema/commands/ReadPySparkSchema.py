from __future__ import annotations

from typing import Any, cast

from structure.core.tools.model import StructureToolError
from structure.platform.api.v1.model import SchemaInspectionRequest


class ReadPySparkSchema:
    def __call__(self, request: SchemaInspectionRequest) -> object:
        from structure.platform.pyspark.api.PySpark import PySpark

        if request.schema is not None:
            return getattr(request.schema, "schema", request.schema)

        spark = cast(Any, request.runtime)
        if request.from_table is not None:
            try:
                return spark.table(request.from_table).schema
            except Exception as error:
                if PySpark.capabilities.spark_connect().session(session=request, spark=spark):
                    raise self._spark_connect_error("table", request.from_table, error) from error
                raise

        reader = spark.read
        if request.options:
            reader = reader.options(**dict(request.options))
        try:
            return reader.format(request.format).load(request.from_path).schema
        except Exception as error:
            if PySpark.capabilities.spark_connect().session(session=request, spark=spark):
                raise self._spark_connect_error("path", request.from_path, error) from error
            raise

    def _spark_connect_error(self, source: str, value: str | None, error: Exception) -> StructureToolError:
        return StructureToolError(
            f"StructureTools could not read schema from {source} {value!r} through Spark Connect. "
            "Spark Connect metadata access must stay within APIs exposed by the remote session. "
            "Pass schema=... with an explicit StructType, or run the tool with target_variant = \"ordinary\" "
            f"when metadata access requires classic PySpark internals. Cause: {type(error).__name__}: {error}. "
            "See docs/reference/SparkConnect.md."
        )
