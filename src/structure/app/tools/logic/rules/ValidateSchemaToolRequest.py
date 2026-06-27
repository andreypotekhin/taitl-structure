from __future__ import annotations

import keyword
from typing import Mapping

from structure.app.tools.model import StructureToolError


class ValidateSchemaToolRequest:

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
    ) -> None:
        self._class_name(to)
        self._source(schema=schema, from_path=from_path, from_table=from_table)
        self._path(format=format, from_path=from_path)
        self._options(options=options, from_path=from_path)
        self._spark_source(schema=schema, from_path=from_path, from_table=from_table, spark=spark, session=session)

    def field(self, name: str, *, path: str) -> None:
        if not isinstance(name, str) or not name.isidentifier() or keyword.iskeyword(name):
            raise StructureToolError(
                f"Cannot generate Structure field for Spark field {path!r}. "
                "Structure v1 requires Spark field names to be valid Python identifiers."
            )

    def _class_name(self, name: str) -> None:
        if not isinstance(name, str) or not name.isidentifier() or keyword.iskeyword(name) or not name[:1].isupper():
            raise StructureToolError(
                f"Invalid Structure class name: {name!r}. Use a Python class name such as OrderRaw."
            )

    def _source(self, *, schema=None, from_path: str | None, from_table: str | None) -> None:
        sources = [schema is not None, from_path is not None, from_table is not None]
        if sum(sources) != 1:
            raise StructureToolError("Pass exactly one schema source: schema=..., from_path=..., or from_table=....")

    def _spark_source(
        self, *, schema=None, from_path: str | None, from_table: str | None, spark=None, session=None
    ) -> None:
        if schema is not None and spark is None and session is None:
            return
        if schema is not None:
            return
        if spark is not None and session is not None:
            raise StructureToolError("Pass spark=... or session=..., not both.")
        if spark is None and session is None:
            raise StructureToolError(
                "Live source schema generation needs Spark metadata access. "
                "Pass spark=... or session=StructureSession(...)."
            )
        if session is not None and getattr(session, "spark", None) is None:
            raise StructureToolError(
                "StructureSession has no spark session. Create it with StructureSession(spark=spark)."
            )

    def _path(self, *, format: str | None, from_path: str | None) -> None:
        if from_path is None:
            return
        if not format:
            raise StructureToolError("Path schema generation requires format='parquet' or format='delta'.")
        if format not in {"parquet", "delta"}:
            raise StructureToolError(f"Unsupported source format: {format!r}. Use 'parquet' or 'delta'.")

    def _options(self, *, options: Mapping[str, str] | None, from_path: str | None) -> None:
        if options and from_path is None:
            raise StructureToolError("Reader options are supported only with from_path=....")
