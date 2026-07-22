from __future__ import annotations

from typing import Mapping

from structure.core.plugins.api.Plugin import Plugin
from structure.core.tools.logic.rules.ValidateSchemaToolRequest import ValidateSchemaToolRequest
from structure.plugin.api.v1.model import SchemaInspectionRequest


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
        target = getattr(session, "target_backend", "pyspark")
        schema_api = Plugin.registry().select(target).api.schema
        schema = schema_api.read(
            SchemaInspectionRequest(
                schema=schema,
                from_path=from_path,
                from_table=from_table,
                format=format,
                runtime=spark if spark is not None else getattr(session, "runtime", None),
                target_variant=getattr(session, "target_variant", None),
                options=options,
            )
        )
        source = schema_api.source(schema, to=to)
        render = getattr(schema_api, "render_source", None)
        if not callable(render):
            raise ValueError(f"PLUGIN-E2709: Plugin {target!r} does not render generated schema source.")
        return render(source)
