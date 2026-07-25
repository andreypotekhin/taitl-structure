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
        target: str | None = None,
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
        selected_target = target if target is not None else getattr(session, "target", "pyspark")
        if not isinstance(selected_target, str):
            selected_target = "pyspark"
        plugin_options = getattr(session, "plugin_options", {})
        schema_api = Plugin.registry().select(selected_target).api.schema
        schema = schema_api.read(
            SchemaInspectionRequest(
                schema=schema,
                from_path=from_path,
                from_table=from_table,
                format=format,
                runtime=spark if spark is not None else getattr(session, "runtime", None),
                plugin_options=plugin_options if isinstance(plugin_options, Mapping) else {},
                options=options,
            )
        )
        source = schema_api.source(schema, to=to)
        render = getattr(schema_api, "render_source", None)
        if not callable(render):
            raise ValueError(f"PLUGIN-E2709: Plugin {selected_target!r} does not render generated schema source.")
        return render(source)
