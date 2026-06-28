import json

from structure.app.tools.logic.model.GeneratedSchemaClass import GeneratedSchemaClass
from structure.app.tools.logic.model.GeneratedSchemaField import GeneratedSchemaField
from structure.app.tools.logic.model.GeneratedSchemaSource import GeneratedSchemaSource


class RenderStructureSchemaSource:

    def __call__(self, source: GeneratedSchemaSource) -> str:
        lines = [*self._imports(), ""]
        for index, schema in enumerate(source.classes):
            if index:
                lines.append("")
            lines.extend(self._class(schema))
        return "\n".join(lines) + "\n"

    def _imports(self) -> tuple[str, ...]:
        return (
            "from structure import (",
            "    Array,",
            "    Boolean,",
            "    Date,",
            "    Decimal,",
            "    Double,",
            "    Float,",
            "    Integer,",
            "    Long,",
            "    Map,",
            "    String,",
            "    Struct,",
            "    Structure,",
            "    Timestamp,",
            "    field,",
            ")",
        )

    def _class(self, schema: GeneratedSchemaClass) -> tuple[str, ...]:
        lines = [f"class {schema.name}(Structure):"]
        if not schema.fields:
            lines.append("    pass")
            return tuple(lines)

        lines.extend(self._field(field) for field in schema.fields)
        return tuple(lines)

    def _field(self, field: GeneratedSchemaField) -> str:
        nullable = "True" if field.nullable else "False"
        alias = f", alias={json.dumps(field.alias)}" if field.alias is not None else ""
        return f"    {field.name} = field({field.type}, nullable={nullable}{alias})"
