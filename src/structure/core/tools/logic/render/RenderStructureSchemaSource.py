import json

from structure.core.tools.logic.model.GeneratedSchemaClass import GeneratedSchemaClass
from structure.core.tools.logic.model.GeneratedSchemaField import GeneratedSchemaField
from structure.core.tools.logic.model.GeneratedSchemaSource import GeneratedSchemaSource


class RenderStructureSchemaSource:
    def __call__(self, source: GeneratedSchemaSource) -> str:
        lines = [*self._imports(), ""]
        for index, schema in enumerate(source.classes):
            if index:
                lines.append("")
            lines.extend(self._class(schema))
        return "\n".join(lines) + "\n"

    def _imports(self) -> tuple[str, ...]:
        return ("from structure import Schema", "from structure.platform.pyspark.dsl.field import *")

    def _class(self, schema: GeneratedSchemaClass) -> tuple[str, ...]:
        lines = [f"class {schema.name}(Schema):"]
        if not schema.fields:
            lines.append("    pass")
            return tuple(lines)

        lines.extend(self._field(field) for field in schema.fields)
        return tuple(lines)

    def _field(self, field: GeneratedSchemaField) -> str:
        options = []
        if not field.nullable:
            options.append("nullable=False")
        if field.alias is not None:
            options.append(f"alias={json.dumps(field.alias)}")
        declaration = field.type
        if options:
            prefix = declaration[:-1]
            separator = "" if prefix.endswith("(") else ", "
            declaration = f"{prefix}{separator}{', '.join(options)})"
        return f"    {field.name} = {declaration}"
