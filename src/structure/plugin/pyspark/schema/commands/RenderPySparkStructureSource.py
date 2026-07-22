import json
import re

from structure.plugin.api.v1.model import GeneratedSchemaSource


class RenderPySparkStructureSource:
    def __call__(self, source: GeneratedSchemaSource) -> str:
        lines = ["from structure import *", "from structure.plugin.pyspark import *", ""]
        for index, schema in enumerate(source.classes):
            if index:
                lines.append("")
            lines.extend(self._schema(schema))
        return "\n".join(lines) + "\n"

    def _schema(self, schema) -> tuple[str, ...]:
        if not schema.fields:
            return (f"class {schema.name}(Schema):", "    pass")
        return (f"class {schema.name}(Schema):", *(self._field(field) for field in schema.fields))

    def _field(self, field) -> str:
        options = []
        if not field.nullable:
            options.append("nullable=False")
        if field.alias is not None:
            options.append(f"alias={json.dumps(field.alias)}")
        declaration = re.sub(
            r"\b(array|boolean|date|decimal|double|float|integer|long|map|string|struct|timestamp)\(",
            r"\1(",
            field.type,
        )
        if options:
            prefix = declaration[:-1]
            declaration = f"{prefix}{'' if prefix.endswith('(') else ', '}{', '.join(options)})"
        return f"    {field.name} = {declaration}"
