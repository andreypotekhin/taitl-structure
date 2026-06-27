from dataclasses import dataclass

from structure.app.tools.logic.model.GeneratedSchemaField import GeneratedSchemaField


@dataclass(frozen=True)
class GeneratedSchemaClass:
    name: str
    fields: tuple[GeneratedSchemaField, ...]
