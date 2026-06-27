from dataclasses import dataclass

from structure.app.tools.logic.model.GeneratedSchemaClass import GeneratedSchemaClass


@dataclass(frozen=True)
class GeneratedSchemaSource:
    classes: tuple[GeneratedSchemaClass, ...]
