from dataclasses import dataclass

from structure.core.tools.logic.model.GeneratedSchemaClass import GeneratedSchemaClass


@dataclass(frozen=True)
class GeneratedSchemaSource:
    classes: tuple[GeneratedSchemaClass, ...]
