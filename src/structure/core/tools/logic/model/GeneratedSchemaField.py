from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratedSchemaField:
    name: str
    type: str
    nullable: bool
    alias: str | None = None
