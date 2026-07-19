from dataclasses import dataclass


@dataclass(frozen=True)
class TransformSchemaRequest:
    payload: object
    types: object | None = None
