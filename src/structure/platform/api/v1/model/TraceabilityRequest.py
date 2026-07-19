from dataclasses import dataclass


@dataclass(frozen=True)
class TraceabilityRequest:
    payload: object
    source_transform: str
    transform_module: str
