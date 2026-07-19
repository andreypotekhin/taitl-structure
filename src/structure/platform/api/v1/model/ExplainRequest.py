from dataclasses import dataclass


@dataclass(frozen=True)
class ExplainRequest:
    transform: object
    payload: object | None = None
    analysis: object | None = None
