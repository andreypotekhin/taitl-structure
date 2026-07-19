from dataclasses import dataclass


@dataclass(frozen=True)
class ExplainRequest:
    transform: object
