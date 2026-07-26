from dataclasses import dataclass


@dataclass(frozen=True)
class ExactlyOnePlan:
    scope: str
