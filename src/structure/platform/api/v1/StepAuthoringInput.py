from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringInput:
    parameter: str
    schema: object
    source: str
    scope: str
    lane: str
    ordinal: int
    driving: bool
