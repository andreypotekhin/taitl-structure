from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringResult:
    schema: object
    lane: str
    frame: str
    ordinal: int
