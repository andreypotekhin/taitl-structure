from dataclasses import dataclass


@dataclass(frozen=True)
class StepInputPlan:
    """A Core-resolved structural binding from a step parameter to a lane."""

    parameter: str
    schema: object
    source: str
    scope: str
    lane: str
    ordinal: int
    driving: bool
