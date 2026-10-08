from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringResult:
    schema: object
    lane: str
    frame: str
    ordinal: int
    binding: str = "dataframe"
    table_source: str | None = None
