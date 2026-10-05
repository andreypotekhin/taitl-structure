from dataclasses import dataclass


@dataclass(frozen=True)
class StepSinkCapture:
    """A plugin-captured reference from one returned step result to a sink."""

    sink: str
    result_ordinal: int | None
    row_schema: str
    input_ordinal: int | None = None
    kind: str = "row"
