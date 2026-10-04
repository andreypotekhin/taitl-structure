from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringSink:
    """A Core-resolved sink parameter supplied to a plugin authoring session."""

    parameter: str
    name: str
    writer_type: type
    ordinal: int
