from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringSink:
    """A named consumed schema declaration supplied to a plugin authoring session."""

    name: str
    schema: type
