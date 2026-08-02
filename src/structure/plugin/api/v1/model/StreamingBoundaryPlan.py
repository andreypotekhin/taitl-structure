from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingBoundaryPlan:
    """A composition boundary whose undeclared streaming mode needs backend analysis."""

    producer: str
    output: str
    consumer: str
    input: str
