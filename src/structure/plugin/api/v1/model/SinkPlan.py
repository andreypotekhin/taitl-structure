from dataclasses import dataclass


@dataclass(frozen=True)
class SinkPlan:
    """A final output row handoff to a caller-owned PySpark writer class."""

    name: str
    output: str
    writer_module: str
    writer_qualname: str
    streaming: bool
