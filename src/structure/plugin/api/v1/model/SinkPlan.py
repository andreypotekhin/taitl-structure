from dataclasses import dataclass


@dataclass(frozen=True)
class SinkPlan:
    """A final output handoff tied to its consumed Structure schema."""

    name: str
    output: str
    streaming: bool
    kind: str = "row"
    sink_module: str | None = None
    sink_qualname: str | None = None
