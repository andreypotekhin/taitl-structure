from dataclasses import dataclass


@dataclass(frozen=True)
class SinkPlan:
    """A final output handoff to a caller-owned row writer or batch processor."""

    name: str
    output: str
    streaming: bool
    kind: str = "row"
    sink_module: str | None = None
    sink_qualname: str | None = None

    @property
    def writer_module(self) -> str | None:
        return self.sink_module if self.kind == "row" else None

    @property
    def writer_qualname(self) -> str | None:
        return self.sink_qualname if self.kind == "row" else None
