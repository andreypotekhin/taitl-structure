from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringSink:
    """A Core-resolved sink parameter supplied to a plugin authoring session."""

    parameter: str
    name: str
    sink_type: type
    ordinal: int
    kind: str = "row"

    @property
    def writer_type(self) -> type:
        """Compatibility alias for row-sink plugin implementations."""
        return self.sink_type
