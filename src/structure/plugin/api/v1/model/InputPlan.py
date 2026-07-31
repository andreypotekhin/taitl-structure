from dataclasses import dataclass


@dataclass(frozen=True)
class InputPlan:
    """A Core-resolved transform input declaration."""

    name: str
    schema: object
    ordinal: int
    streaming: bool = False
    aliases: tuple[str, ...] = ()
    streaming_declared: bool = False
