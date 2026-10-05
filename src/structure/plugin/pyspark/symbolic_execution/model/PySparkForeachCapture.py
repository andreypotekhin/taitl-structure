from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkForeachCapture:
    row: object
    sink: object
    kind: str = "row"
