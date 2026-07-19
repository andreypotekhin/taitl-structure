from dataclasses import dataclass


@dataclass(frozen=True)
class WindowBound:
    kind: str
    value: int | None = None

    def as_pyspark(self) -> str | int:
        if self.kind == "unbounded_preceding":
            return "Window.unboundedPreceding"
        if self.kind == "unbounded_following":
            return "Window.unboundedFollowing"
        if self.kind == "current_row":
            return "Window.currentRow"
        if self.kind == "preceding":
            return -(self.value or 0)
        if self.kind == "following":
            return self.value or 0
        raise TypeError(f"Unsupported window bound: {self.kind}")
