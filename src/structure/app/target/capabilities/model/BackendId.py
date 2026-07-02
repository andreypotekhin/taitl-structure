from dataclasses import dataclass


@dataclass(frozen=True)
class BackendId:
    name: str
    target: str
    family: str
    variant: str = "ordinary"

    def display(self) -> str:
        return f"{self.name} {self.target} {self.variant}"
