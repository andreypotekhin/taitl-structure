from dataclasses import dataclass


@dataclass(frozen=True)
class BackendId:
    name: str
    target: str
    family: str

    def display(self) -> str:
        return f"{self.name} {self.target}"
