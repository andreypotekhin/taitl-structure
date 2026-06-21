from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class RuntimeDiagnostic:
    code: str
    title: str
    transform: str
    execution_mode: str
    target_backend: str
    problem: str
    use: str
    docs: str
    context: Mapping[str, str] = field(default_factory=dict)

    def render(self) -> str:
        lines = [
            f"RuntimeError {self.code}: {self.title}",
            "",
            "Transform:",
            f"  {self.transform}",
            "",
            "Execution mode:",
            f"  {self.execution_mode}",
            "",
            "Target backend:",
            f"  {self.target_backend}",
        ]
        if self.context:
            lines.extend(["", "Context:"])
            lines.extend(f"  {key}: {value}" for key, value in self.context.items())
        lines.extend(
            [
                "",
                "Problem:",
                f"  {self.problem}",
                "",
                "Use:",
                f"  {self.use}",
                "",
                f"See {self.docs}",
            ]
        )
        return "\n".join(lines)
