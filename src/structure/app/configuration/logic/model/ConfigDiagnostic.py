from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfigDiagnostic:
    code: str
    setting: str
    problem: str
    use: str
    docs: str = "docs/specifications/ConfigSchema.md"

    def render(self) -> str:
        return "\n".join(
            [
                f"ConfigError {self.code}: {self.problem}",
                "",
                f"Setting: {self.setting}",
                "",
                f"Use: {self.use}",
                "",
                f"See {self.docs}",
            ]
        )
