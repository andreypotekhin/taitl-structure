from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from structure.lib.cross.errors.DiagnosticEntry import DiagnosticEntry


@dataclass(frozen=True)
class Diagnostic:
    entry: DiagnosticEntry
    problem: str = ""
    use: str = ""
    context: Mapping[str, str] = field(default_factory=dict)
    source: str = ""

    @property
    def code(self) -> str:
        return self.entry.code

    @property
    def severity(self) -> str:
        return self.entry.severity

    @property
    def title(self) -> str:
        return self.entry.title

    @property
    def docs(self) -> str:
        return self.entry.docs

    @property
    def why(self) -> str:
        return self.entry.why_template

    def problem_text(self) -> str:
        return self.problem or self.entry.problem_template

    def use_text(self) -> str:
        return self.use or self.entry.use_template
