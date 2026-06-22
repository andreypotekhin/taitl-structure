from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DiagnosticEntry:
    code: str
    severity: str
    title: str
    owner: str
    status: str
    docs: str
    introduced: str
    problem_template: str
    use_template: str
    why_template: str = ""
    replaced_by: str = ""
