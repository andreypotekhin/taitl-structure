from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ArtifactDependency:
    kind: str
    name: str
    path: str | None
    digest: str | None
