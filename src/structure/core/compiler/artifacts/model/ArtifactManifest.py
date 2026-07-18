from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from structure.core.compiler.artifacts.model.ArtifactDependency import ArtifactDependency


@dataclass(frozen=True)
class ArtifactManifest:
    dependencies: tuple[ArtifactDependency, ...]
    options: tuple[object, ...]
    structure_version: str
    capability: str

    @property
    def fingerprint(self) -> str:
        payload = {
            "capability": self.capability,
            "dependencies": [dependency.__dict__ for dependency in self.dependencies],
            "options": self.options,
            "structure_version": self.structure_version,
        }
        encoded = json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()
