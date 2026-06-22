from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.traceability.logic.model.CompilerProvenance import CompilerProvenance
from structure.app.compiler.traceability.logic.model.DataflowDependency import DataflowDependency
from structure.app.compiler.traceability.logic.model.OpaqueBoundary import OpaqueBoundary


@dataclass(frozen=True)
class CompilerTraceability:
    provenance: tuple[CompilerProvenance, ...]
    static_dataflow: tuple[DataflowDependency, ...]
    opaque_boundaries: tuple[OpaqueBoundary, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "opaque_boundaries": [boundary.to_dict() for boundary in self.opaque_boundaries],
            "provenance": [record.to_dict() for record in self.provenance],
            "static_dataflow": [dependency.to_dict() for dependency in self.static_dataflow],
        }
