from typing import Protocol

from structure.plugin.api.v1.model.OptimizationGraph import OptimizationGraph
from structure.plugin.api.v1.model.OptimizationReport import OptimizationReport
from structure.plugin.api.v1.model.OptimizationRequest import OptimizationRequest
from structure.plugin.api.v1.model.PluginCompilation import PluginCompilation


class OptimizationAPI(Protocol):
    """Opt-in dependency facts and application of Core's whole-step selection."""

    def describe(self, request: OptimizationRequest) -> OptimizationGraph: ...

    def apply(self, request: OptimizationRequest, selection: OptimizationReport) -> PluginCompilation: ...
