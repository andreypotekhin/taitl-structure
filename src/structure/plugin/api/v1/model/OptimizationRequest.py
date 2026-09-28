from dataclasses import dataclass

from structure.plugin.api.v1.model.CompileRequest import CompileRequest
from structure.plugin.api.v1.model.PluginCompilation import PluginCompilation


@dataclass(frozen=True)
class OptimizationRequest:
    compile_request: CompileRequest
    compilation: PluginCompilation
