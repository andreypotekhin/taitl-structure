from typing import Protocol

from structure.plugin.api.v1.model import CompileRequest, PluginCompilation


class CompilerAPI(Protocol):
    def compile(self, request: CompileRequest) -> PluginCompilation: ...
