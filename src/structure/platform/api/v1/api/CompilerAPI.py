from typing import Protocol

from structure.platform.api.v1.model import CompileRequest, PlatformCompilation


class CompilerAPI(Protocol):
    def compile(self, request: CompileRequest) -> PlatformCompilation: ...
