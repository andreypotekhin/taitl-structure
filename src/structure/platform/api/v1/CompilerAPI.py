from typing import Protocol

from structure.platform.api.v1.CompileRequest import CompileRequest
from structure.platform.api.v1.PlatformCompilation import PlatformCompilation


class CompilerAPI(Protocol):
    def compile(self, request: CompileRequest) -> PlatformCompilation: ...
