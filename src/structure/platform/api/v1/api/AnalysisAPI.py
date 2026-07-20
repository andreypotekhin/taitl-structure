from typing import Protocol

from structure.platform.api.v1.model import (
    CompilerTraceability,
    StreamingAnalysisRequest,
    StreamingReport,
    TraceabilityRequest,
)


class AnalysisAPI(Protocol):
    def classify_streaming(self, request: StreamingAnalysisRequest) -> StreamingReport: ...

    def build_traceability(self, request: TraceabilityRequest) -> CompilerTraceability: ...
