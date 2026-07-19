from typing import Protocol

from structure.core.compiler.compileability.streaming_compatibility.model.StreamingReport import StreamingReport
from structure.core.compiler.traceability.model.CompilerTraceability import CompilerTraceability
from structure.platform.api.v1.StreamingAnalysisRequest import StreamingAnalysisRequest
from structure.platform.api.v1.TraceabilityRequest import TraceabilityRequest


class AnalysisAPI(Protocol):
    def classify_streaming(self, request: StreamingAnalysisRequest) -> StreamingReport: ...

    def build_traceability(self, request: TraceabilityRequest) -> CompilerTraceability: ...
