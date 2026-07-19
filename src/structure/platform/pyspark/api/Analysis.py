from typing import Any, cast

from structure.core.compiler.compileability.streaming_compatibility.model.StreamingReport import StreamingReport
from structure.core.compiler.traceability.model.CompilerTraceability import CompilerTraceability
from structure.platform.api.v1 import AnalysisAPI, StreamingAnalysisRequest, TraceabilityRequest
from structure.platform.pyspark.api.PySpark import PySpark


class Analysis(AnalysisAPI):
    def classify_streaming(self, request: StreamingAnalysisRequest) -> StreamingReport:
        return PySpark.compiler.streaming()(cast(Any, request.payload), required=request.required)

    def build_traceability(self, request: TraceabilityRequest) -> CompilerTraceability:
        return PySpark.compiler.traceability()(
            cast(Any, request.payload),
            source_transform=request.source_transform,
            transform_module=request.transform_module,
        )
