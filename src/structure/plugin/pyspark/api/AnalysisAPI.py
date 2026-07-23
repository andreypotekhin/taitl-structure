from typing import Any, cast

from structure.plugin.api.v1 import AnalysisAPI as AnalysisAPIV1
from structure.plugin.api.v1 import StreamingAnalysisRequest, TraceabilityRequest
from structure.plugin.api.v1.model import CompilerTraceability, StreamingReport
from structure.plugin.pyspark.api.PySpark import PySpark


class AnalysisAPI(AnalysisAPIV1):
    def classify_streaming(self, request: StreamingAnalysisRequest) -> StreamingReport:
        return PySpark.compiler.streaming()(cast(Any, request.payload), required=request.required)

    def build_traceability(self, request: TraceabilityRequest) -> CompilerTraceability:
        return PySpark.compiler.traceability()(
            cast(Any, request.payload),
            source_transform=request.source_transform,
            transform_module=request.transform_module,
        )

    def describe_documentation(self, payload: object) -> dict[str, object]:
        return PySpark.render.documentation()(cast(Any, payload))
