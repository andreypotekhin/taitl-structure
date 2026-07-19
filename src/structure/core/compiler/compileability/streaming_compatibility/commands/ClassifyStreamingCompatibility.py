from structure.core.compiler.compileability.streaming_compatibility.model.StreamingReport import StreamingReport
from structure.platform.api.v1.StreamingAnalysisRequest import StreamingAnalysisRequest


class ClassifyStreamingCompatibility:
    def __call__(self, payload: object, *, required: bool) -> StreamingReport:
        return self._analysis(payload).classify_streaming(StreamingAnalysisRequest(payload=payload, required=required))

    def _analysis(self, payload: object):
        from structure.core.platforms.api.Platform import Platform

        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLATFORM-E2708: Streaming analysis requires a platform-owned payload.")
        analysis = Platform.registry().select(target).api.analysis
        if analysis is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide compiler analysis.")
        return analysis
