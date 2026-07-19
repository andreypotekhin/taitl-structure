from structure.core.compiler.traceability.model.CompilerTraceability import CompilerTraceability
from structure.platform.api.v1.model import TraceabilityRequest


class BuildCompilerTraceability:
    def __call__(self, payload: object, *, source_transform: str, transform_module: str) -> CompilerTraceability:
        return self._analysis(payload).build_traceability(
            TraceabilityRequest(payload=payload, source_transform=source_transform, transform_module=transform_module)
        )

    def _analysis(self, payload: object):
        from structure.core.platforms.api.Platform import Platform

        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLATFORM-E2708: Traceability requires a platform-owned payload.")
        analysis = Platform.registry().select(target).api.analysis
        if analysis is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide compiler analysis.")
        return analysis
