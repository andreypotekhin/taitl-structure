from structure.core.compiler.traceability.model.CompilerTraceability import CompilerTraceability
from structure.plugin.api.v1.model import TraceabilityRequest


class BuildCompilerTraceability:
    def __call__(self, payload: object, *, source_transform: str, transform_module: str) -> CompilerTraceability:
        return self._analysis(payload).build_traceability(
            TraceabilityRequest(payload=payload, source_transform=source_transform, transform_module=transform_module)
        )

    def _analysis(self, payload: object):
        from structure.core.plugins.api.Plugin import Plugin

        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLUGIN-E2708: Traceability requires a plugin-owned payload.")
        analysis = Plugin.registry().select(target).api.analysis
        if analysis is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {target!r} does not provide compiler analysis.")
        return analysis
