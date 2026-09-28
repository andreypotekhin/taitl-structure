from dataclasses import dataclass

from structure.plugin.api.v1.model.OptimizationReport import OptimizationReport


@dataclass(frozen=True)
class PluginCompilation:
    lowered: object
    fingerprint: str
    analysis: object | None = None
    schemas: object | None = None
    diagnostics: tuple[object, ...] = ()
    executable_steps: tuple[str, ...] | None = None
    optimization: OptimizationReport | None = None
