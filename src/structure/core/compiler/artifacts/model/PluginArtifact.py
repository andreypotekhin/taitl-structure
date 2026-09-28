from dataclasses import dataclass

from structure.plugin.api.v1.model.OptimizationReport import OptimizationReport


@dataclass(frozen=True)
class PluginArtifact:
    plugin: str
    distribution: str
    plugin_version: str
    api_version: int
    configuration: tuple[tuple[str, object], ...]
    fingerprint: str
    payload: object
    analysis: object | None = None
    optimization: OptimizationReport | None = None
