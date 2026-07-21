from dataclasses import dataclass


@dataclass(frozen=True)
class PluginCompilation:
    lowered: object
    fingerprint: str
    analysis: object | None = None
    schemas: object | None = None
