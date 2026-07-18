from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformArtifact:
    platform: str
    distribution: str
    plugin_version: str
    api_version: int
    configuration: tuple[tuple[str, object], ...]
    fingerprint: str
    payload: object
    analysis: object | None = None
