from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformCompilation:
    lowered: object
    fingerprint: str
    analysis: object | None = None
