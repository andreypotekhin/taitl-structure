from typing import Protocol

from structure.plugin.api.v1.model import BackendCapabilities


class CapabilitiesAPI(Protocol):
    def resolve(self, *, profile: str, variant: str) -> BackendCapabilities: ...
