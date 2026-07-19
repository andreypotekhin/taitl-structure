from typing import Protocol

from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities


class CapabilitiesAPI(Protocol):
    def resolve(self, *, profile: str, variant: str) -> BackendCapabilities: ...
