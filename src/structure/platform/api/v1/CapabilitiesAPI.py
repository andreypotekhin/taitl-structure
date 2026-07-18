from typing import Protocol


class CapabilitiesAPI(Protocol):
    def supports(self, capability: str) -> bool: ...
