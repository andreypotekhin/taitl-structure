from typing import Protocol

from structure.app.backend.capabilities.logic.model.BackendId import BackendId
from structure.app.backend.capabilities.logic.model.CapabilityDecision import CapabilityDecision
from structure.app.backend.capabilities.logic.model.CapabilityRequirement import CapabilityRequirement
from structure.app.backend.capabilities.logic.model.GeneratedImports import GeneratedImports


class BackendCapabilities(Protocol):
    id: BackendId

    def imports(self) -> GeneratedImports: ...

    def supports(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...

    def require(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...
