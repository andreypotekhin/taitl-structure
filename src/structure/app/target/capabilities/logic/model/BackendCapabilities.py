from typing import Protocol

from structure.app.target.capabilities.logic.model.BackendId import BackendId
from structure.app.target.capabilities.logic.model.CapabilityDecision import CapabilityDecision
from structure.app.target.capabilities.logic.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.capabilities.logic.model.GeneratedImports import GeneratedImports


class BackendCapabilities(Protocol):
    id: BackendId

    def imports(self) -> GeneratedImports: ...

    def supports(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...

    def require(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...
