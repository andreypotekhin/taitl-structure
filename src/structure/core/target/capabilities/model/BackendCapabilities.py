from typing import Protocol

from structure.core.target.capabilities.model.BackendId import BackendId
from structure.core.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.core.target.capabilities.model.GeneratedImports import GeneratedImports


class BackendCapabilities(Protocol):
    id: BackendId

    def imports(self) -> GeneratedImports: ...

    def supports(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...

    def require(self, requirement: CapabilityRequirement) -> CapabilityDecision: ...
