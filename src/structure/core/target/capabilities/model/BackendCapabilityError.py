from structure.core.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.lib.cross.errors import render_diagnostic


class BackendCapabilityError(Exception):

    def __init__(self, decision: CapabilityDecision) -> None:
        super().__init__(render_diagnostic(decision.to_diagnostic().to_diagnostic(), kind="BackendError"))
        self.decision = decision
        self.diagnostic = decision.to_diagnostic()
