from structure.app.backend.logic.model.CapabilityDecision import CapabilityDecision


class BackendCapabilityError(Exception):

    def __init__(self, decision: CapabilityDecision) -> None:
        super().__init__(decision.problem)
        self.decision = decision
        self.diagnostic = decision.to_diagnostic()
