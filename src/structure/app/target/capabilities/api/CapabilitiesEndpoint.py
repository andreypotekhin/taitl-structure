from structure.app.target.capabilities.logic.actions.ResolveBackendCapabilities import ResolveBackendCapabilities


class CapabilitiesEndpoint:

    def resolve(self) -> ResolveBackendCapabilities:
        return ResolveBackendCapabilities()
