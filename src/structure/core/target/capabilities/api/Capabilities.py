from structure.core.target.capabilities.commands.ResolveBackendCapabilities import ResolveBackendCapabilities


class Capabilities:

    @staticmethod
    def resolve() -> ResolveBackendCapabilities:
        return ResolveBackendCapabilities()
