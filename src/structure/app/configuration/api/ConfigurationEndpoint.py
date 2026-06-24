from structure.app.configuration.logic.actions.ResolveStructureConfig import ResolveStructureConfig


class ConfigurationEndpoint:

    def resolve(self) -> ResolveStructureConfig:
        return ResolveStructureConfig()
