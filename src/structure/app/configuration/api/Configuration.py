from structure.app.configuration.commands.ResolveStructureConfig import ResolveStructureConfig


class Configuration:

    @staticmethod
    def resolve() -> ResolveStructureConfig:
        return ResolveStructureConfig()
