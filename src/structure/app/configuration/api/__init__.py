from structure.app.configuration.api.ConfigurationEndpoint import ConfigurationEndpoint
from structure.app.configuration.logic.actions.ResolveStructureConfig import ResolveStructureConfig
from structure.app.configuration.logic.model.ConfigDiagnostic import ConfigDiagnostic
from structure.app.configuration.logic.model.ConfigError import ConfigError
from structure.app.configuration.logic.model.StructureConfig import StructureConfig

configuration = ConfigurationEndpoint()

__all__ = [
    "ConfigDiagnostic",
    "ConfigError",
    "ConfigurationEndpoint",
    "ResolveStructureConfig",
    "StructureConfig",
    "configuration",
]
