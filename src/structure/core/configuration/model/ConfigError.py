from __future__ import annotations

from structure.core.configuration.model.ConfigDiagnostic import ConfigDiagnostic


class ConfigError(ValueError):

    def __init__(self, diagnostic: ConfigDiagnostic) -> None:
        super().__init__(diagnostic.render())
        self.diagnostic = diagnostic
