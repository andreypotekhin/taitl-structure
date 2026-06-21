from __future__ import annotations

from structure.app.configuration.logic.model.ConfigDiagnostic import ConfigDiagnostic


class ConfigError(ValueError):

    def __init__(self, diagnostic: ConfigDiagnostic) -> None:
        super().__init__(diagnostic.render())
        self.diagnostic = diagnostic
