import difflib
from collections.abc import Mapping

from structure.app.configuration.model.ConfigDiagnostic import ConfigDiagnostic
from structure.app.configuration.model.ConfigError import ConfigError


class StructureConfigMerger:

    def __init__(self, keys: set[str]) -> None:
        self._keys = keys

    def merge(
        self, values: dict[str, object], sources: dict[str, str], incoming: Mapping[str, object], source: str
    ) -> None:
        for key, value in incoming.items():
            if key not in self._keys:
                self._fail_unknown(key)
            values[key] = value
            sources[key] = source

    def _fail_unknown(self, key: str) -> None:
        suggestion = difflib.get_close_matches(key, self._keys, n=1)
        use = (
            f"Did you mean {suggestion[0]}?"
            if suggestion
            else "Remove the key or add it to the config specification first."
        )
        raise ConfigError(
            ConfigDiagnostic(code="CONF-E0101", setting=key, problem="Unknown configuration key", use=use)
        )
