import difflib
from collections.abc import Mapping
from typing import cast

from structure.core.configuration.model.ConfigDiagnostic import ConfigDiagnostic
from structure.core.configuration.model.ConfigError import ConfigError


class StructureConfigMerger:

    _retired = {
        "target_pyspark": 'Use target_profile = ">=3.5,<4.1".',
    }

    def __init__(self, keys: set[str]) -> None:
        self._keys = keys

    def merge(
        self, values: dict[str, object], sources: dict[str, str], incoming: Mapping[str, object], source: str
    ) -> None:
        for key, value in incoming.items():
            if key not in self._keys:
                self._fail_unknown(key)
            if key == "plugin" and isinstance(values.get(key), Mapping) and isinstance(value, Mapping):
                values[key] = self._merge_plugins(
                    cast(Mapping[str, object], values[key]), cast(Mapping[str, object], value)
                )
            else:
                values[key] = value
            sources[key] = source

    @staticmethod
    def _merge_plugins(current: Mapping[str, object], incoming: Mapping[str, object]) -> dict[str, object]:
        merged = dict(current)
        for name, value in incoming.items():
            previous = merged.get(name)
            merged[name] = {**previous, **value} if isinstance(previous, Mapping) and isinstance(value, Mapping) else value
        return merged

    def _fail_unknown(self, key: str) -> None:
        if key in self._retired:
            raise ConfigError(
                ConfigDiagnostic(
                    code="CONF-E0101",
                    setting=key,
                    problem="Unknown configuration key",
                    use=self._retired[key],
                )
            )
        suggestion = difflib.get_close_matches(key, self._keys, n=1)
        use = (
            f"Did you mean {suggestion[0]}?"
            if suggestion
            else "Remove the key or add it to the config specification first."
        )
        raise ConfigError(
            ConfigDiagnostic(code="CONF-E0101", setting=key, problem="Unknown configuration key", use=use)
        )
