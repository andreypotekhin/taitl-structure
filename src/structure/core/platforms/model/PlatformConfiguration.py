from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, cast


@dataclass(frozen=True)
class PlatformConfiguration:
    default: str | None
    disabled_distributions: frozenset[str]
    plugin_options: str | None
    plugins: Mapping[str, Mapping[str, object]]

    @classmethod
    def resolve(cls, *layers: Mapping[str, object]) -> "PlatformConfiguration":
        platform: dict[str, object] = {}
        for layer in layers:
            values = layer.get("platform", {})
            if not isinstance(values, Mapping):
                raise ValueError("PLATFORM-E2701: platform must be a table.")
            for name, value in values.items():
                previous = platform.get(name)
                if isinstance(value, Mapping) and isinstance(previous, Mapping):
                    platform[name] = {**cast(Mapping[str, object], previous), **cast(Mapping[str, object], value)}
                else:
                    platform[name] = value
        return cls._build(platform)

    @classmethod
    def _build(cls, values: Mapping[str, object]) -> "PlatformConfiguration":
        default = values.pop("default", None) if isinstance(values, dict) else values.get("default")
        disabled = values.pop("disabled_distributions", ()) if isinstance(values, dict) else values.get("disabled_distributions", ())
        options = values.pop("plugin_options", None) if isinstance(values, dict) else values.get("plugin_options")
        if default is not None and (not isinstance(default, str) or not default):
            raise ValueError("PLATFORM-E2701: platform.default must be a non-empty string.")
        if not isinstance(disabled, (list, tuple)) or not all(isinstance(name, str) for name in disabled):
            raise ValueError("PLATFORM-E2701: platform.disabled_distributions must be a list of strings.")
        if options is not None and options != "allow_injection":
            raise ValueError("PLATFORM-E2701: platform.plugin_options must be 'allow_injection' when set.")
        if not all(isinstance(name, str) and isinstance(value, Mapping) for name, value in values.items()):
            raise ValueError("PLATFORM-E2701: platform plugin settings must be tables.")
        plugins = {
            name: MappingProxyType(dict(cast(Mapping[str, object], value)))
            for name, value in values.items()
            if name not in {"default", "disabled_distributions", "plugin_options"}
        }
        return cls(default, frozenset(disabled), options, MappingProxyType(plugins))
