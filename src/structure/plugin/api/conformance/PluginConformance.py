"""Public checks for a plugin's metadata and negotiated API façade."""

import re
from dataclasses import dataclass

from structure.plugin.api import CORE_API_MAX_VERSION, CORE_API_MIN_VERSION, Plugin, PluginDescriptor
from structure.plugin.api.v1 import PluginAPI


@dataclass(frozen=True)
class ConformingPlugin:
    """A plugin that passed metadata, negotiation, and required-facet checks."""

    descriptor: PluginDescriptor
    api_version: int
    api: PluginAPI


class PluginConformance:
    """Reusable public validation for plugin authors and Core's registry."""

    _name = re.compile(r"[a-z][a-z0-9-]*$")

    @classmethod
    def negotiate(
        cls,
        plugin: Plugin,
        *,
        entry_name: str,
        distribution: str,
        minimum_api_version: int = CORE_API_MIN_VERSION,
        maximum_api_version: int = CORE_API_MAX_VERSION,
    ) -> ConformingPlugin:
        """Return the highest mutually supported façade or raise an actionable error."""
        descriptor = plugin.descriptor
        cls._validate_identity(descriptor, entry_name, distribution)
        version = min(maximum_api_version, descriptor.maximum_api_version)
        if version < max(minimum_api_version, descriptor.minimum_api_version):
            raise ValueError(
                f"PLUGIN-E2707: Plugin {entry_name!r} has no compatible Plugin API version. "
                f"Core supports v{minimum_api_version} through v{maximum_api_version}; the plugin supports "
                f"v{descriptor.minimum_api_version} through v{descriptor.maximum_api_version}."
            )
        try:
            api = plugin.api(version)
        except Exception as error:
            raise ValueError(
                f"PLUGIN-E2708: Plugin {entry_name!r} could not provide its advertised Plugin API v{version}: "
                f"{type(error).__name__}: {error}"
            ) from error
        cls._validate_api(api, entry_name, version)
        return ConformingPlugin(descriptor, version, api)

    @classmethod
    def _validate_identity(cls, descriptor: PluginDescriptor, entry_name: str, distribution: str) -> None:
        if cls._name.fullmatch(entry_name) is None:
            raise ValueError(f"PLUGIN-E2706: Plugin entry-point name {entry_name!r} is not a lowercase identifier.")
        if descriptor.name != entry_name:
            raise ValueError(
                f"PLUGIN-E2706: Plugin entry point {entry_name!r} loaded a plugin named {descriptor.name!r}."
            )
        if cls._distribution(descriptor.distribution) != cls._distribution(distribution):
            raise ValueError(
                f"PLUGIN-E2706: Plugin {entry_name!r} declares distribution {descriptor.distribution!r}, "
                f"but was discovered from {distribution!r}."
            )

    @staticmethod
    def _validate_api(api: object, name: str, version: int) -> None:
        missing: tuple[str, ...] = () if isinstance(api, PluginAPI) else ("PluginAPI façade",)
        if not missing:
            missing = tuple(
                facet for facet in ("schema", "authoring", "compiler", "capabilities") if getattr(api, facet) is None
            )
        if missing:
            raise ValueError(
                f"PLUGIN-E2708: Plugin {name!r} did not supply a complete Plugin API v{version} façade; "
                f"missing {', '.join(missing)}."
            )

    @staticmethod
    def _distribution(name: str | None) -> str:
        return re.sub(r"[-_.]+", "-", name or "unknown").lower()
